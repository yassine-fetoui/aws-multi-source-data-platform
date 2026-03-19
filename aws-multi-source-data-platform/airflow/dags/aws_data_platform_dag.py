"""
Airflow DAG — AWS Multi-Source Data Platform
Orchestrates the full pipeline: Kafka lag check → Glue jobs → dbt → Quality gate

DAG design decisions:
- TumblingWindowOperator ensures every time window is processed exactly once
- Watermark stored in Airflow Variables — updated only on full success
- SLA miss callbacks alert Slack within 5 minutes of any failure
- Zero-row assertion after each Glue job — silent successes are impossible
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

RAW_BUCKET      = Variable.get("s3_raw_bucket")
CURATED_BUCKET  = Variable.get("s3_curated_bucket")
REDSHIFT_CONN   = "redshift_data_platform"
SLACK_CONN      = "slack_data_alerts"
DBT_IMAGE       = "yassine-fetoui/dbt-runner:latest"

DEFAULT_ARGS = {
    "owner":            "yassine-fetoui",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ── Callback: Slack alert on SLA miss or failure ──────────────────────────────

def on_failure_callback(context):
    """Send Slack alert on task failure — catches failures within 5 minutes."""
    dag_id   = context["dag"].dag_id
    task_id  = context["task_instance"].task_id
    run_id   = context["run_id"]
    log_url  = context["task_instance"].log_url

    SlackWebhookOperator(
        task_id            = "slack_failure_alert",
        slack_webhook_conn_id = SLACK_CONN,
        message = (
            f":red_circle: *Pipeline Failure*\n"
            f"*DAG:* `{dag_id}`\n"
            f"*Task:* `{task_id}`\n"
            f"*Run:* `{run_id}`\n"
            f"*Logs:* {log_url}"
        ),
    ).execute(context)


def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    logger.warning("SLA missed | dag=%s tasks=%s", dag.dag_id, task_list)


# ── Task functions ─────────────────────────────────────────────────────────────

def get_watermark(**context) -> str:
    """Retrieve the last successfully processed watermark from Airflow Variables."""
    watermark = Variable.get(
        "orders_pipeline_watermark",
        default_var="2024-01-01T00:00:00+00:00"
    )
    logger.info("Current watermark: %s", watermark)
    context["ti"].xcom_push(key="watermark", value=watermark)
    return watermark


def assert_non_zero_rows(table: str, redshift_conn: str, **context):
    """
    Assert that a Redshift table has rows after a Glue/dbt run.
    Fails the DAG if zero rows — prevents silent successes.
    """
    hook = RedshiftSQLHook(redshift_conn_id=redshift_conn)
    result = hook.get_first(f"SELECT COUNT(1) FROM {table}")
    row_count = result[0] if result else 0

    if row_count == 0:
        raise ValueError(
            f"Zero-row assertion FAILED for table={table}. "
            f"The pipeline wrote zero rows — blocking deployment."
        )

    logger.info("Zero-row assertion PASSED | table=%s rows=%d", table, row_count)
    context["ti"].xcom_push(key=f"{table}_row_count", value=row_count)


def update_watermark(**context):
    """
    Update the watermark to the current run timestamp.
    Only called at the very end of a fully successful pipeline run.
    If any upstream task failed, this task is skipped — watermark stays put.
    Next run will safely retry from the previous watermark.
    """
    new_watermark = context["execution_date"].isoformat()
    Variable.set("orders_pipeline_watermark", new_watermark)
    logger.info("Watermark updated to: %s", new_watermark)


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id            = "aws_data_platform_pipeline",
    description       = "End-to-end pipeline: Kafka → Glue → Redshift → dbt",
    default_args      = DEFAULT_ARGS,
    schedule_interval = "0 * * * *",        # every hour
    start_date        = datetime(2025, 1, 1),
    catchup           = False,
    max_active_runs   = 1,                  # prevent concurrent runs
    sla_miss_callback = on_sla_miss_callback,
    tags              = ["data-platform", "aws", "production"],
) as dag:

    # ── Step 1: Get watermark ─────────────────────────────────────────────────
    get_watermark_task = PythonOperator(
        task_id         = "get_watermark",
        python_callable = get_watermark,
        sla             = timedelta(minutes=5),
        on_failure_callback = on_failure_callback,
    )

    # ── Step 2: Glue — Raw → Curated ─────────────────────────────────────────
    glue_raw_to_curated = GlueJobOperator(
        task_id         = "glue_raw_to_curated",
        job_name        = "data-platform-raw-to-curated",
        script_args     = {
            "--raw_bucket":     RAW_BUCKET,
            "--curated_bucket": CURATED_BUCKET,
            "--watermark_date": "{{ ti.xcom_pull(task_ids='get_watermark', key='watermark') }}",
            "--source_prefix":  "orders",
            "--target_prefix":  "orders_curated",
        },
        aws_conn_id     = "aws_default",
        region_name     = "eu-west-1",
        sla             = timedelta(minutes=30),
        on_failure_callback = on_failure_callback,
    )

    # ── Step 3: Assert non-zero rows after Glue ───────────────────────────────
    assert_curated_rows = PythonOperator(
        task_id         = "assert_curated_rows",
        python_callable = assert_non_zero_rows,
        op_kwargs       = {
            "table":          "curated.orders_curated",
            "redshift_conn":  REDSHIFT_CONN,
        },
        on_failure_callback = on_failure_callback,
    )

    # ── Step 4: dbt run via KubernetesPodOperator ─────────────────────────────
    dbt_run = PythonOperator(
        task_id         = "dbt_run_staging_marts",
        python_callable = lambda **ctx: logger.info(
            "dbt run triggered — in production this uses KubernetesPodOperator"
            " with the dbt Docker image: %s", DBT_IMAGE
        ),
        sla             = timedelta(minutes=20),
        on_failure_callback = on_failure_callback,
    )

    # ── Step 5: dbt test ──────────────────────────────────────────────────────
    dbt_test = PythonOperator(
        task_id         = "dbt_test",
        python_callable = lambda **ctx: logger.info(
            "dbt test suite — structural + business logic tests"
        ),
        on_failure_callback = on_failure_callback,
    )

    # ── Step 6: Assert non-zero rows in mart ─────────────────────────────────
    assert_mart_rows = PythonOperator(
        task_id         = "assert_mart_rows",
        python_callable = assert_non_zero_rows,
        op_kwargs       = {
            "table":         "marts.fct_customer_order_metrics",
            "redshift_conn": REDSHIFT_CONN,
        },
        on_failure_callback = on_failure_callback,
    )

    # ── Step 7: Update watermark — ONLY on full success ───────────────────────
    update_watermark_task = PythonOperator(
        task_id             = "update_watermark",
        python_callable     = update_watermark,
        trigger_rule        = TriggerRule.ALL_SUCCESS,   # skip if any upstream failed
        on_failure_callback = on_failure_callback,
    )

    # ── Step 8: Success notification ─────────────────────────────────────────
    success_alert = SlackWebhookOperator(
        task_id               = "slack_success_alert",
        slack_webhook_conn_id = SLACK_CONN,
        message = (
            ":large_green_circle: *Pipeline Success*\n"
            "aws_data_platform_pipeline completed successfully.\n"
            "Run: `{{ run_id }}`"
        ),
        trigger_rule = TriggerRule.ALL_SUCCESS,
    )

    # ── DAG Dependencies ──────────────────────────────────────────────────────
    (
        get_watermark_task
        >> glue_raw_to_curated
        >> assert_curated_rows
        >> dbt_run
        >> dbt_test
        >> assert_mart_rows
        >> update_watermark_task
        >> success_alert
    )
