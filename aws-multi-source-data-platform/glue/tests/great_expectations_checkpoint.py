"""
Great Expectations Checkpoint — AWS Multi-Source Data Platform
Runs between Silver and Gold zones to enforce data quality gates.

Challenge solved: tests passing but wrong data in dashboards.
Solution: business logic expectations beyond structural schema tests.
"""

import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

import great_expectations as gx
from great_expectations.core.batch import BatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_expectation_suite(context: gx.DataContext) -> str:
    """
    Define all expectations for the orders curated dataset.
    Covers both structural correctness AND business logic correctness.
    """
    suite_name = "orders_curated_quality_suite"

    suite = context.add_or_update_expectation_suite(suite_name)

    validator = context.get_validator(
        batch_request=BatchRequest(
            datasource_name     = "redshift_datasource",
            data_connector_name = "default_inferred_data_connector_name",
            data_asset_name     = "curated.orders_curated",
        ),
        expectation_suite_name=suite_name,
    )

    # ── Structural Expectations ───────────────────────────────────────────────

    # Required columns must not be null
    for col in ["event_id", "order_id", "customer_id", "total_amount",
                "event_timestamp", "status", "region"]:
        validator.expect_column_values_to_not_be_null(column=col)

    # event_id must be unique — no duplicates
    validator.expect_column_values_to_be_unique(column="event_id")

    # Status must be a known value
    validator.expect_column_values_to_be_in_set(
        column  = "status",
        value_set = ["PENDING", "CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
    )

    # Region must be a known value
    validator.expect_column_values_to_be_in_set(
        column    = "region",
        value_set = ["eu-west", "eu-central", "us-east", "us-west", "ap-southeast"]
    )

    # ── Business Logic Expectations ───────────────────────────────────────────

    # Order amounts must be positive — a zero or negative amount is a data error
    validator.expect_column_values_to_be_between(
        column    = "total_amount",
        min_value = 0.01,
        max_value = 100000,
        mostly    = 1.0,   # 100% of rows must pass
    )

    # Quantity must be a positive integer
    validator.expect_column_values_to_be_between(
        column    = "quantity",
        min_value = 1,
        max_value = 10000,
    )

    # Unit price must be positive
    validator.expect_column_values_to_be_between(
        column    = "unit_price",
        min_value = 0.01,
        max_value = 50000,
    )

    # Statistical distribution: total_amount should be within expected range
    # Catches sudden shifts in data distribution — e.g. currency conversion bug
    validator.expect_column_median_to_be_between(
        column    = "total_amount",
        min_value = 10,
        max_value = 1000,
    )

    # total_amount should approximately equal unit_price × quantity
    # (allows for discounts/fees but catches gross calculation errors)
    validator.expect_column_pair_values_a_to_be_greater_than_b(
        column_A  = "total_amount",
        column_B  = "unit_price",  # total should be >= unit price (quantity >= 1)
        or_equal  = True,
        mostly    = 0.95,          # allow 5% variance for discounts
    )

    # Event timestamps must be within a reasonable time window
    # Catches events with wrong years (e.g. Unix epoch 1970) or future dates
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    tomorrow  = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()

    validator.expect_column_values_to_be_between(
        column    = "event_timestamp",
        min_value = "2024-01-01T00:00:00+00:00",
        max_value = tomorrow,
        mostly    = 0.999,   # allow 0.1% for edge cases
    )

    # Row count must be non-zero — no silent empty loads
    validator.expect_table_row_count_to_be_between(
        min_value = 1,
        max_value = None,   # no upper bound
    )

    # Column count must match expected schema
    validator.expect_table_column_count_to_equal(value=14)

    validator.save_expectation_suite(discard_failed_expectations=False)
    logger.info("Expectation suite saved: %s", suite_name)

    return suite_name


def run_checkpoint(
    datasource_name: str = "redshift_datasource",
    suite_name: str      = "orders_curated_quality_suite",
    run_name: str        = None,
) -> Dict[str, Any]:
    """
    Run the Great Expectations checkpoint and return results.
    Raises ValueError if any expectation fails — blocks pipeline promotion.
    """
    context = gx.get_context()

    if run_name is None:
        run_name = f"orders_quality_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"

    checkpoint = SimpleCheckpoint(
        name            = "orders_curated_checkpoint",
        data_context    = context,
        validations     = [{
            "batch_request": BatchRequest(
                datasource_name     = datasource_name,
                data_connector_name = "default_inferred_data_connector_name",
                data_asset_name     = "curated.orders_curated",
            ),
            "expectation_suite_name": suite_name,
        }],
        run_name_template = run_name,
    )

    results = checkpoint.run()

    success        = results.success
    failed_count   = sum(
        1 for v in results.run_results.values()
        for r in v["validation_result"].results
        if not r.success
    )

    logger.info(
        "Quality checkpoint complete | success=%s failed_expectations=%d run=%s",
        success, failed_count, run_name
    )

    if not success:
        failed_details = [
            {
                "expectation": r.expectation_config.expectation_type,
                "column":      r.expectation_config.kwargs.get("column", "table-level"),
                "observed":    r.result.get("observed_value"),
            }
            for v in results.run_results.values()
            for r in v["validation_result"].results
            if not r.success
        ]

        raise ValueError(
            f"Data quality checkpoint FAILED — {failed_count} expectations violated.\n"
            f"Failed expectations: {failed_details}\n"
            f"Pipeline promotion to Gold layer is BLOCKED."
        )

    return {"success": True, "run_name": run_name, "failed_count": 0}


if __name__ == "__main__":
    context    = gx.get_context()
    suite_name = build_expectation_suite(context)
    results    = run_checkpoint(suite_name=suite_name)
    logger.info("All quality checks passed: %s", results)
