# RetailCo — Code commentary guide

Keep the comments short and plain. Explain the rule, the risk, or the boundary. Skip the rest.

## Read in this order

1. src/extract_orders.py — full SQL snapshot pull and landing contract.
2. src/orders_io.py — snapshot selection and safe file replacement.
3. src/bronze_orders.py — landed snapshot to Bronze.
4. src/silver_orders.py — validation, business rules, and customer checks.
5. src/orders_runs.py — run state and file checksums.
6. src/run_orders_pipeline.py — orchestration, retry, and recovery.
7. src/orders_cdc.py — local CDC reducer for order updates.
8. src/resolve_customer_identity.py and src/silver_customer.py — matching and current-state customer merge.
9. src/quarantine_customer_updates.py — unresolved identity rows.
10. src/gold_customer.py and src/gold_customer_order_summary.py — consumer-ready outputs.

## What this code does not guarantee

- This is not SQL Server CDC capture.
- The run state and the Parquet write are not one atomic transaction.
- Customer history is current-state only, not Type 2 history.
- File timestamps are not a reliable source-of-truth feed.
- Finance metrics still need sign-off.

## Comment style

- Top-level docstrings explain what the file is for and where it stops.
- Function docstrings explain what the function does.
- Inline notes flag real risks, assumptions, and recovery issues.
- Leave obvious code alone.
- No long speeches. No fluff. Keep it readable.

## Install this change

Copy the Python files and this guide into your checkout, review the diff, then commit. This archive does not include databases or production data. Run `python -m pytest tests -v`, then commit and push. Do not overwrite `.git`, `.env`, or the local data folder.

## Where this came from

This was assembled from the provided Stage 3C and Stage 4A archives. If GitHub has newer work, compare before overwriting it.
