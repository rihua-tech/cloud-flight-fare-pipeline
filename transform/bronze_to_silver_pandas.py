"""Bronze -> Silver transform (pandas version).

In production, this could be Spark/Glue. For a portfolio demo, pandas is enough.

This script is optional because dbt can also perform light transformations, but
keeping it demonstrates a typical medallion workflow (bronze -> silver -> marts).
"""
