from deltalake import DeltaTable
import os
import string

TABLE_PATH = os.getenv("TARGET_TABLE_PATH")
RETENTION_HOURS = os.getenv("RETENTION_HOURS")

if TABLE_PATH is None:
    raise Exception("You must set the environment variable $TARGET_TABLE_PATH")

if RETENTION_HOURS is None:
    RETENTION_HOURS = 7 * 24  # Default retention time of 7 days.
else:
    if not all(c in string.digits for c in RETENTION_HOURS):
        raise Exception(
            f"Environment variable RETENTION_HOURS can only contain digits. Current value: {RETENTION_HOURS}"
        )
    RETENTION_HOURS = int(RETENTION_HOURS)

table = DeltaTable(TABLE_PATH)

print(f"Cleaning up delta table {TABLE_PATH}")
print()
print("Optimizing table:")
print(table.optimize.compact())
print("*" * 50)

print(f"Vacuuming table with retention {RETENTION_HOURS}:")
print(
    table.vacuum(
        retention_hours=RETENTION_HOURS, enforce_retention_duration=False, dry_run=False
    )
)
