import shutil
from datetime import datetime
from pathlib import Path


SOURCE_FILE = Path("data/source/json/customer_updates.json")
LANDING_DIR = Path("data/landing/json")


def ingest_json():
    if not SOURCE_FILE.exists():
        raise FileNotFoundError(
            f"Source file not found: {SOURCE_FILE}"
        )

    LANDING_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    landing_file = (
        LANDING_DIR
        / f"customer_updates_{timestamp}.json"
    )

    shutil.copy2(SOURCE_FILE, landing_file)

    print("Ingestion completed")
    print(f"Source : {SOURCE_FILE}")
    print(f"Landing: {landing_file}")


if __name__ == "__main__":
    ingest_json()