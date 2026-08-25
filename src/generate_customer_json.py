import json
import random
from datetime import datetime, timedelta
from pathlib import Path


OUTPUT_FILE = Path("data/source/json/customer_updates.json")
NUM_RECORDS = 10_000


cities = {
    "British Columbia": ["Vancouver", "Victoria", "Kelowna"],
    "Alberta": ["Calgary", "Edmonton"],
    "Ontario": ["Toronto", "Ottawa"],
    "Quebec": ["Montreal", "Quebec City"],
    "Manitoba": ["Winnipeg"],
    "Nova Scotia": ["Halifax"],
}


province_abbreviations = {
    "British Columbia": "BC",
    "Alberta": "AB",
    "Ontario": "ON",
    "Quebec": "QC",
    "Manitoba": "MB",
    "Nova Scotia": "NS",
}


records = []

start_date = datetime(2026, 1, 1)

for customer_id in range(1, NUM_RECORDS + 1):

    province = random.choice(list(cities.keys()))
    city = random.choice(cities[province])

    email = f"customer{customer_id}@retailco.com"

    # Introduce some intentional data-quality problems
    issue = random.randint(1, 100)

    if issue <= 5:
        email = f" CUSTOMER{customer_id}@RETAILCO.COM "

    elif issue <= 8:
        email = f"customer{customer_id}@retailco"

    if issue <= 10:
        city = city.lower()

    elif issue <= 15:
        city = f" {city} "

    if issue <= 10:
        province_value = province_abbreviations[province]
    else:
        province_value = province

    phone = f"+100000{customer_id:05d}"

    if issue <= 10:
        phone = None

    updated_at = start_date + timedelta(
        minutes=random.randint(0, 60 * 24 * 240)
    )

    record = {
        "customer_id": customer_id,
        "first_name": f"FirstName{customer_id}",
        "last_name": f"LastName{customer_id}",
        "email": email,
        "phone": phone,
        "city": city,
        "province": province_value,
        "updated_at": updated_at.isoformat(),
    }

    records.append(record)


OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(records, f, indent=2)

print(f"Generated {len(records):,} customer records")
print(f"Output: {OUTPUT_FILE}")