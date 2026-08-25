import json

file_path = "data/source/json/customer_updates.json"

with open(file_path, "r", encoding="utf-8") as f:
    data = json.load(f)

print(f"Records loaded: {len(data)}")

for record in data:
    print(record)