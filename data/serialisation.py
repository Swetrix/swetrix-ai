import json
import re


def serialise_predictions(data):
    """Process predictions from a list of dictionaries after the result of model's prediction"""

    results = []

    # Function to split the key and categorize
    def categorize_key(key):
        match = re.match(r"(\w+)_([\w-]+)_next_(\d+)_hr", key)
        if match:
            category, field, hour = match.groups()
            return category, field, f"next_{hour}_hour"
        return None, None, None

    for record in data:
        pid = record.get("pid")

        hours = {}
        for key, value in record.items():
            if value == 0:
                continue
            category, field, hour = categorize_key(key)
            if category and field and hour:
                if hour not in hours:
                    hours[hour] = {}
                if category not in hours[hour]:
                    hours[hour][category] = {}
                hours[hour][category][field] = value

        result = {"pid": pid, **hours}

        results.append(result)

    return results


def serialise_data_for_clickhouse(data):
    """Serialise the processed data for ClickHouse insertion"""
    serialized_data = []

    for record in data:
        pid = record["pid"]
        next_1_hour = json.dumps(record.get("next_1_hour", {}))
        next_4_hour = json.dumps(record.get("next_4_hour", {}))
        next_8_hour = json.dumps(record.get("next_8_hour", {}))
        next_12_hour = json.dumps(record.get("next_12_hour", {}))
        next_24_hour = json.dumps(record.get("next_24_hour", {}))
        next_72_hour = json.dumps(record.get("next_72_hour", {}))
        next_168_hour = json.dumps(record.get("next_168_hour", {}))

        serialized_data.append(
            (
                pid,
                next_1_hour,
                next_4_hour,
                next_8_hour,
                next_12_hour,
                next_24_hour,
                next_72_hour,
                next_168_hour,
            )
        )

    return serialized_data
