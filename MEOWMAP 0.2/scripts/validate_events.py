import json, sys, os
from jsonschema import validate, Draft202012Validator

SCHEMA_PATH = os.path.join(os.path.dirname(__file__), "..", "schemas", "events_v2.schema.json")
EVENTS_PATH = os.path.join(os.path.dirname(__file__), "..", "events.json")

def main():
    with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
        schema = json.load(f)
    with open(EVENTS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise SystemExit("events.json must be a JSON array")
    validator = Draft202012Validator(schema)
    errors = []
    for i, ev in enumerate(data):
        for err in validator.iter_errors(ev):
            errors.append(f"[{i}] {err.message}")
    if errors:
        print("Validation errors:")
        for e in errors:
            print("-", e)
        raise SystemExit(1)
    print(f"OK: {len(data)} events validated")

if __name__ == "__main__":
    main()
