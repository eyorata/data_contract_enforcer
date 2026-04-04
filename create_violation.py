import json
import random


def main():
    src = "outputs/week3/extractions.jsonl"
    dst = "outputs/week3/extractions_violated.jsonl"

    with open(src, "r", encoding="utf-8") as f:
        lines = [l for l in f if l.strip()]

    records = [json.loads(l) for l in lines]

    # Inject a confidence scale violation on a subset of facts
    for rec in records[:10]:
        for fact in rec.get("extracted_facts", []):
            if "confidence" in fact:
                fact["confidence"] = round(float(fact["confidence"]) * 100, 4)

    with open(dst, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")

    print(f"Wrote violated dataset: {dst}")


if __name__ == "__main__":
    main()
