from __future__ import annotations

import csv
from pathlib import Path


INPUT_FILE = Path(__file__).with_name("Bank Customer Churn Prediction.csv")
OUTPUT_FILE = Path(__file__).with_name("Bank Customer Churn Prediction_cleaned.csv")

EXPECTED_COLUMNS = [
    "customer_id",
    "credit_score",
    "country",
    "gender",
    "age",
    "tenure",
    "balance",
    "products_number",
    "credit_card",
    "active_member",
    "estimated_salary",
    "churn",
]

INTEGER_COLUMNS = {
    "customer_id",
    "credit_score",
    "age",
    "tenure",
    "products_number",
    "credit_card",
    "active_member",
    "churn",
}

FLOAT_COLUMNS = {"balance", "estimated_salary"}


def normalize_row(row: dict[str, str]) -> dict[str, str]:
    cleaned_row: dict[str, str] = {}

    for column in EXPECTED_COLUMNS:
        value = row.get(column, "")
        value = "" if value is None else value.strip()
        if value == "":
            raise ValueError(f"Missing value in required column: {column}")

        if column in INTEGER_COLUMNS:
            cleaned_row[column] = str(int(float(value)))
        elif column in FLOAT_COLUMNS:
            cleaned_row[column] = f"{float(value):.2f}".rstrip("0").rstrip(".")
        else:
            cleaned_row[column] = value

    return cleaned_row


def main() -> None:
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_FILE}")

    seen_rows: set[tuple[str, ...]] = set()
    cleaned_rows: list[dict[str, str]] = []
    skipped_blank = 0
    skipped_duplicates = 0

    with INPUT_FILE.open("r", newline="", encoding="utf-8-sig") as input_handle:
        reader = csv.DictReader(input_handle)

        if reader.fieldnames is None:
            raise ValueError("The input file does not contain a header row.")

        actual_columns = [column.strip() for column in reader.fieldnames]
        if actual_columns != EXPECTED_COLUMNS:
            raise ValueError(
                "Unexpected columns in source file.\n"
                f"Expected: {EXPECTED_COLUMNS}\n"
                f"Found:    {actual_columns}"
            )

        for raw_row in reader:
            if not any((value or "").strip() for value in raw_row.values()):
                skipped_blank += 1
                continue

            cleaned_row = normalize_row(raw_row)
            row_key = tuple(cleaned_row[column] for column in EXPECTED_COLUMNS)

            if row_key in seen_rows:
                skipped_duplicates += 1
                continue

            seen_rows.add(row_key)
            cleaned_rows.append(cleaned_row)

    with OUTPUT_FILE.open("w", newline="", encoding="utf-8") as output_handle:
        writer = csv.DictWriter(output_handle, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f"Read {len(cleaned_rows) + skipped_blank + skipped_duplicates} rows from {INPUT_FILE.name}")
    print(f"Wrote {len(cleaned_rows)} cleaned rows to {OUTPUT_FILE.name}")
    print(f"Skipped blank rows: {skipped_blank}")
    print(f"Skipped duplicate rows: {skipped_duplicates}")


if __name__ == "__main__":
    main()