import re
import csv
import pandas as pd
from io import StringIO
from pathlib import Path

SQL_FILE = Path("toview.sql")
OUTPUT_XLSX = Path("output.xlsx")

insert_re = re.compile(
    r"INSERT\s+INTO\s+\[?(?P<schema>\w+)\]?\.\[?(?P<table>\w+)\]?"
    r"\s*\((?P<cols>[^)]+)\)\s*VALUES\s*(?P<all_vals>\([^\;]+?\))\s*;?",
    flags=re.IGNORECASE | re.DOTALL
)

def sanitize_cell(val):
    if isinstance(val, str):
        return re.sub(r'[\x00-\x1F]+', ' ', val)
    return val

sql_text = SQL_FILE.read_text(encoding="utf-8")
table_data = {}

for m in insert_re.finditer(sql_text):
    table = m.group("table")
    cols  = [c.strip().strip("[]") for c in m.group("cols").split(",")]
    raw_vals = m.group("all_vals")

    tuples = re.findall(r"\([^\)]*\)", raw_vals, flags=re.DOTALL)
    for tup in tuples:
        body = tup[1:-1].strip()
        reader = csv.reader(
            StringIO(body),
            delimiter=",",
            quotechar="'",
            skipinitialspace=True,
            strict=True
        )
        try:
            parts = next(reader)
        except Exception as e:
            print(f"[{table}] CSV parse error for {tup!r}: {e}")
            continue

        vals = [p.strip() for p in parts]
        if len(vals) != len(cols):
            print(f"[{table}] skipping row: {len(cols)} cols vs {len(vals)} vals")
            continue

        tbl = table_data.setdefault(table, {"cols": cols, "rows": []})
        tbl["rows"].append(vals)

with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    for table, info in table_data.items():
        df = pd.DataFrame(info["rows"], columns=info["cols"])
        for col in df.columns:
            try:
                df[col] = pd.to_numeric(df[col])
            except Exception:
                pass
        df = df.applymap(sanitize_cell)
        df.to_excel(writer, sheet_name=table, index=False)

print(f"Wrote {len(table_data)} tables to {OUTPUT_XLSX.resolve()}")
