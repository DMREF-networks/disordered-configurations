"""
Ingests data from the guest collection. This needs to be run everytime the guest collection is updated. 
The output from this script goes to ingest_data.json. 
"""

import requests
import pandas as pd
from io import BytesIO
import json
import math

# GLOBUS location to ingest from
URL = "https://g-387955.7ce1a.03c0.data.globus.org/ConfigLib/ConfigLib_20250922.xlsx"

# download
response = requests.get(URL)
response.raise_for_status()

excel = pd.ExcelFile(BytesIO(response.content))

# library sheet header starts at row 5 (zero-based index header=4)
df = pd.read_excel(excel, sheet_name="Library", header=3)

# strip whitespace from column names
df.columns = [str(c).strip() for c in df.columns]

# drop fully empty rows and rows that don't have PC Name
df = df.dropna(how="all")
if "PC Name" not in df.columns:
    raise ValueError("Expected column 'PC Name' not found")
df = df[df["PC Name"].notna()]

def normalize_val(x):
    """
    Return:
      - None for NaN, empty strings, '" "', '"\"\""' etc.
      - For numeric types: return string with no trailing .0 (e.g., 1024.0 -> "1024")
      - For other types: stripped string
    """
    # pandas / numpy NaN
    if pd.isna(x):
        return None

    # convert booleans / ints / floats to normalized strings
    if isinstance(x, (int,)):
        return str(x)
    if isinstance(x, float):
        # prefer integer-like representation for floats with no fractional part
        if math.isfinite(x) and x.is_integer():
            return str(int(x))
        if math.isfinite(x):
            # remove trailing zeros
            s = repr(x)
            # simple formatting: strip trailing .0 if present
            if s.endswith(".0"):
                return s[:-2]
            return s
        return None

    # strings
    s = str(x).strip()

    # common empty-ish tokens we observed in Excel
    if s == "" or s.lower() == "nan" or s.lower() == "none":
        return None
    # Some cells contain literal double-quotes like: "" or "\"\"" or '""'
    if s in ('""', '"', "''"):
        return None
    # If someone typed just a pair of quotes with whitespace
    if s.strip('"').strip() == "":
        return None

    return s

def make_tags(row):

    """
    Build a cleaned tags list:
      [# of Nodes, Dim, Generator, Adjacency Method, Perturbation Method, Start-End]
    but omit any entries that normalize to None or empty.
    """
    fields = [
        ("Name", row.get("PC Name")),
        ("# of Nodes", row.get("# of Nodes")),
        ("Dim", row.get("Dim")),
        ("Generator", row.get("Generator")),
        ("Adjacency Method", row.get("Adjacency Method")),
        ("Perturbation Method", row.get("Perturbation Method")),
    ]

    tags = []
    for name, value in fields:
        nv = normalize_val(value)
        if nv is not None:
            tags.append(nv)

    # start/end handling: only include if at least one side exists
    start = normalize_val(row.get("Start Param"))
    end = normalize_val(row.get("End Param"))
    if start is not None and end is not None:
        tags.append(f"{start}-{end}")
    elif start is not None:
        tags.append(str(start))
    elif end is not None:
        tags.append(str(end))

    return tags

gmeta_list = []
for _, raw_row in df.iterrows():
    # normalize important fields
    pc_name = normalize_val(raw_row.get("PC Name"))
    if not pc_name:
        # skip rows that don't have a usable subject
        continue

    creator = normalize_val(raw_row.get("Creator"))
    date_created = normalize_val(raw_row.get("Date Created"))
    notes = normalize_val(raw_row.get("Notes"))
    group = normalize_val(raw_row.get("File Name Prefix"))

    # Build download URL
    download_url = "https://g-387955.7ce1a.03c0.data.globus.org/ConfigLib/DATA-zip/" + raw_row.get("PC Name") + ".zip"

    # Image needs to be added to this later
    entry = {
        "subject": pc_name,
        "visible_to": ["public"],
        "content": {
            "creator": creator,
            "date_created": str(date_created) if date_created is not None else None,
            "notes": notes,
            "name": pc_name,
            "tags": make_tags(raw_row),
            "group": group,
            "website": download_url
        }
    }
    gmeta_list.append(entry)

output = {"ingest_type": "GMetaList", "ingest_data": {"gmeta": gmeta_list}}

with open("ingest_data.json", "w") as f:
    # ensure NaN is not written; json.dump will turn None -> null
    json.dump(output, f, indent=2, ensure_ascii=False)

print("Generated ingest_data.json with", len(gmeta_list), "entries.")
