#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pyarrow>=16",
# ]
# ///
"""
Create a Parquet file from Python that carries Stata variable metadata
(dataset/variable labels, value labels, notes, display formats and storage
types) so that `pq use` in Stata restores all of it -- and so that a file
`pq save` writes can be read straight back in Python.

The metadata is a single JSON string stored under the Parquet key/value
metadata key ``org.stata.pq.labels.v1``.  This is exactly the envelope
`pq save` writes and `pq use` reads (see ``src/stata_metadata.rs`` upstream),
so files are interoperable both ways:

    Python  --write-->  Parquet  --`pq use`-->  Stata  --`pq save`-->  Parquet

JSON schema (every field except ``version`` is optional; omit rather than
send an empty value, mirroring how Stata serializes it):

    {
      "version": 1,
      "dataset_label": "<dataset label>",
      "dataset_notes": ["<note 1>", ...],
      "variables": {
        "<parquet column name>": {
          "label":       "<variable label>",
          "value_label": "<name of a value label in value_labels below>",
          "notes":       ["<note 1>", ...],
          "format":      "<Stata display format, e.g. %9.2f>",
          "stata_type":  "byte|int|long|float|double|date|time|datetime"
        }
      },
      "value_labels": {
        "<value label name>": {"0": "Control", "1": "Treatment"}
      }
    }

Notes that match the Rust implementation:
  * ``variables`` is keyed by the *physical Parquet column name*.
  * Value-label codes are STRINGS holding integers; value labels apply only
    to numeric columns. Codes that never appear in the data are preserved.
  * ``stata_type`` is a *hint*. On read Stata only trusts it when the
    Parquet row-group statistics confirm the data still fits; it can widen
    the type but never narrow it below what the real values need. Strings,
    strL and binary carry no ``stata_type``.
  * Use column names that are legal Stata variable names (<=32 chars, no
    leading digit) to avoid a rename on import; renamed columns re-attach
    their metadata via the ``_pq_parquet_name`` characteristic.

Requires: pyarrow (``uv run`` installs it automatically from the header above).

Run it to produce the demo file the round-trip example uses:

    uv run examples/python_stata_metadata.py python_made.parquet
"""

import json
import sys

import pyarrow as pa
import pyarrow.parquet as pq

STATA_META_KEY = b"org.stata.pq.labels.v1"
STATA_META_VERSION = 1


def build_envelope(
    variables=None,
    value_labels=None,
    dataset_label=None,
    dataset_notes=None,
):
    """Assemble a metadata envelope, omitting empty fields (as Stata does)."""
    env = {"version": STATA_META_VERSION}
    if dataset_label:
        env["dataset_label"] = dataset_label
    if dataset_notes:
        env["dataset_notes"] = list(dataset_notes)
    if variables:
        # Drop per-variable fields that are empty so the envelope matches
        # the canonical form Stata writes back out.
        cleaned = {}
        for name, var in variables.items():
            v = {k: val for k, val in var.items() if val not in (None, "", [], {})}
            cleaned[name] = v
        env["variables"] = cleaned
    if value_labels:
        env["value_labels"] = {
            name: {str(code): text for code, text in defn.items()}
            for name, defn in value_labels.items()
        }
    return env


def write_stata_parquet(path, table, envelope, existing_metadata=None):
    """Write `table` to `path`, embedding `envelope` as Stata metadata.

    table    : pyarrow.Table
    envelope : dict built with build_envelope() (or a raw dict in the schema)
    """
    schema_meta = dict(existing_metadata or {})
    schema_meta[STATA_META_KEY] = json.dumps(envelope).encode("utf-8")
    table = table.replace_schema_metadata(schema_meta)
    pq.write_table(table, path)


def read_stata_metadata(path):
    """Return the parsed Stata metadata envelope, or None if the file has none.

    Reads the Parquet *footer* key/value metadata, which is where both this
    writer and Stata's `pq save` store the envelope. Do NOT use
    ``pq.read_schema(path).metadata`` here: that returns the Arrow *schema*
    metadata, and a file written by polars/Stata carries the key only in the
    footer -- it would read back as None.
    """
    kv = pq.ParquetFile(path).metadata.metadata or {}
    raw = kv.get(STATA_META_KEY)
    if raw is None:
        return None
    return json.loads(raw)


def _demo_table_and_envelope():
    """A small mixed-type dataset plus a representative metadata envelope.

    Column storage types and display formats are chosen to match what Stata
    will re-derive on `pq save`, so the round trip comes back unchanged:
      int32  -> Stata "long"   (%9.0g)
      float64-> Stata "double" (%9.2f)
      string -> no stata_type  (strings carry none)
    """
    table = pa.table(
        {
            "id": pa.array([1, 2, 3, 4], pa.int32()),
            "grp": pa.array([0, 1, 1, 0], pa.int32()),
            "score": pa.array([1.5, 2.0, 3.25, 4.0], pa.float64()),
            "name": pa.array(["alpha", "bravo", "charlie", "delta"], pa.string()),
        }
    )
    variables = {
        "id": {"label": "Row identifier", "format": "%9.0g", "stata_type": "long"},
        "grp": {
            "label": "Treatment group",
            "value_label": "grplbl",
            "format": "%9.0g",
            "stata_type": "long",
            "notes": ["0 = control, 1 = treatment"],
        },
        "score": {"label": "Test score", "format": "%9.2f", "stata_type": "double"},
        "name": {"label": "Participant name"},
    }
    value_labels = {"grplbl": {0: "Control", 1: "Treatment"}}
    envelope = build_envelope(
        variables=variables,
        value_labels=value_labels,
        dataset_label="Python-authored demo dataset",
        dataset_notes=["Written by examples/python_stata_metadata.py"],
    )
    return table, envelope


def main(argv):
    path = argv[1] if len(argv) > 1 else "python_made.parquet"
    table, envelope = _demo_table_and_envelope()
    write_stata_parquet(path, table, envelope)

    # Self-check: what we read back must byte-for-byte match what we wrote.
    back = read_stata_metadata(path)
    assert back == json.loads(json.dumps(envelope)), "Python read-back mismatch"

    print(f"Wrote {path} with Stata metadata under {STATA_META_KEY.decode()}:")
    print(json.dumps(back, indent=2))
    print(
        "\nNext: in Stata run examples/python_roundtrip.do, then verify with\n"
        f"    uv run examples/python_verify.py {path} stata_resaved.parquet"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
