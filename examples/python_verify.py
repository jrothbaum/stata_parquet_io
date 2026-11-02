#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "pyarrow>=16",
#   "polars>=1.0",
# ]
# ///
"""
Verify a Python -> Stata -> Python round trip is lossless.

Compares two Parquet files -- the one Python wrote and the one Stata
re-saved after `pq use` -- on both axes:

  1. DATA: every column and value must be identical (polars).
  2. METADATA: the Stata metadata envelope (org.stata.pq.labels.v1) must
     match. ``version``, dataset label/notes, variable labels, value labels
     and notes are compared strictly. ``format`` and ``stata_type`` are
     compared too but reported separately, because they are the fields Stata
     is most likely to normalize (e.g. fill in a default display format a
     Python writer left off); a difference there is printed, not fatal,
     unless --strict is given.

Usage:
    uv run examples/python_verify.py python_made.parquet stata_resaved.parquet
    uv run examples/python_verify.py a.parquet b.parquet --strict

Exit status is 0 only when the files round-tripped within the chosen policy.
"""

import json
import sys

import polars as pl
import pyarrow.parquet as pq

STATA_META_KEY = b"org.stata.pq.labels.v1"
HINT_FIELDS = ("format", "stata_type")


def read_metadata(path):
    # Read the Parquet FOOTER key/value metadata, where both pyarrow and
    # polars/Stata store the envelope. pq.read_schema().metadata is the Arrow
    # *schema* metadata, which a polars/Stata-written file does not populate
    # with this key -- reading there returns None for a perfectly good file.
    kv = pq.ParquetFile(path).metadata.metadata or {}
    raw = kv.get(STATA_META_KEY)
    return json.loads(raw) if raw is not None else None


def split_hint_fields(envelope):
    """Return (core, hints): a copy of the envelope with format/stata_type
    stripped from every variable, plus the stripped hint values keyed by
    variable name."""
    if envelope is None:
        return None, {}
    core = json.loads(json.dumps(envelope))  # deep copy
    hints = {}
    for name, var in core.get("variables", {}).items():
        picked = {f: var.pop(f) for f in HINT_FIELDS if f in var}
        if picked:
            hints[name] = picked
    return core, hints


def diff_lines(a, b, path=""):
    """Yield human-readable differences between two JSON-like structures."""
    if isinstance(a, dict) and isinstance(b, dict):
        for key in sorted(set(a) | set(b)):
            sub = f"{path}.{key}" if path else key
            if key not in a:
                yield f"  only in Stata file : {sub} = {b[key]!r}"
            elif key not in b:
                yield f"  only in Python file: {sub} = {a[key]!r}"
            else:
                yield from diff_lines(a[key], b[key], sub)
    elif a != b:
        yield f"  {path or '(root)'}: python={a!r}  stata={b!r}"


def main(argv):
    args = [a for a in argv[1:] if not a.startswith("--")]
    strict = "--strict" in argv
    if len(args) != 2:
        print(__doc__)
        return 2
    py_path, stata_path = args

    ok = True

    # --- 1. data ---
    py_df = pl.read_parquet(py_path)
    st_df = pl.read_parquet(stata_path)
    if py_df.equals(st_df):
        print(f"DATA     : identical ({py_df.height} rows x {py_df.width} cols)")
    else:
        ok = False
        print("DATA     : DIFFER")
        if py_df.columns != st_df.columns:
            print(f"  columns: python={py_df.columns}  stata={st_df.columns}")
        if py_df.schema != st_df.schema:
            print(f"  schema : python={py_df.schema}  stata={st_df.schema}")
        if py_df.shape != st_df.shape:
            print(f"  shape  : python={py_df.shape}  stata={st_df.shape}")

    # --- 2. metadata ---
    py_meta = read_metadata(py_path)
    st_meta = read_metadata(stata_path)
    py_core, py_hints = split_hint_fields(py_meta)
    st_core, st_hints = split_hint_fields(st_meta)

    if py_core == st_core:
        print("METADATA : core fields identical "
              "(version, labels, value labels, notes)")
    else:
        ok = False
        print("METADATA : core fields DIFFER")
        for line in diff_lines(py_core, st_core):
            print(line)

    if py_hints == st_hints:
        print("HINTS    : format / stata_type identical")
    else:
        msg = "DIFFER (Stata may have normalized these)"
        print(f"HINTS    : {msg}")
        for line in diff_lines(py_hints, st_hints):
            print(line)
        if strict:
            ok = False

    print()
    print("RESULT   :", "round trip is lossless ✓" if ok else "round trip LOST something ✗")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
