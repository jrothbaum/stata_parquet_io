# Interop examples

How to exchange Stata metadata with other tools through Parquet.

`pq save ..., statametadata` stores Stata's variable metadata — dataset and
variable labels, value labels, notes, display formats and storage types — as
a single JSON string in the **Parquet file-level (footer) key/value metadata**,
under the key:

```
org.stata.pq.labels.v1
```

Any tool that can read/write Parquet footer metadata can therefore produce
files that `pq use` restores in full, and can read back everything `pq save`
wrote. The envelope (every field except `version` optional) is:

```json
{
  "version": 1,
  "dataset_label": "…",
  "dataset_notes": ["…"],
  "variables": {
    "<parquet column name>": {
      "label":       "…",
      "value_label": "<name of a value label below>",
      "notes":       ["…"],
      "format":      "%9.2f",
      "stata_type":  "byte|int|long|float|double|date|time|datetime"
    }
  },
  "value_labels": {
    "<value label name>": {"0": "Control", "1": "Treatment"}
  }
}
```

Notes:
- `variables` is keyed by the physical Parquet column name.
- Value-label codes are strings holding integers; value labels apply to
  numeric columns only. Codes absent from the data are preserved.
- `stata_type` is a hint: on read it is trusted only when the row-group
  statistics confirm the values still fit; it can widen the type but not
  narrow it. Strings/strL/binary carry none.
- Store the JSON in the **footer** KV metadata (e.g. pyarrow's
  `Table.replace_schema_metadata`, which writes it there), not only in an
  Arrow-schema slot.

## Python

- [`python_stata_metadata.py`](python_stata_metadata.py) — write / read a
  Parquet file carrying Stata metadata (run it to create a demo file).
- [`python_roundtrip.do`](python_roundtrip.do) — `pq use` the Python file,
  then `pq save ..., statametadata`.
- [`python_verify.py`](python_verify.py) — assert the
  Python → Stata → Python round trip preserved both data and metadata.

```bash
uv run examples/python_stata_metadata.py python_made.parquet
# ...run examples/python_roundtrip.do in Stata...
uv run examples/python_verify.py python_made.parquet stata_resaved.parquet
```

## R

- [`r_stata_metadata.R`](r_stata_metadata.R) — write / read / verify the same
  envelope from R via the `arrow` package.

```r
Rscript examples/r_stata_metadata.R write  r_made.parquet
Rscript examples/r_stata_metadata.R verify r_made.parquet other.parquet
```

Because the envelope is tool-agnostic, R and Python interoperate directly. A
cross-tool round trip (no Stata needed) — R writes, Python re-emits, R checks:

```bash
Rscript examples/r_stata_metadata.R write r_made.parquet
uv run examples/python_stata_metadata.py --passthrough r_made.parquet py_from_r.parquet
Rscript examples/r_stata_metadata.R verify r_made.parquet py_from_r.parquet
```

`requires: arrow, jsonlite` (`install.packages(c("arrow", "jsonlite"))`).
