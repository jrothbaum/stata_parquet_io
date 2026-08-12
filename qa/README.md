# pq QA

The `pq` QA suite covers Parquet metadata round trips, adversarial regressions, generic-reader compatibility, malformed metadata rejection, and release-surface contracts. One curated runner drives the flat suite, and every test file is independently runnable from this directory.

## How to run

```bash
cd qa
stata-mp -b do run_all.do            # full lane (default release gate)
stata-mp -b do run_all.do quick      # focused regression lane
stata-mp -b do test_regressions.do   # one suite standalone
stata-mp -b do test_stata_metadata_multifile.do
cargo llvm-cov --lib --lcov --output-path /tmp/pq.lcov
python3 check_stata_metadata.py coverage /tmp/pq.lcov
```

Build the release plugin first with `cargo build --release`. Read the terminal `RESULT:` line because Stata batch mode does not reliably expose a failing do-file through the shell status.

## Conventions

- `test_*` files contain functional, regression, and package-contract checks; there is no estimator or external numerical oracle requiring `validation_*` or `crossval_*` files.
- Every runnable suite ends with `RESULT: <name> tests=N pass=N fail=N` and exits nonzero on a recorded failure.
- Suites load `pq.ado` from `../src/ado/p` and bind the locally built release plugin from `../target/release`; they do not modify the user's installed ado packages.
- Paths derive from `c(pwd)` and contain no machine-local directories.
- Test data are generated at runtime; the Python helper creates only temporary malformed-footer and generic-reader artifacts.
- Generated logs, Parquet files, sentinels, and Python caches are gitignored.

## Dependencies

| Suite or lane | Needs | If missing |
|---|---|---|
| All lanes | Stata 16+ and `target/release/libstata_parquet_io.so` | Hard failure |
| `test_stata_metadata.do`, `test_stata_metadata_multifile.do` | Python 3 with `pyarrow` | Hard failure |

## File index

### Functional and regression tests

| File | Covers |
|---|---|
| `test_regressions.do` | Automatic `.parquet` resolution across save/use/describe/append/merge, variable-label-only metadata, `drop_strl` metadata filtering, and long physical names on strL columns. |
| `test_stata_metadata.do` | Numeric and label round trips, projection, opt-out, malformed metadata, merge scope, empty data, and rejected option compositions. |
| `test_stata_metadata_multifile.do` | Partition, chunk, stream, consolidation, combined writer modes, small/unlabeled state, plain/mixed/conflicting extension preflight, `pq append`, empty inputs, selection/filter/sample/cast/compress reads, relaxed schemas, long names, mixed/conflicting footers, transaction, and generic-reader contracts. |
| `test_package_release.do` | Help rendering, version/date and documentation sync, package-manifest targets, QA structure, and the targeted Rust lint regression. |

### Support

| Path | Contents |
|---|---|
| `run_all.do` | Curated `quick`, `core`, and `full` lane runner. |
| `check_stata_metadata.py` | Independent PyArrow inspection, malformed-footer fixture construction, and the 100% metadata-module line-coverage gate. |
| `.gitignore` | Generated QA artifact policy. |

## Coverage map

| Command surface | Functional | Package contracts | Also exercised in |
|---|---|---|---|
| `pq save, statametadata` | `test_regressions`, `test_stata_metadata` | `test_package_release` | Rust unit tests in `src/stata_metadata.rs` |
| Multi-file save modes | `test_stata_metadata_multifile` | `test_package_release` | PyArrow checks in `check_stata_metadata.py` |
| `pq use` / `pq append` | `test_regressions`, `test_stata_metadata`, `test_stata_metadata_multifile` | `test_package_release` | Existing scripts under `src/ado/testing/` |
| `pq merge` | `test_stata_metadata` | `test_package_release` | Existing merge scripts under `src/ado/testing/` |

## Lane membership

`quick` ⊆ `core` = `full`; `full` is the default release gate.

| Lane | Suites |
|---|---|
| `quick` | `test_regressions.do` |
| `core` | `quick` plus `test_stata_metadata.do`, `test_stata_metadata_multifile.do`, and `test_package_release.do` |
| `full` | Same correctness gate as `core`; retained as the default lane name for future external-oracle additions. |
