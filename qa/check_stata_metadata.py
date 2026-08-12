"""Independent PyArrow checks and fixtures for Stata metadata QA."""

# Package: pq
# Author: Timothy P Copeland, Karolinska Institutet

import json
import glob
import hashlib
from pathlib import Path
import shutil
import sys

import pyarrow as pa
import pyarrow.parquet as parquet


METADATA_KEY = b"org.stata.pq.labels.v1"
ARROW_SCHEMA_KEY = b"ARROW:schema"


def resolve_fragments(path_arg: str) -> list[Path]:
    """Resolve one file, a directory, or a glob to sorted Parquet files."""

    path = Path(path_arg)
    if path.is_file():
        candidates = [path]
    elif path.is_dir():
        candidates = list(path.rglob("*.parquet"))
    else:
        candidates = [Path(item) for item in glob.glob(path_arg, recursive=True)]

    files = sorted(
        {
            candidate.resolve()
            for candidate in candidates
            if candidate.is_file() and candidate.suffix.lower() == ".parquet"
        }
    )
    assert files, f"no Parquet fragments resolved from {path_arg!r}"
    return files


def inspect_dataset(
    path_arg: str,
    sentinel_path: Path,
    expected_files: int | None,
    expected_rows: int | None,
    require_stata_metadata: bool = True,
    expected_id_start: int = 1,
) -> None:
    """Check every physical fragment, footer, and generic-reader data path."""

    fragments = resolve_fragments(path_arg)
    if expected_files is not None:
        assert len(fragments) == expected_files, (len(fragments), expected_files)

    envelopes: list[bytes] = []
    row_count = 0
    ids: list[int] = []
    for fragment in fragments:
        metadata = parquet.read_metadata(fragment).metadata or {}
        if require_stata_metadata:
            assert METADATA_KEY in metadata, f"missing Stata metadata in {fragment}"
            envelopes.append(metadata[METADATA_KEY])
        else:
            assert METADATA_KEY not in metadata, f"unexpected Stata metadata in {fragment}"
        assert ARROW_SCHEMA_KEY in metadata, f"missing ARROW:schema in {fragment}"

        # ParquetFile reads the physical fragment only. read_table(path) also
        # infers Hive keys from parent directories such as group=1, which can
        # collide with a stored partition column and obscure the writer result.
        table = parquet.ParquetFile(fragment).read()
        row_count += table.num_rows
        if "id" in table.schema.names:
            ids.extend(table.column("id").to_pylist())
        for field in table.schema:
            if field.name in {"status", "status_copy"}:
                assert pa.types.is_integer(field.type), (fragment, field)

    if require_stata_metadata:
        assert len(set(envelopes)) == 1, "fragments do not carry one exact envelope"
    if expected_rows is not None:
        assert row_count == expected_rows, (row_count, expected_rows)
        if ids:
            assert sorted(ids) == list(
                range(expected_id_start, expected_id_start + expected_rows)
            ), ids

    sentinel_path.write_text(
        f"fragments={len(fragments)} rows={row_count}\n", encoding="utf-8"
    )


def assemble_dataset(output_dir: Path, sources: list[Path], sentinel_path: Path) -> None:
    assert sources, "assemble requires at least one source"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)
    for index, source in enumerate(sources, start=1):
        assert source.is_file(), source
        shutil.copy2(source, output_dir / f"data_{index}.parquet")
    sentinel_path.write_text("dataset assembled\n", encoding="utf-8")


def strip_stata_metadata(source_path: Path, output_path: Path, sentinel_path: Path) -> None:
    table = parquet.read_table(source_path)
    metadata = dict(table.schema.metadata or {})
    metadata.pop(METADATA_KEY, None)
    parquet.write_table(table.replace_schema_metadata(metadata), output_path)
    assert METADATA_KEY not in (parquet.read_metadata(output_path).metadata or {})
    sentinel_path.write_text("Stata metadata removed\n", encoding="utf-8")


def snapshot_dataset(path_arg: str, output_path: Path) -> None:
    root = Path(path_arg)
    files = resolve_fragments(path_arg)
    payload = {
        "path_is_directory": root.is_dir(),
        "files": [
            {
                "path": str(file.relative_to(root) if root.is_dir() else file.name),
                "sha256": hashlib.sha256(file.read_bytes()).hexdigest(),
            }
            for file in files
        ],
    }
    output_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def compare_snapshot(path_arg: str, snapshot_path: Path, sentinel_path: Path) -> None:
    expected = json.loads(snapshot_path.read_text(encoding="utf-8"))
    current_path = snapshot_path.with_suffix(".current.json")
    try:
        snapshot_dataset(path_arg, current_path)
        current = json.loads(current_path.read_text(encoding="utf-8"))
        assert current == expected, (current, expected)
    finally:
        current_path.unlink(missing_ok=True)
    sentinel_path.write_text("dataset unchanged\n", encoding="utf-8")


def compare_envelopes(
    first_path: Path, second_path: Path, relation: str, sentinel_path: Path
) -> None:
    first = (parquet.read_metadata(first_path).metadata or {})[METADATA_KEY]
    second = (parquet.read_metadata(second_path).metadata or {})[METADATA_KEY]
    if relation == "same":
        assert first == second, "expected identical Stata metadata envelopes"
    elif relation == "different":
        assert first != second, "expected different Stata metadata envelopes"
    else:
        raise ValueError(f"Unknown envelope relation: {relation}")
    sentinel_path.write_text(f"envelopes {relation}\n", encoding="utf-8")


def reset_workdir(path: Path) -> None:
    assert path.name.startswith("pq_metadata_multifile_"), path
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)


def remove_workdir(path: Path) -> None:
    assert path.name.startswith("pq_metadata_multifile_"), path
    if path.exists():
        shutil.rmtree(path)


def check_roundtrip(parquet_path: Path, sentinel_path: Path) -> None:

    table = parquet.read_table(parquet_path)
    assert pa.types.is_integer(table.schema.field("status").type)
    assert table.column("status").to_pylist() == [-1, 1, 1]
    assert table.column("status_copy").to_pylist() == [-1, 1, 1]

    metadata = parquet.read_metadata(parquet_path).metadata or {}
    assert METADATA_KEY in metadata
    sentinel_path.write_text("generic parquet reader passed\n", encoding="utf-8")


def check_column(parquet_path: Path, sentinel_path: Path, column_name: str) -> None:
    table = parquet.read_table(parquet_path)
    assert table.schema.names == [column_name]
    assert pa.types.is_integer(table.schema.field(column_name).type)
    assert METADATA_KEY in (parquet.read_metadata(parquet_path).metadata or {})
    sentinel_path.write_text("long column name passed\n", encoding="utf-8")


def make_malformed(source_path: Path, output_path: Path, sentinel_path: Path) -> None:
    table = parquet.read_table(source_path)
    metadata = dict(table.schema.metadata or {})
    metadata[METADATA_KEY] = json.dumps(
        {
            "format_version": 1,
            "encoding": "zstd+base64",
            "capsule": "",
            "columns": [
                {
                    "parquet_name": table.schema.names[0],
                    "capsule_name": 'x"); display "embedded code"',
                }
            ],
        }
    ).encode("utf-8")
    parquet.write_table(table.replace_schema_metadata(metadata), output_path)
    sentinel_path.write_text("malformed footer fixture written\n", encoding="utf-8")


def check_coverage(lcov_path: Path) -> None:
    target_suffix = "/src/stata_metadata.rs"
    in_target = False
    lines_found: int | None = None
    lines_hit: int | None = None

    for line in lcov_path.read_text(encoding="utf-8").splitlines():
        if line.startswith("SF:"):
            in_target = line[3:].endswith(target_suffix)
        elif in_target and line.startswith("LF:"):
            lines_found = int(line[3:])
        elif in_target and line.startswith("LH:"):
            lines_hit = int(line[3:])

    assert lines_found is not None, "stata_metadata.rs not found in LCOV report"
    assert lines_hit == lines_found, (
        f"stata_metadata.rs line coverage is {lines_hit}/{lines_found}, expected 100%"
    )
    print(f"stata_metadata.rs line coverage: {lines_hit}/{lines_found} (100%)")


def main() -> None:
    mode = sys.argv[1]
    if mode == "roundtrip":
        check_roundtrip(Path(sys.argv[2]), Path(sys.argv[3]))
    elif mode == "column":
        check_column(Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4])
    elif mode == "malformed":
        make_malformed(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
    elif mode == "coverage":
        check_coverage(Path(sys.argv[2]))
    elif mode == "inspect-dataset":
        expected_files = None if sys.argv[4] == "." else int(sys.argv[4])
        expected_rows = None if sys.argv[5] == "." else int(sys.argv[5])
        expected_id_start = int(sys.argv[6]) if len(sys.argv) > 6 else 1
        inspect_dataset(
            sys.argv[2],
            Path(sys.argv[3]),
            expected_files,
            expected_rows,
            expected_id_start=expected_id_start,
        )
    elif mode == "inspect-plain-dataset":
        expected_files = None if sys.argv[4] == "." else int(sys.argv[4])
        expected_rows = None if sys.argv[5] == "." else int(sys.argv[5])
        expected_id_start = int(sys.argv[6]) if len(sys.argv) > 6 else 1
        inspect_dataset(
            sys.argv[2],
            Path(sys.argv[3]),
            expected_files,
            expected_rows,
            require_stata_metadata=False,
            expected_id_start=expected_id_start,
        )
    elif mode == "assemble":
        assemble_dataset(
            Path(sys.argv[2]),
            [Path(item) for item in sys.argv[4:]],
            Path(sys.argv[3]),
        )
    elif mode == "strip":
        strip_stata_metadata(Path(sys.argv[2]), Path(sys.argv[3]), Path(sys.argv[4]))
    elif mode == "snapshot":
        snapshot_dataset(sys.argv[2], Path(sys.argv[3]))
    elif mode == "compare-snapshot":
        compare_snapshot(sys.argv[2], Path(sys.argv[3]), Path(sys.argv[4]))
    elif mode == "compare-envelopes":
        compare_envelopes(
            Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4], Path(sys.argv[5])
        )
    elif mode == "reset-workdir":
        reset_workdir(Path(sys.argv[2]))
    elif mode == "remove-workdir":
        remove_workdir(Path(sys.argv[2]))
    else:
        raise ValueError(f"Unknown mode: {mode}")


if __name__ == "__main__":
    main()
