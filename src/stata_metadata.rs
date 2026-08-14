use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use polars::prelude::KeyValueMetadata;
use polars_readstat_rs::{readstat_metadata_json, ReadStatFormat};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const STATA_METADATA_KEY: &str = "org.stata.pq.labels.v1";
pub const STATA_METADATA_VERSION: u32 = 1;
const STATA_METADATA_ENCODING: &str = "zstd+base64";

const MAX_COMPRESSED_CAPSULE_BYTES: usize = 16 * 1024 * 1024;
const MAX_DECOMPRESSED_CAPSULE_BYTES: usize = 64 * 1024 * 1024;
const MAX_ENVELOPE_BYTES: usize = 24 * 1024 * 1024;
const MAX_PARQUET_FOOTER_BYTES: u64 = 128 * 1024 * 1024;
static METADATA_TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn format_capsule_compression_error(error: std::io::Error) -> String {
    format!("Could not compress Stata metadata capsule: {error}")
}

fn format_envelope_serialization_error(error: serde_json::Error) -> String {
    format!("Could not serialize Stata metadata envelope: {error}")
}

fn format_zstd_initialization_error(error: std::io::Error) -> String {
    format!("Invalid zstd Stata metadata capsule: {error}")
}

fn format_footer_size_error(error: std::io::Error) -> String {
    format!("Could not determine Parquet file size: {error}")
}

fn format_footer_seek_error(error: std::io::Error) -> String {
    format!("Could not seek to Parquet footer: {error}")
}

fn format_footer_read_error(error: std::io::Error) -> String {
    format!("Could not read Parquet footer trailer: {error}")
}

fn validate_compressed_capsule_size(size: usize) -> Result<(), String> {
    if size > MAX_COMPRESSED_CAPSULE_BYTES {
        return Err(format!(
            "Compressed Stata metadata capsule exceeds the {} MiB limit",
            MAX_COMPRESSED_CAPSULE_BYTES / (1024 * 1024)
        ));
    }
    Ok(())
}

fn validate_decompressed_capsule_size(size: usize) -> Result<(), String> {
    if size > MAX_DECOMPRESSED_CAPSULE_BYTES {
        return Err(format!(
            "Stata metadata capsule exceeds the {} MiB decompressed-size limit",
            MAX_DECOMPRESSED_CAPSULE_BYTES / (1024 * 1024)
        ));
    }
    Ok(())
}

fn validate_envelope_size(size: usize) -> Result<(), String> {
    if size > MAX_ENVELOPE_BYTES {
        return Err(format!(
            "Stata metadata envelope exceeds the {} MiB limit",
            MAX_ENVELOPE_BYTES / (1024 * 1024)
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StataMetadataColumn {
    pub parquet_name: String,
    pub capsule_name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StataMetadataEnvelope {
    format_version: u32,
    encoding: String,
    capsule: String,
    columns: Vec<StataMetadataColumn>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtractedStataMetadata {
    pub format_version: u32,
    pub columns: Vec<StataMetadataColumn>,
}

#[derive(Clone, Debug)]
pub struct PreparedStataMetadata {
    pub envelope_json: String,
    pub key_value_metadata: KeyValueMetadata,
    pub decoded_capsule: Vec<u8>,
    pub columns: Vec<StataMetadataColumn>,
    semantic: CanonicalStataMetadata,
}

#[derive(Clone, Debug)]
pub enum StataMetadataFooter {
    Absent,
    Present(PreparedStataMetadata),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalStataMetadata {
    columns: Vec<StataMetadataColumn>,
    variables: Vec<CanonicalStataVariable>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalStataVariable {
    name: String,
    label: Option<String>,
    value_label_name: Option<String>,
    value_labels: BTreeMap<String, String>,
}

pub fn build_key_value_metadata(
    capsule_path: &str,
    capsule_variables: &str,
    saved_variables: &HashSet<String>,
    physical_names: &HashMap<String, String>,
) -> Result<Option<KeyValueMetadata>, String> {
    Ok(prepare_stata_metadata(
        capsule_path,
        capsule_variables,
        saved_variables,
        physical_names,
    )?
    .map(|prepared| prepared.key_value_metadata))
}

pub fn prepare_stata_metadata(
    capsule_path: &str,
    capsule_variables: &str,
    saved_variables: &HashSet<String>,
    physical_names: &HashMap<String, String>,
) -> Result<Option<PreparedStataMetadata>, String> {
    let has_path = !capsule_path.trim().is_empty();
    let capsule_names: Vec<String> = capsule_variables
        .split_whitespace()
        .map(str::to_owned)
        .collect();

    if !has_path && capsule_names.is_empty() {
        return Ok(None);
    }
    if !has_path || capsule_names.is_empty() {
        return Err(
            "Stata metadata requires both a capsule file and at least one capsule variable"
                .to_string(),
        );
    }

    let mut seen_capsule_names = HashSet::with_capacity(capsule_names.len());
    let mut seen_parquet_names = HashSet::with_capacity(capsule_names.len());
    let mut columns = Vec::with_capacity(capsule_names.len());
    for capsule_name in capsule_names {
        if !seen_capsule_names.insert(capsule_name.clone()) {
            return Err(format!(
                "Duplicate Stata metadata capsule variable: {capsule_name}"
            ));
        }
        if !saved_variables.contains(&capsule_name) {
            return Err(format!(
                "Stata metadata capsule variable is not in the saved varlist: {capsule_name}"
            ));
        }

        let parquet_name = physical_names
            .get(&capsule_name)
            .cloned()
            .unwrap_or_else(|| capsule_name.clone());
        if parquet_name.is_empty() {
            return Err(format!(
                "Empty Parquet name for Stata metadata variable: {capsule_name}"
            ));
        }
        if !seen_parquet_names.insert(parquet_name.clone()) {
            return Err(format!(
                "Duplicate Parquet name in Stata metadata column map: {parquet_name}"
            ));
        }
        columns.push(StataMetadataColumn {
            parquet_name,
            capsule_name,
        });
    }
    validate_column_map(&columns)?;
    columns.sort_by(|left, right| {
        (&left.parquet_name, &left.capsule_name).cmp(&(&right.parquet_name, &right.capsule_name))
    });

    let capsule = fs::read(capsule_path)
        .map_err(|e| format!("Could not read Stata metadata capsule {capsule_path}: {e}"))?;
    validate_decompressed_capsule_size(capsule.len())?;
    validate_dta_header(&capsule)?;
    let semantic = canonical_metadata_from_bytes(&capsule, &columns)?;

    let compressed = zstd::stream::encode_all(capsule.as_slice(), 3)
        .map_err(format_capsule_compression_error)?;
    validate_compressed_capsule_size(compressed.len())?;

    let envelope = StataMetadataEnvelope {
        format_version: STATA_METADATA_VERSION,
        encoding: STATA_METADATA_ENCODING.to_string(),
        capsule: BASE64_STANDARD.encode(compressed),
        columns: columns.clone(),
    };
    let envelope_json =
        serde_json::to_string(&envelope).map_err(format_envelope_serialization_error)?;
    validate_envelope_size(envelope_json.len())?;

    let key_value_metadata = metadata_key_value(&envelope_json);
    Ok(Some(PreparedStataMetadata {
        envelope_json,
        key_value_metadata,
        decoded_capsule: capsule,
        columns,
        semantic,
    }))
}

pub fn extract_capsule_from_parquet(
    parquet_path: &str,
    output_path: &str,
) -> Result<Option<ExtractedStataMetadata>, String> {
    match read_fragment_stata_metadata(Path::new(parquet_path))? {
        StataMetadataFooter::Absent => Ok(None),
        StataMetadataFooter::Present(prepared) => {
            write_capsule_safely(Path::new(output_path), &prepared.decoded_capsule)?;
            Ok(Some(ExtractedStataMetadata {
                format_version: STATA_METADATA_VERSION,
                columns: prepared.columns,
            }))
        }
    }
}

pub fn extract_capsule_from_parquet_input(
    parquet_input: &str,
    output_path: &str,
) -> Result<Option<ExtractedStataMetadata>, String> {
    let fragments = resolve_parquet_fragments(parquet_input)?;
    let prepared = match validate_stata_metadata_fragments(&fragments)? {
        Some(prepared) => prepared,
        None => return Ok(None),
    };
    write_capsule_safely(Path::new(output_path), &prepared.decoded_capsule)?;
    Ok(Some(ExtractedStataMetadata {
        format_version: STATA_METADATA_VERSION,
        columns: prepared.columns,
    }))
}

pub fn validate_parquet_input_metadata(parquet_input: &str) -> Result<(), String> {
    let fragments = resolve_parquet_fragments(parquet_input)?;
    validate_stata_metadata_fragments(&fragments)?;
    Ok(())
}

pub fn resolve_parquet_fragments(input: &str) -> Result<Vec<PathBuf>, String> {
    let normalized = input.replace('\\', "/");
    let input_path = Path::new(&normalized);
    let mut fragments = Vec::new();

    if input_path.is_file() {
        fragments.push(input_path.to_path_buf());
    } else if input_path.is_dir() {
        collect_parquet_fragments(input_path, &mut fragments)?;
    } else {
        let pattern = normalized.replace("**.", "**/*.");
        let entries = glob::glob(&pattern)
            .map_err(|e| format!("Invalid Parquet glob pattern {input:?}: {e}"))?;
        for entry in entries {
            let path =
                entry.map_err(|e| format!("Could not resolve Parquet glob {input:?}: {e}"))?;
            if path.is_file() && is_parquet_file(&path) {
                fragments.push(path);
            }
        }
    }

    fragments.sort();
    fragments.dedup();
    if fragments.is_empty() {
        return Err(format!("No Parquet fragments resolved from: {input}"));
    }
    Ok(fragments)
}

pub fn read_fragment_stata_metadata(path: &Path) -> Result<StataMetadataFooter, String> {
    let mut parquet_file = File::open(path)
        .map_err(|e| format!("Could not open Parquet file {}: {e}", path.display()))?;
    validate_footer_size(&mut parquet_file)?;

    let metadata = polars_parquet::read::read_metadata(&mut parquet_file)
        .map_err(|e| format!("Could not read Parquet footer metadata: {e}"))?;
    let matching_values: Vec<&str> = metadata
        .key_value_metadata()
        .as_ref()
        .into_iter()
        .flatten()
        .filter(|item| item.key == STATA_METADATA_KEY)
        .map(|item| {
            item.value
                .as_deref()
                .ok_or_else(|| "Stata metadata key has no value".to_string())
        })
        .collect::<Result<Vec<_>, _>>()?;

    match matching_values.as_slice() {
        [] => Ok(StataMetadataFooter::Absent),
        [value] => decode_prepared_metadata(value).map(StataMetadataFooter::Present),
        _ => Err("Parquet footer contains duplicate Stata metadata keys".to_string()),
    }
}

pub fn validate_stata_metadata_fragments(
    fragments: &[PathBuf],
) -> Result<Option<PreparedStataMetadata>, String> {
    let mut ordered_fragments = fragments.to_vec();
    ordered_fragments.sort();
    ordered_fragments.dedup();
    let (first_path, remaining) = ordered_fragments
        .split_first()
        .ok_or_else(|| "No Parquet fragments were provided for metadata validation".to_string())?;
    let first = read_fragment_stata_metadata(first_path)
        .map_err(|e| format!("Invalid Stata metadata in {}: {e}", first_path.display()))?;

    match first {
        StataMetadataFooter::Absent => {
            for path in remaining {
                match read_fragment_stata_metadata(path)
                    .map_err(|e| format!("Invalid Stata metadata in {}: {e}", path.display()))?
                {
                    StataMetadataFooter::Absent => {}
                    StataMetadataFooter::Present(_) => {
                        return Err(format!(
                            "Parquet fragments have mixed Stata metadata: {} has metadata but {} is missing Stata metadata",
                            path.display(),
                            first_path.display()
                        ));
                    }
                }
            }
            Ok(None)
        }
        StataMetadataFooter::Present(reference) => {
            for path in remaining {
                match read_fragment_stata_metadata(path)
                    .map_err(|e| format!("Invalid Stata metadata in {}: {e}", path.display()))?
                {
                    StataMetadataFooter::Absent => {
                        return Err(format!(
                            "Parquet fragment {} is missing Stata metadata present in {}",
                            path.display(),
                            first_path.display()
                        ));
                    }
                    StataMetadataFooter::Present(candidate) => {
                        if candidate.envelope_json != reference.envelope_json
                            && candidate.semantic != reference.semantic
                        {
                            return Err(format!(
                                "Parquet fragment {} has conflicting Stata metadata relative to {}",
                                path.display(),
                                first_path.display()
                            ));
                        }
                    }
                }
            }
            Ok(Some(reference))
        }
    }
}

pub fn validate_stata_metadata_compatibility(
    existing: Option<&PreparedStataMetadata>,
    proposed: Option<&PreparedStataMetadata>,
) -> Result<(), String> {
    match (existing, proposed) {
        (None, None) => Ok(()),
        (None, Some(_)) => Err("Existing Parquet fragments are missing Stata metadata".to_string()),
        (Some(_), None) => Err("Proposed Parquet fragments are missing Stata metadata".to_string()),
        (Some(existing), Some(proposed)) => {
            if existing.envelope_json == proposed.envelope_json
                || existing.semantic == proposed.semantic
            {
                Ok(())
            } else {
                Err("Existing Parquet fragments have conflicting Stata metadata".to_string())
            }
        }
    }
}

pub fn reconcile_stata_metadata_for_append(
    existing: Option<PreparedStataMetadata>,
    proposed: Option<&PreparedStataMetadata>,
) -> Result<Option<PreparedStataMetadata>, String> {
    validate_stata_metadata_compatibility(existing.as_ref(), proposed)?;
    Ok(existing)
}

fn metadata_key_value(envelope_json: &str) -> KeyValueMetadata {
    KeyValueMetadata::from_static(vec![(
        STATA_METADATA_KEY.to_string(),
        envelope_json.to_string(),
    )])
}

fn is_parquet_file(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
}

fn collect_parquet_fragments(directory: &Path, fragments: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(directory).map_err(|e| {
        format!(
            "Could not read Parquet dataset directory {}: {e}",
            directory.display()
        )
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| {
            format!(
                "Could not read an entry in Parquet dataset directory {}: {e}",
                directory.display()
            )
        })?;
        let file_type = entry.file_type().map_err(|e| {
            format!(
                "Could not inspect Parquet dataset entry {}: {e}",
                entry.path().display()
            )
        })?;
        if file_type.is_dir() {
            collect_parquet_fragments(&entry.path(), fragments)?;
        } else if file_type.is_file() && is_parquet_file(&entry.path()) {
            fragments.push(entry.path());
        }
    }
    Ok(())
}

fn decode_prepared_metadata(value: &str) -> Result<PreparedStataMetadata, String> {
    let (decoded_capsule, extracted) = decode_envelope(value)?;
    let semantic = canonical_metadata_from_bytes(&decoded_capsule, &extracted.columns)?;
    Ok(PreparedStataMetadata {
        envelope_json: value.to_string(),
        key_value_metadata: metadata_key_value(value),
        decoded_capsule,
        columns: extracted.columns,
        semantic,
    })
}

fn canonical_metadata_from_bytes(
    capsule: &[u8],
    columns: &[StataMetadataColumn],
) -> Result<CanonicalStataMetadata, String> {
    let sequence = METADATA_TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut attempts = 0u32;
    loop {
        let path = std::env::temp_dir().join(format!(
            "stata_parquet_io_capsule_{}_{}_{}.dta",
            std::process::id(),
            sequence,
            attempts
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                let write_result = file
                    .write_all(capsule)
                    .and_then(|_| file.flush())
                    .map_err(|e| format!("Could not stage Stata metadata capsule: {e}"));
                drop(file);
                let result =
                    write_result.and_then(|_| canonical_metadata_from_path(&path, columns));
                let _ = fs::remove_file(&path);
                return result;
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && attempts < 100 => {
                attempts += 1;
            }
            Err(error) => {
                return Err(format!(
                    "Could not create a temporary Stata metadata capsule: {error}"
                ));
            }
        }
    }
}

fn canonical_metadata_from_path(
    capsule_path: &Path,
    columns: &[StataMetadataColumn],
) -> Result<CanonicalStataMetadata, String> {
    let metadata_json = readstat_metadata_json(capsule_path, Some(ReadStatFormat::Stata))
        .map_err(|e| format!("Could not parse Stata metadata capsule: {e}"))?;
    canonical_metadata_from_json(&metadata_json, columns)
}

fn canonical_metadata_from_json(
    metadata_json: &str,
    columns: &[StataMetadataColumn],
) -> Result<CanonicalStataMetadata, String> {
    let root: Value = serde_json::from_str(metadata_json)
        .map_err(|e| format!("Invalid Stata metadata capsule JSON: {e}"))?;
    if root.get("row_count").and_then(Value::as_u64) != Some(0) {
        return Err("Stata metadata capsule must contain zero observations".to_string());
    }

    let variable_values = root
        .get("variables")
        .and_then(Value::as_array)
        .ok_or_else(|| "Stata metadata capsule has no variables array".to_string())?;
    let mut variables = Vec::with_capacity(variable_values.len());
    let mut variable_names = HashSet::with_capacity(variable_values.len());
    for variable in variable_values {
        let object = variable
            .as_object()
            .ok_or_else(|| "Stata metadata capsule contains an invalid variable".to_string())?;
        let name = required_string(object.get("name"), "variable name")?;
        validate_stata_name(&name)?;
        if !variable_names.insert(name.clone()) {
            return Err(format!(
                "Duplicate variable in Stata metadata capsule: {name}"
            ));
        }
        let label = optional_string(object.get("label"), "variable label")?;
        let value_label_name = optional_string(
            object.get("value_label_name"),
            "value-label association name",
        )?;
        let value_labels = parse_value_labels(object.get("value_labels"))?;
        variables.push(CanonicalStataVariable {
            name,
            label,
            value_label_name,
            value_labels,
        });
    }
    variables.sort_by(|left, right| left.name.cmp(&right.name));

    let mut canonical_columns = columns.to_vec();
    canonical_columns.sort_by(|left, right| {
        (&left.parquet_name, &left.capsule_name).cmp(&(&right.parquet_name, &right.capsule_name))
    });
    let expected_names: HashSet<&str> = canonical_columns
        .iter()
        .map(|column| column.capsule_name.as_str())
        .collect();
    let actual_names: HashSet<&str> = variables
        .iter()
        .map(|variable| variable.name.as_str())
        .collect();
    if expected_names != actual_names {
        let mut missing: Vec<&str> = expected_names.difference(&actual_names).copied().collect();
        let mut unexpected: Vec<&str> = actual_names.difference(&expected_names).copied().collect();
        missing.sort_unstable();
        unexpected.sort_unstable();
        return Err(format!(
            "Stata metadata capsule variables do not match the column map (missing: {}; unexpected: {})",
            missing.join(", "),
            unexpected.join(", ")
        ));
    }

    Ok(CanonicalStataMetadata {
        columns: canonical_columns,
        variables,
    })
}

fn required_string(value: Option<&Value>, field: &str) -> Result<String, String> {
    value
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("Stata metadata capsule has an invalid {field}"))
}

fn optional_string(value: Option<&Value>, field: &str) -> Result<Option<String>, String> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(format!("Stata metadata capsule has an invalid {field}")),
    }
}

fn parse_value_labels(value: Option<&Value>) -> Result<BTreeMap<String, String>, String> {
    match value {
        None | Some(Value::Null) => Ok(BTreeMap::new()),
        Some(Value::Object(mapping)) => mapping
            .iter()
            .map(|(code, label)| {
                label
                    .as_str()
                    .map(|label| (code.clone(), label.to_string()))
                    .ok_or_else(|| {
                        "Stata metadata capsule contains an invalid value-label mapping".to_string()
                    })
            })
            .collect(),
        Some(_) => Err("Stata metadata capsule has invalid value labels".to_string()),
    }
}

fn decode_envelope(value: &str) -> Result<(Vec<u8>, ExtractedStataMetadata), String> {
    validate_envelope_size(value.len())?;
    let envelope: StataMetadataEnvelope = serde_json::from_str(value)
        .map_err(|e| format!("Invalid Stata metadata JSON envelope: {e}"))?;
    if envelope.format_version != STATA_METADATA_VERSION {
        return Err(format!(
            "Unsupported Stata metadata format version: {}",
            envelope.format_version
        ));
    }
    if envelope.encoding != STATA_METADATA_ENCODING {
        return Err(format!(
            "Unsupported Stata metadata encoding: {}",
            envelope.encoding
        ));
    }
    validate_column_map(&envelope.columns)?;

    let compressed = BASE64_STANDARD
        .decode(envelope.capsule.as_bytes())
        .map_err(|e| format!("Invalid base64 Stata metadata capsule: {e}"))?;
    validate_compressed_capsule_size(compressed.len())?;

    let decoder = zstd::stream::read::Decoder::new(compressed.as_slice())
        .map_err(format_zstd_initialization_error)?;
    let mut capsule = Vec::new();
    decoder
        .take((MAX_DECOMPRESSED_CAPSULE_BYTES + 1) as u64)
        .read_to_end(&mut capsule)
        .map_err(|e| format!("Could not decompress Stata metadata capsule: {e}"))?;
    validate_decompressed_capsule_size(capsule.len())?;
    validate_dta_header(&capsule)?;

    Ok((
        capsule,
        ExtractedStataMetadata {
            format_version: envelope.format_version,
            columns: envelope.columns,
        },
    ))
}

fn validate_column_map(columns: &[StataMetadataColumn]) -> Result<(), String> {
    if columns.is_empty() {
        return Err("Stata metadata column map is empty".to_string());
    }
    let mut parquet_names = HashSet::with_capacity(columns.len());
    let mut capsule_names = HashSet::with_capacity(columns.len());
    for column in columns {
        if column.parquet_name.is_empty() || column.capsule_name.is_empty() {
            return Err("Stata metadata column names cannot be empty".to_string());
        }
        validate_stata_name(&column.capsule_name)?;
        if !parquet_names.insert(column.parquet_name.as_str()) {
            return Err(format!(
                "Duplicate Parquet name in Stata metadata column map: {}",
                column.parquet_name
            ));
        }
        if !capsule_names.insert(column.capsule_name.as_str()) {
            return Err(format!(
                "Duplicate capsule name in Stata metadata column map: {}",
                column.capsule_name
            ));
        }
    }
    Ok(())
}

pub fn validate_stata_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.len() > 32 {
        return Err(format!(
            "Invalid Stata metadata capsule variable name: {name:?}"
        ));
    }

    let mut characters = name.chars();
    let first = characters
        .next()
        .expect("non-empty Stata metadata name checked above");
    if first != '_' && !first.is_alphabetic() {
        return Err(format!(
            "Invalid Stata metadata capsule variable name: {name:?}"
        ));
    }
    if characters.any(|character| character != '_' && !character.is_alphanumeric()) {
        return Err(format!(
            "Invalid Stata metadata capsule variable name: {name:?}"
        ));
    }
    Ok(())
}

fn validate_dta_header(capsule: &[u8]) -> Result<(), String> {
    if !capsule.starts_with(b"<stata_dta>") {
        return Err("Stata metadata capsule is not a supported .dta file".to_string());
    }
    Ok(())
}

fn validate_footer_size(file: &mut File) -> Result<(), String> {
    let file_size = file
        .seek(SeekFrom::End(0))
        .map_err(format_footer_size_error)?;
    if file_size < 12 {
        return Err("Parquet file is too small to contain a valid footer".to_string());
    }
    file.seek(SeekFrom::End(-8))
        .map_err(format_footer_seek_error)?;
    let mut trailer = [0u8; 8];
    file.read_exact(&mut trailer)
        .map_err(format_footer_read_error)?;
    if &trailer[4..] != b"PAR1" {
        return Err("Parquet file does not end with the PAR1 marker".to_string());
    }
    let footer_metadata_bytes = u32::from_le_bytes(trailer[..4].try_into().unwrap()) as u64;
    if footer_metadata_bytes > MAX_PARQUET_FOOTER_BYTES {
        return Err(format!(
            "Parquet footer exceeds the {} MiB metadata limit",
            MAX_PARQUET_FOOTER_BYTES / (1024 * 1024)
        ));
    }
    if footer_metadata_bytes + 8 > file_size {
        return Err("Parquet footer length exceeds the file size".to_string());
    }
    Ok(())
}

fn write_capsule_safely(path: &Path, capsule: &[u8]) -> Result<(), String> {
    let result = (|| -> Result<(), std::io::Error> {
        let mut file = File::create(path)?;
        file.write_all(capsule)?;
        file.flush()?;
        Ok(())
    })();
    if let Err(e) = result {
        let _ = fs::remove_file(path);
        return Err(format!(
            "Could not write extracted Stata metadata capsule {}: {e}",
            path.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    use polars::df;
    use polars::prelude::ParquetWriteOptions;
    use polars_parquet::write::KeyValue;
    use polars_readstat_rs::{StataWriter, ValueLabelMap, ValueLabels, VariableLabels};

    use super::*;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let sequence = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "stata_parquet_io_metadata_{}_{}_{}",
            std::process::id(),
            sequence,
            suffix
        ))
    }

    fn sample_capsule() -> Vec<u8> {
        let mut bytes = b"<stata_dta>".to_vec();
        bytes.extend_from_slice("Unicode Å π 👍".as_bytes());
        bytes
    }

    fn encoded_capsule(capsule: &[u8]) -> String {
        let compressed = zstd::stream::encode_all(capsule, 3).unwrap();
        BASE64_STANDARD.encode(compressed)
    }

    fn envelope_json(
        format_version: u32,
        encoding: &str,
        capsule: String,
        columns: Vec<StataMetadataColumn>,
    ) -> String {
        serde_json::to_string(&StataMetadataEnvelope {
            format_version,
            encoding: encoding.to_string(),
            capsule,
            columns,
        })
        .unwrap()
    }

    fn write_parquet_with_metadata(path: &Path, metadata: KeyValueMetadata) {
        let mut data = df!("x" => [1i32, 2i32]).unwrap();
        let mut options = ParquetWriteOptions::default();
        options.key_value_metadata = Some(metadata);
        options
            .to_writer(File::create(path).unwrap())
            .finish(&mut data)
            .unwrap();
    }

    fn write_zero_row_capsule(path: &Path, variable_label: &str, unused_label: &str) {
        let data = df!("status" => Vec::<i32>::new()).unwrap();
        let mut mapping: ValueLabelMap = BTreeMap::new();
        mapping.insert(-1, "negative".to_string());
        mapping.insert(1, "observed".to_string());
        mapping.insert(3, unused_label.to_string());
        let labels = ValueLabels::from([("status".to_string(), mapping)]);
        StataWriter::new(path)
            .with_value_labels(labels)
            .with_variable_labels(VariableLabels::from([(
                "status".to_string(),
                variable_label.to_string(),
            )]))
            .write_df(&data)
            .unwrap();
    }

    fn metadata_for_capsule(path: &Path) -> KeyValueMetadata {
        prepare_stata_metadata(
            path.to_str().unwrap(),
            "status",
            &HashSet::from(["status".to_string()]),
            &HashMap::new(),
        )
        .unwrap()
        .unwrap()
        .key_value_metadata
    }

    fn mutate_timestamp_without_changing_semantics(path: &Path) {
        let mut capsule = fs::read(path).unwrap();
        let timestamp_tag = b"<timestamp>";
        let length_offset = capsule
            .windows(timestamp_tag.len())
            .position(|window| window == timestamp_tag)
            .unwrap()
            + timestamp_tag.len();
        assert_eq!(capsule[length_offset], 0);
        capsule[length_offset] = 1;
        capsule.insert(length_offset + 1, b'x');

        let map_tag = b"<map>";
        let map_values_offset = capsule
            .windows(map_tag.len())
            .position(|window| window == map_tag)
            .unwrap()
            + map_tag.len();
        for index in 0..14 {
            let start = map_values_offset + index * 8;
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&capsule[start..start + 8]);
            let offset = u64::from_le_bytes(bytes);
            if offset != 0 {
                capsule[start..start + 8].copy_from_slice(&(offset + 1).to_le_bytes());
            }
        }
        fs::write(path, capsule).unwrap();
    }

    #[test]
    fn metadata_round_trip_preserves_arrow_schema_and_capsule() {
        let capsule_path = temp_path("capsule.dta");
        let parquet_path = temp_path("data.parquet");
        let extracted_path = temp_path("extracted.dta");
        write_zero_row_capsule(&capsule_path, "Status", "unused");
        let capsule = fs::read(&capsule_path).unwrap();

        let saved_variables = HashSet::from(["status".to_string()]);
        let physical_names = HashMap::from([(
            "status".to_string(),
            "status_with_a_long_physical_name".to_string(),
        )]);
        let key_value_metadata = build_key_value_metadata(
            capsule_path.to_str().unwrap(),
            "status",
            &saved_variables,
            &physical_names,
        )
        .unwrap();

        let mut data = df!("status_with_a_long_physical_name" => [1i32, 2i32]).unwrap();
        let mut options = ParquetWriteOptions::default();
        options.key_value_metadata = key_value_metadata;
        options
            .to_writer(File::create(&parquet_path).unwrap())
            .finish(&mut data)
            .unwrap();

        let mut file = File::open(&parquet_path).unwrap();
        let footer = polars_parquet::read::read_metadata(&mut file).unwrap();
        let keys: HashSet<&str> = footer
            .key_value_metadata()
            .as_ref()
            .unwrap()
            .iter()
            .map(|item| item.key.as_str())
            .collect();
        assert!(keys.contains("ARROW:schema"));
        assert!(keys.contains(STATA_METADATA_KEY));

        let extracted = extract_capsule_from_parquet(
            parquet_path.to_str().unwrap(),
            extracted_path.to_str().unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(extracted.format_version, STATA_METADATA_VERSION);
        assert_eq!(
            extracted.columns,
            vec![StataMetadataColumn {
                parquet_name: "status_with_a_long_physical_name".to_string(),
                capsule_name: "status".to_string(),
            }]
        );
        assert_eq!(fs::read(&extracted_path).unwrap(), capsule);

        let _ = fs::remove_file(capsule_path);
        let _ = fs::remove_file(parquet_path);
        let _ = fs::remove_file(extracted_path);
    }

    #[test]
    fn file_without_metadata_is_compatible() {
        let parquet_path = temp_path("plain.parquet");
        let extracted_path = temp_path("plain-extracted.dta");
        let mut data = df!("x" => [1i32, 2i32]).unwrap();
        ParquetWriteOptions::default()
            .to_writer(File::create(&parquet_path).unwrap())
            .finish(&mut data)
            .unwrap();

        let extracted = extract_capsule_from_parquet(
            parquet_path.to_str().unwrap(),
            extracted_path.to_str().unwrap(),
        )
        .unwrap();
        assert!(extracted.is_none());
        assert!(!extracted_path.exists());

        let _ = fs::remove_file(parquet_path);
    }

    #[test]
    fn invalid_base64_is_rejected() {
        let envelope = r#"{"format_version":1,"encoding":"zstd+base64","capsule":"%%%","columns":[{"parquet_name":"x","capsule_name":"x"}]}"#;
        let error = decode_envelope(envelope).unwrap_err();
        assert!(error.contains("Invalid base64"));
    }

    #[test]
    fn unsafe_capsule_name_is_rejected_before_stata_receives_it() {
        let envelope = r#"{"format_version":1,"encoding":"zstd+base64","capsule":"","columns":[{"parquet_name":"x","capsule_name":"x\"); do evil.do"}]}"#;
        let error = decode_envelope(envelope).unwrap_err();
        assert!(error.contains("Invalid Stata metadata capsule variable name"));
    }

    #[test]
    fn build_metadata_validates_arguments_and_column_map() {
        let capsule_path = temp_path("build-capsule.dta");
        fs::write(&capsule_path, sample_capsule()).unwrap();
        let capsule_path = capsule_path.to_str().unwrap();
        let saved = HashSet::from(["x".to_string(), "y".to_string()]);

        assert!(build_key_value_metadata("", "", &saved, &HashMap::new())
            .unwrap()
            .is_none());
        assert!(
            build_key_value_metadata(capsule_path, "", &saved, &HashMap::new())
                .unwrap_err()
                .contains("requires both")
        );
        assert!(build_key_value_metadata("", "x", &saved, &HashMap::new())
            .unwrap_err()
            .contains("requires both"));
        assert!(
            build_key_value_metadata(capsule_path, "x x", &saved, &HashMap::new())
                .unwrap_err()
                .contains("Duplicate Stata metadata capsule variable")
        );
        assert!(
            build_key_value_metadata(capsule_path, "missing", &saved, &HashMap::new())
                .unwrap_err()
                .contains("not in the saved varlist")
        );

        let empty_physical = HashMap::from([("x".to_string(), String::new())]);
        assert!(
            build_key_value_metadata(capsule_path, "x", &saved, &empty_physical)
                .unwrap_err()
                .contains("Empty Parquet name")
        );

        let duplicate_physical = HashMap::from([
            ("x".to_string(), "same".to_string()),
            ("y".to_string(), "same".to_string()),
        ]);
        assert!(
            build_key_value_metadata(capsule_path, "x y", &saved, &duplicate_physical)
                .unwrap_err()
                .contains("Duplicate Parquet name")
        );

        let missing_path = temp_path("missing-capsule.dta");
        assert!(build_key_value_metadata(
            missing_path.to_str().unwrap(),
            "x",
            &saved,
            &HashMap::new(),
        )
        .unwrap_err()
        .contains("Could not read Stata metadata capsule"));

        let invalid_path = temp_path("invalid-capsule.dta");
        fs::write(&invalid_path, b"not a dta").unwrap();
        assert!(build_key_value_metadata(
            invalid_path.to_str().unwrap(),
            "x",
            &saved,
            &HashMap::new(),
        )
        .unwrap_err()
        .contains("not a supported .dta"));

        let _ = fs::remove_file(capsule_path);
        let _ = fs::remove_file(invalid_path);
    }

    #[test]
    fn envelope_validation_rejects_every_unsupported_shape() {
        let column = StataMetadataColumn {
            parquet_name: "x".to_string(),
            capsule_name: "x".to_string(),
        };
        let valid_capsule = encoded_capsule(&sample_capsule());

        assert!(decode_envelope("not json")
            .unwrap_err()
            .contains("Invalid Stata metadata JSON"));
        assert!(decode_envelope(
            r#"{"format_version":1,"encoding":"zstd+base64","capsule":"","columns":[],"extra":1}"#,
        )
        .unwrap_err()
        .contains("unknown field"));
        assert!(decode_envelope(&envelope_json(
            2,
            STATA_METADATA_ENCODING,
            valid_capsule.clone(),
            vec![column.clone()],
        ))
        .unwrap_err()
        .contains("Unsupported Stata metadata format version"));
        assert!(decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            "plain",
            valid_capsule.clone(),
            vec![column.clone()],
        ))
        .unwrap_err()
        .contains("Unsupported Stata metadata encoding"));
        assert!(decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            valid_capsule.clone(),
            vec![],
        ))
        .unwrap_err()
        .contains("column map is empty"));

        let empty_name = StataMetadataColumn {
            parquet_name: String::new(),
            capsule_name: "x".to_string(),
        };
        assert!(decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            valid_capsule.clone(),
            vec![empty_name],
        ))
        .unwrap_err()
        .contains("column names cannot be empty"));

        let duplicate_parquet = vec![
            column.clone(),
            StataMetadataColumn {
                parquet_name: "x".to_string(),
                capsule_name: "y".to_string(),
            },
        ];
        assert!(decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            valid_capsule.clone(),
            duplicate_parquet,
        ))
        .unwrap_err()
        .contains("Duplicate Parquet name"));

        let duplicate_capsule = vec![
            column.clone(),
            StataMetadataColumn {
                parquet_name: "y".to_string(),
                capsule_name: "x".to_string(),
            },
        ];
        assert!(decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            valid_capsule,
            duplicate_capsule,
        ))
        .unwrap_err()
        .contains("Duplicate capsule name"));

        let invalid_zstd = BASE64_STANDARD.encode(b"not zstd");
        let invalid_zstd_error = decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            invalid_zstd,
            vec![column.clone()],
        ))
        .unwrap_err();
        assert!(
            invalid_zstd_error.contains("decompress Stata metadata capsule"),
            "unexpected error: {invalid_zstd_error}"
        );

        let invalid_dta = encoded_capsule(b"not a dta");
        assert!(decode_envelope(&envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            invalid_dta,
            vec![column],
        ))
        .unwrap_err()
        .contains("not a supported .dta"));
    }

    #[test]
    fn stata_name_validation_covers_boundaries() {
        assert!(validate_stata_name("_valid_name_9").is_ok());
        assert!(validate_stata_name("Ångström9").is_ok());
        assert!(validate_stata_name("").is_err());
        assert!(validate_stata_name(&"x".repeat(33)).is_err());
        assert!(validate_stata_name("9starts_with_digit").is_err());
        assert!(validate_stata_name("contains-dash").is_err());
    }

    #[test]
    fn internal_error_formatters_preserve_context() {
        let io_error = || std::io::Error::other("injected failure");
        assert!(format_capsule_compression_error(io_error()).contains("compress"));
        assert!(format_zstd_initialization_error(io_error()).contains("Invalid zstd"));
        assert!(format_footer_size_error(io_error()).contains("file size"));
        assert!(format_footer_seek_error(io_error()).contains("seek to Parquet footer"));
        assert!(format_footer_read_error(io_error()).contains("footer trailer"));

        let json_error = serde_json::from_str::<StataMetadataEnvelope>("not json").unwrap_err();
        assert!(format_envelope_serialization_error(json_error).contains("serialize"));
    }

    #[test]
    fn envelope_and_capsule_size_limits_are_enforced() {
        assert!(validate_envelope_size(MAX_ENVELOPE_BYTES).is_ok());
        assert!(validate_envelope_size(MAX_ENVELOPE_BYTES + 1).is_err());
        assert!(validate_compressed_capsule_size(MAX_COMPRESSED_CAPSULE_BYTES).is_ok());
        assert!(validate_compressed_capsule_size(MAX_COMPRESSED_CAPSULE_BYTES + 1).is_err());
        assert!(validate_decompressed_capsule_size(MAX_DECOMPRESSED_CAPSULE_BYTES).is_ok());
        assert!(validate_decompressed_capsule_size(MAX_DECOMPRESSED_CAPSULE_BYTES + 1).is_err());

        let oversized_envelope = "x".repeat(MAX_ENVELOPE_BYTES + 1);
        assert!(decode_envelope(&oversized_envelope)
            .unwrap_err()
            .contains("envelope exceeds"));

        let oversized_compressed = vec![0u8; MAX_COMPRESSED_CAPSULE_BYTES + 1];
        let column = StataMetadataColumn {
            parquet_name: "x".to_string(),
            capsule_name: "x".to_string(),
        };
        let envelope = envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            BASE64_STANDARD.encode(oversized_compressed),
            vec![column.clone()],
        );
        assert!(decode_envelope(&envelope)
            .unwrap_err()
            .contains("Compressed Stata metadata capsule exceeds"));

        let mut oversized_capsule = vec![0u8; MAX_DECOMPRESSED_CAPSULE_BYTES + 1];
        oversized_capsule[..11].copy_from_slice(b"<stata_dta>");
        let envelope = envelope_json(
            STATA_METADATA_VERSION,
            STATA_METADATA_ENCODING,
            encoded_capsule(&oversized_capsule),
            vec![column],
        );
        assert!(decode_envelope(&envelope)
            .unwrap_err()
            .contains("decompressed-size limit"));

        let oversized_path = temp_path("oversized-capsule.dta");
        let mut oversized_file = File::create(&oversized_path).unwrap();
        oversized_file.write_all(b"<stata_dta>").unwrap();
        oversized_file
            .set_len((MAX_DECOMPRESSED_CAPSULE_BYTES + 1) as u64)
            .unwrap();
        oversized_file.flush().unwrap();
        drop(oversized_file);
        let saved = HashSet::from(["x".to_string()]);
        assert!(build_key_value_metadata(
            oversized_path.to_str().unwrap(),
            "x",
            &saved,
            &HashMap::new(),
        )
        .unwrap_err()
        .contains("decompressed-size limit"));
        let _ = fs::remove_file(oversized_path);
    }

    #[test]
    fn invalid_parquet_footers_are_rejected() {
        let missing_path = temp_path("missing.parquet");
        assert!(extract_capsule_from_parquet(
            missing_path.to_str().unwrap(),
            temp_path("unused.dta").to_str().unwrap(),
        )
        .unwrap_err()
        .contains("Could not open Parquet file"));

        let cases = [
            (b"small".to_vec(), "too small"),
            (
                [b"xxxx".as_slice(), &[0, 0, 0, 0], b"BAD!"].concat(),
                "PAR1 marker",
            ),
            (
                [
                    b"xxxx".as_slice(),
                    &((MAX_PARQUET_FOOTER_BYTES + 1) as u32).to_le_bytes(),
                    b"PAR1",
                ]
                .concat(),
                "footer exceeds",
            ),
            (
                [b"xxxx".as_slice(), &100u32.to_le_bytes(), b"PAR1"].concat(),
                "length exceeds",
            ),
        ];
        for (bytes, expected) in cases {
            let parquet_path = temp_path("invalid-footer.parquet");
            fs::write(&parquet_path, bytes).unwrap();
            let error = extract_capsule_from_parquet(
                parquet_path.to_str().unwrap(),
                temp_path("unused.dta").to_str().unwrap(),
            )
            .unwrap_err();
            assert!(error.contains(expected), "unexpected error: {error}");
            let _ = fs::remove_file(parquet_path);
        }

        let invalid_metadata = temp_path("invalid-metadata.parquet");
        fs::write(
            &invalid_metadata,
            [b"xxxx".as_slice(), &0u32.to_le_bytes(), b"PAR1"].concat(),
        )
        .unwrap();
        assert!(extract_capsule_from_parquet(
            invalid_metadata.to_str().unwrap(),
            temp_path("unused.dta").to_str().unwrap(),
        )
        .unwrap_err()
        .contains("Could not read Parquet footer metadata"));
        let _ = fs::remove_file(invalid_metadata);
    }

    #[test]
    fn duplicate_and_valueless_footer_keys_are_rejected() {
        let duplicate_path = temp_path("duplicate-key.parquet");
        let duplicate_metadata = KeyValueMetadata::Static(vec![
            KeyValue {
                key: STATA_METADATA_KEY.to_string(),
                value: Some("one".to_string()),
            },
            KeyValue {
                key: STATA_METADATA_KEY.to_string(),
                value: Some("two".to_string()),
            },
        ]);
        write_parquet_with_metadata(&duplicate_path, duplicate_metadata);
        assert!(extract_capsule_from_parquet(
            duplicate_path.to_str().unwrap(),
            temp_path("unused.dta").to_str().unwrap(),
        )
        .unwrap_err()
        .contains("duplicate Stata metadata keys"));

        let valueless_path = temp_path("valueless-key.parquet");
        let valueless_metadata = KeyValueMetadata::Static(vec![KeyValue {
            key: STATA_METADATA_KEY.to_string(),
            value: None,
        }]);
        write_parquet_with_metadata(&valueless_path, valueless_metadata);
        assert!(extract_capsule_from_parquet(
            valueless_path.to_str().unwrap(),
            temp_path("unused.dta").to_str().unwrap(),
        )
        .unwrap_err()
        .contains("key has no value"));

        let _ = fs::remove_file(duplicate_path);
        let _ = fs::remove_file(valueless_path);
    }

    #[test]
    fn extracted_capsule_write_failure_cleans_partial_output() {
        let capsule_path = temp_path("write-failure-capsule.dta");
        let parquet_path = temp_path("write-failure.parquet");
        write_zero_row_capsule(&capsule_path, "X", "unused");
        let metadata = build_key_value_metadata(
            capsule_path.to_str().unwrap(),
            "status",
            &HashSet::from(["status".to_string()]),
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        write_parquet_with_metadata(&parquet_path, metadata);

        let missing_parent = temp_path("missing-parent");
        let output_path = missing_parent.join("capsule.dta");
        let error = extract_capsule_from_parquet(
            parquet_path.to_str().unwrap(),
            output_path.to_str().unwrap(),
        )
        .unwrap_err();
        assert!(error.contains("Could not write extracted Stata metadata capsule"));
        assert!(!output_path.exists());

        let _ = fs::remove_file(capsule_path);
        let _ = fs::remove_file(parquet_path);
    }

    #[test]
    fn prepared_metadata_is_deterministic() {
        let capsule_path = temp_path("prepared-deterministic.dta");
        write_zero_row_capsule(&capsule_path, "Status", "unused");
        let saved = HashSet::from(["status".to_string()]);
        let first = prepare_stata_metadata(
            capsule_path.to_str().unwrap(),
            "status",
            &saved,
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        let second = prepare_stata_metadata(
            capsule_path.to_str().unwrap(),
            "status",
            &saved,
            &HashMap::new(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(first.envelope_json, second.envelope_json);
        assert_eq!(first.decoded_capsule, second.decoded_capsule);
        assert_eq!(first.columns, second.columns);
        let _ = fs::remove_file(capsule_path);
    }

    #[test]
    fn fragment_resolver_handles_file_directory_and_glob_deterministically() {
        let root = temp_path("resolver");
        let nested = root.join("group=1");
        fs::create_dir_all(&nested).unwrap();
        let first = root.join("b.parquet");
        let extensionless = root.join("extensionless");
        let second = nested.join("a.parquet");
        fs::write(&first, b"placeholder").unwrap();
        fs::write(&extensionless, b"placeholder").unwrap();
        fs::write(&second, b"placeholder").unwrap();
        fs::write(root.join("ignored.txt"), b"ignored").unwrap();

        assert_eq!(
            resolve_parquet_fragments(first.to_str().unwrap()).unwrap(),
            vec![first.clone()]
        );
        assert_eq!(
            resolve_parquet_fragments(extensionless.to_str().unwrap()).unwrap(),
            vec![extensionless]
        );
        let directory_files = resolve_parquet_fragments(root.to_str().unwrap()).unwrap();
        assert_eq!(directory_files, vec![first.clone(), second.clone()]);
        let glob_pattern = format!("{}/**/*.parquet", root.display());
        assert_eq!(
            resolve_parquet_fragments(&glob_pattern).unwrap(),
            directory_files
        );
        assert!(
            resolve_parquet_fragments(&format!("{}/missing/*.parquet", root.display()))
                .unwrap_err()
                .contains("No Parquet fragments")
        );

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn footer_states_and_uniformity_cover_absent_mixed_malformed_and_equal() {
        let capsule_path = temp_path("uniform-capsule.dta");
        let first = temp_path("uniform-first.parquet");
        let second = temp_path("uniform-second.parquet");
        let plain = temp_path("uniform-plain.parquet");
        let malformed = temp_path("uniform-malformed.parquet");
        write_zero_row_capsule(&capsule_path, "Status", "unused");
        let metadata = metadata_for_capsule(&capsule_path);
        write_parquet_with_metadata(&first, metadata.clone());
        write_parquet_with_metadata(&second, metadata);
        write_parquet_with_metadata(&plain, KeyValueMetadata::from_static(Vec::new()));
        write_parquet_with_metadata(
            &malformed,
            KeyValueMetadata::from_static(vec![(
                STATA_METADATA_KEY.to_string(),
                "not-json".to_string(),
            )]),
        );

        assert!(matches!(
            read_fragment_stata_metadata(&plain).unwrap(),
            StataMetadataFooter::Absent
        ));
        assert!(matches!(
            read_fragment_stata_metadata(&first).unwrap(),
            StataMetadataFooter::Present(_)
        ));
        assert!(validate_stata_metadata_fragments(&[plain.clone()])
            .unwrap()
            .is_none());
        assert!(
            validate_stata_metadata_fragments(&[first.clone(), second.clone()])
                .unwrap()
                .is_some()
        );

        let mixed_error =
            validate_stata_metadata_fragments(&[first.clone(), plain.clone()]).unwrap_err();
        assert!(mixed_error.contains(plain.to_str().unwrap()));
        assert!(mixed_error.contains("missing Stata metadata"));
        let reversed_mixed_error =
            validate_stata_metadata_fragments(&[plain.clone(), first.clone()]).unwrap_err();
        assert_eq!(mixed_error, reversed_mixed_error);

        let malformed_error = validate_stata_metadata_fragments(&[malformed.clone()]).unwrap_err();
        assert!(malformed_error.contains(malformed.to_str().unwrap()));
        assert!(malformed_error.contains("Invalid Stata metadata JSON"));

        for path in [capsule_path, first, second, plain, malformed] {
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn semantic_uniformity_ignores_header_timestamp_but_detects_unused_mapping() {
        let first_capsule = temp_path("semantic-first.dta");
        let equal_capsule = temp_path("semantic-equal.dta");
        let conflict_capsule = temp_path("semantic-conflict.dta");
        let first = temp_path("semantic-first.parquet");
        let equal = temp_path("semantic-equal.parquet");
        let conflict = temp_path("semantic-conflict.parquet");

        write_zero_row_capsule(&first_capsule, "Status", "unused");
        write_zero_row_capsule(&equal_capsule, "Status", "unused");
        mutate_timestamp_without_changing_semantics(&equal_capsule);
        write_zero_row_capsule(&conflict_capsule, "Status", "changed unused");
        write_parquet_with_metadata(&first, metadata_for_capsule(&first_capsule));
        write_parquet_with_metadata(&equal, metadata_for_capsule(&equal_capsule));
        write_parquet_with_metadata(&conflict, metadata_for_capsule(&conflict_capsule));

        let first_raw = match read_fragment_stata_metadata(&first).unwrap() {
            StataMetadataFooter::Present(value) => value.envelope_json,
            StataMetadataFooter::Absent => panic!("expected metadata"),
        };
        let equal_raw = match read_fragment_stata_metadata(&equal).unwrap() {
            StataMetadataFooter::Present(value) => value.envelope_json,
            StataMetadataFooter::Absent => panic!("expected metadata"),
        };
        assert_ne!(first_raw, equal_raw);
        assert!(
            validate_stata_metadata_fragments(&[first.clone(), equal.clone()])
                .unwrap()
                .is_some()
        );

        let conflict_error =
            validate_stata_metadata_fragments(&[first.clone(), conflict.clone()]).unwrap_err();
        assert!(conflict_error.contains(conflict.to_str().unwrap()));
        assert!(conflict_error.contains("conflicting Stata metadata"));
        let reversed_conflict_error =
            validate_stata_metadata_fragments(&[conflict.clone(), first.clone()]).unwrap_err();
        assert_eq!(conflict_error, reversed_conflict_error);

        for path in [
            first_capsule,
            equal_capsule,
            conflict_capsule,
            first,
            equal,
            conflict,
        ] {
            let _ = fs::remove_file(path);
        }
    }

    #[test]
    fn canonical_semantics_preserve_sharing_names_and_extended_missing_mappings() {
        let columns = vec![
            StataMetadataColumn {
                parquet_name: "left_physical".to_string(),
                capsule_name: "left".to_string(),
            },
            StataMetadataColumn {
                parquet_name: "right_physical".to_string(),
                capsule_name: "right".to_string(),
            },
        ];
        let metadata_json = |right_label_name: &str, right_variable_label: &str, z_label: &str| {
            serde_json::json!({
                "row_count": 0,
                "variables": [
                    {
                        "name": "left",
                        "label": "Left status",
                        "value_label_name": "shared_status",
                        "value_labels": {
                            "1": "observed",
                            "MISSING_a": "not answered",
                            "MISSING_z": "refused"
                        }
                    },
                    {
                        "name": "right",
                        "label": right_variable_label,
                        "value_label_name": right_label_name,
                        "value_labels": {
                            "1": "observed",
                            "MISSING_a": "not answered",
                            "MISSING_z": z_label
                        }
                    }
                ]
            })
            .to_string()
        };

        let shared = canonical_metadata_from_json(
            &metadata_json("shared_status", "Right status", "refused"),
            &columns,
        )
        .unwrap();
        let shared_again = canonical_metadata_from_json(
            &metadata_json("shared_status", "Right status", "refused"),
            &columns,
        )
        .unwrap();
        assert_eq!(shared, shared_again);

        let renamed_association = canonical_metadata_from_json(
            &metadata_json("other_status", "Right status", "refused"),
            &columns,
        )
        .unwrap();
        assert_ne!(shared, renamed_association);

        let changed_extended_missing = canonical_metadata_from_json(
            &metadata_json("shared_status", "Right status", "changed refusal"),
            &columns,
        )
        .unwrap();
        assert_ne!(shared, changed_extended_missing);

        let changed_variable_label = canonical_metadata_from_json(
            &metadata_json("shared_status", "Changed status", "refused"),
            &columns,
        )
        .unwrap();
        assert_ne!(shared, changed_variable_label);
    }

    #[test]
    fn proposed_metadata_compatibility_accepts_semantics_and_rejects_missing_or_conflicting() {
        let first_capsule = temp_path("compatibility-first.dta");
        let equal_capsule = temp_path("compatibility-equal.dta");
        let conflict_capsule = temp_path("compatibility-conflict.dta");
        write_zero_row_capsule(&first_capsule, "Status", "unused");
        write_zero_row_capsule(&equal_capsule, "Status", "unused");
        mutate_timestamp_without_changing_semantics(&equal_capsule);
        write_zero_row_capsule(&conflict_capsule, "Status", "changed unused");

        let prepare = |path: &Path| {
            prepare_stata_metadata(
                path.to_str().unwrap(),
                "status",
                &HashSet::from(["status".to_string()]),
                &HashMap::new(),
            )
            .unwrap()
            .unwrap()
        };
        let first = prepare(&first_capsule);
        let equal = prepare(&equal_capsule);
        let conflict = prepare(&conflict_capsule);

        assert!(validate_stata_metadata_compatibility(Some(&first), Some(&first)).is_ok());
        assert!(validate_stata_metadata_compatibility(Some(&first), Some(&equal)).is_ok());
        assert!(validate_stata_metadata_compatibility(None, None).is_ok());
        assert!(validate_stata_metadata_compatibility(None, Some(&first)).is_err());
        assert!(validate_stata_metadata_compatibility(Some(&first), None).is_err());
        assert!(validate_stata_metadata_compatibility(Some(&first), Some(&conflict)).is_err());

        let agreed = reconcile_stata_metadata_for_append(Some(first.clone()), Some(&equal))
            .unwrap()
            .unwrap();
        assert_eq!(agreed.envelope_json, first.envelope_json);
        assert_ne!(agreed.envelope_json, equal.envelope_json);

        for path in [first_capsule, equal_capsule, conflict_capsule] {
            let _ = fs::remove_file(path);
        }
    }
}
