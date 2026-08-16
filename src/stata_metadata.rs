use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::path::Path;

use polars::prelude::{KeyValueMetadata, PlSmallStr};
use serde::{Deserialize, Serialize};

use crate::mapping::{resolve_stata_type, StataColumnInfo};
use crate::stata_interface::{display, get_macro, set_macro};

/// Resolves a `pq use` path (file, directory, or glob) to every Parquet
/// file it covers, in glob order.
pub fn resolve_all_parquet_files(path: &str) -> Vec<String> {
    let path_obj = Path::new(path);
    if path_obj.is_file() {
        return vec![path.to_string()];
    }
    let pattern = if path_obj.is_dir() {
        format!("{}/**/*.parquet", path.replace('\\', "/"))
    } else {
        path.replace('\\', "/").replace("**.", "**/*.")
    };
    glob::glob(&pattern)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|p| p.to_string_lossy().to_string())
        .collect()
}

/// Resolves a `pq use` path (file, directory, or glob) to a single
/// representative Parquet file - the cheap path when the caller isn't
/// validating cross-file agreement (e.g. write-side consolidate, which
/// is reading metadata it just wrote itself).
pub fn first_parquet_file(path: &str) -> Option<String> {
    resolve_all_parquet_files(path).into_iter().next()
}

pub const STATA_METADATA_KEY: &str = "org.stata.pq.labels.v1";
const STATA_METADATA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VariableMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
    // Stata display format (e.g. "%9.2f", "%td") as it was at save time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    // Stata storage type at save time ("byte", "int", "long", "float",
    // "double", "date", "time", "datetime"). Read-side treats this as a
    // hint, never a guarantee: it is only trusted when Parquet row-group
    // footer statistics independently confirm the file's actual values
    // still fit it (see parquet_stats::integer_ranges_for_path) - a stale
    // or foreign value here can only widen the read type, never narrow it
    // below what the real data requires. Not recorded for string/strL/
    // binary columns; footer stats can't verify string length the same way.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stata_type: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StataMetadataEnvelope {
    pub version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset_label: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dataset_notes: Vec<String>,
    // Keyed by the physical Parquet column name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub variables: BTreeMap<String, VariableMetadata>,
    // Value-label name -> {code: text}, stored once regardless of how many
    // variables share the same value-label definition.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub value_labels: BTreeMap<String, BTreeMap<String, String>>,
}

/// Gathers Stata metadata that pq.ado staged in indexed macros (pq_meta_*)
/// before the plugin call. Mirrors write::column_info_from_macros, which
/// reads the same kind of indexed macros for plain column info. Returns
/// None when the caller didn't request statametadata (pq_meta_count unset
/// or zero), so ordinary saves pay no extra cost here.
///
/// `column_info` (already resolved, post-rename - the same list used to
/// build the write schema) supplies the exact Stata storage type per
/// variable, so it can be recorded without re-deriving it from macros.
pub fn metadata_from_macros(
    rename_list: &HashMap<PlSmallStr, PlSmallStr>,
    column_info: &[StataColumnInfo],
) -> Option<StataMetadataEnvelope> {
    let n_vars: usize = get_macro("pq_meta_count", false, None).parse().unwrap_or(0);
    if n_vars == 0 {
        return None;
    }

    let column_info_by_name: HashMap<&str, &StataColumnInfo> = column_info
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    let mut envelope = StataMetadataEnvelope {
        version: STATA_METADATA_VERSION,
        ..Default::default()
    };

    let dataset_label = get_macro("pq_meta_dataset_label", false, None);
    if !dataset_label.is_empty() {
        envelope.dataset_label = Some(dataset_label);
    }
    envelope.dataset_notes = read_indexed_list("pq_meta_dataset_note");

    // Value labels are indexed by a small integer (m), not by name, since
    // Stata macro *names* are capped at 32 characters and a value-label
    // name embedded directly into a macro name (e.g. a name-keyed
    // "pq_meta_vallabel_defn_count_<name>") can blow past that.
    let n_vallabels: usize = get_macro("pq_meta_vallabel_count", false, None).parse().unwrap_or(0);
    let value_label_index: HashMap<String, usize> = (1..=n_vallabels)
        .map(|m| (get_macro(&format!("pq_meta_vallabel_name_{m}"), false, None), m))
        .collect();

    for i in 1..=n_vars {
        let stata_name = get_macro(&format!("pq_meta_name_{i}"), false, None);
        if stata_name.is_empty() {
            continue;
        }
        // The staged name is the pre-rename Stata variable name; resolve it
        // through the same rename_list write_from_stata used for the actual
        // Parquet columns, so the metadata key matches what lands in the
        // file for long/invalid Stata names.
        let parquet_name = rename_list
            .get(&PlSmallStr::from(stata_name.as_str()))
            .map(|renamed| renamed.to_string())
            .unwrap_or(stata_name);

        let label = get_macro(&format!("pq_meta_label_{i}"), false, None);
        let value_label = get_macro(&format!("pq_meta_vallabel_{i}"), false, None);
        let notes = read_indexed_list(&format!("pq_meta_note_{i}"));
        let var_format = get_macro(&format!("pq_meta_format_{i}"), false, None);
        let stata_type = column_info_by_name
            .get(parquet_name.as_str())
            .map(|c| resolve_stata_type(&c.dtype, &c.format).to_string().to_string());

        if label.is_empty() && value_label.is_empty() && notes.is_empty()
            && var_format.is_empty() && stata_type.is_none() {
            continue;
        }

        if !value_label.is_empty() && !envelope.value_labels.contains_key(&value_label) {
            if let Some(defn) = value_label_index.get(&value_label).map(|&m| read_value_label_definition(m)) {
                if !defn.is_empty() {
                    envelope.value_labels.insert(value_label.clone(), defn);
                }
            }
        }

        envelope.variables.insert(
            parquet_name,
            VariableMetadata {
                label: if label.is_empty() { None } else { Some(label) },
                value_label: if value_label.is_empty() { None } else { Some(value_label) },
                notes,
                format: if var_format.is_empty() { None } else { Some(var_format) },
                stata_type,
            },
        );
    }

    if envelope.variables.is_empty() && envelope.dataset_label.is_none() && envelope.dataset_notes.is_empty() {
        return None;
    }

    Some(envelope)
}

fn read_indexed_list(prefix: &str) -> Vec<String> {
    let count: usize = get_macro(&format!("{prefix}_count"), false, None)
        .parse()
        .unwrap_or(0);
    (1..=count)
        .map(|i| get_macro(&format!("{prefix}_{i}"), false, None))
        .collect()
}

fn read_value_label_definition(m: usize) -> BTreeMap<String, String> {
    let count: usize = get_macro(&format!("pq_meta_vallabel_defn_count_{m}"), false, None)
        .parse()
        .unwrap_or(0);
    let mut defn = BTreeMap::new();
    for i in 1..=count {
        let code = get_macro(&format!("pq_meta_vallabel_defn_code_{m}_{i}"), false, None);
        let text = get_macro(&format!("pq_meta_vallabel_defn_text_{m}_{i}"), false, None);
        if !code.is_empty() {
            defn.insert(code, text);
        }
    }
    defn
}

pub fn build_key_value_metadata(envelope: &StataMetadataEnvelope) -> Option<KeyValueMetadata> {
    let json = serde_json::to_string(envelope).ok()?;
    Some(KeyValueMetadata::from_static(vec![(
        STATA_METADATA_KEY.to_string(),
        json,
    )]))
}

/// Reads the Stata metadata footer entry from a single Parquet file, as
/// the raw JSON string still attached (no parse) - used for byte-exact
/// cross-file comparison, since two envelopes that are semantically
/// identical but serialized differently would otherwise compare unequal
/// (or, worse, equal by accident after a lossy parse).
fn read_raw_metadata_json(path: &str) -> Option<String> {
    let mut file = File::open(path).ok()?;
    let metadata = polars_parquet::read::read_metadata(&mut file).ok()?;
    metadata
        .key_value_metadata()
        .as_ref()?
        .iter()
        .find(|kv| kv.key == STATA_METADATA_KEY)?
        .value
        .clone()
}

/// Reads the Stata metadata envelope from a single Parquet file's footer.
/// For directories/globs, callers pass the first resolved file - hive
/// files are expected to carry identical metadata. Prefer
/// read_metadata_validated when that expectation actually needs checking.
pub fn read_metadata_from_parquet(path: &str) -> Option<StataMetadataEnvelope> {
    serde_json::from_str(&read_raw_metadata_json(path)?).ok()
}

/// Reads Stata metadata for a `pq use` path, checking that every resolved
/// Parquet file (file, directory, or glob) carries the same footer
/// entry before trusting it - a directory/glob read only ever inspects one
/// file's data, so a silently mixed or conflicting footer would
/// otherwise show metadata that doesn't actually describe the whole read.
/// Comparison is on the raw JSON string, not the parsed envelope, so it
/// also catches a file with a different format_version.
pub fn read_metadata_validated(path: &str) -> Result<Option<StataMetadataEnvelope>, String> {
    let files = resolve_all_parquet_files(path);
    let Some(first_path) = files.first() else {
        return Ok(None);
    };
    let first_json = read_raw_metadata_json(first_path);

    for other_path in &files[1..] {
        let other_json = read_raw_metadata_json(other_path);
        if other_json != first_json {
            return Err(format!(
                "Parquet files have conflicting or mixed Stata metadata ({} vs {}); \
                 use nostatametadata to load the data anyway",
                first_path, other_path
            ));
        }
    }

    match first_json {
        None => Ok(None),
        Some(json) => serde_json::from_str(&json)
            .map(Some)
            .map_err(|e| format!("Invalid Stata metadata in {}: {}", first_path, e)),
    }
}

/// Backs `pq metadata` (analogous to `pq describe`) and the pre-flight
/// check `pq use`/`pq append` run before clearing any existing data: finds
/// the embedded envelope for a file/directory/glob, prints a readable
/// table unless quietly is set, and stages the pq_meta_* macros so ado can
/// `return local` values programmatically. Returns Err on a genuine
/// problem (conflicting/malformed metadata) so callers can fail before
/// mutating the caller's Stata session - "no metadata present" is not an
/// error and returns Ok.
pub fn describe_metadata(path: &str, quietly: bool) -> Result<(), String> {
    match read_metadata_validated(path) {
        Ok(Some(envelope)) => {
            if !quietly {
                print_metadata_table(&envelope);
            }
            push_metadata_to_macros(&envelope);
            Ok(())
        }
        Ok(None) => {
            if !quietly {
                display("No Stata metadata (statametadata) found in this Parquet file.");
            }
            clear_metadata_macro();
            Ok(())
        }
        Err(e) => {
            clear_metadata_macro();
            Err(e)
        }
    }
}

fn print_metadata_table(envelope: &StataMetadataEnvelope) {
    if let Some(label) = &envelope.dataset_label {
        display(&format!("Dataset label: {label}"));
    }
    for (i, note) in envelope.dataset_notes.iter().enumerate() {
        display(&format!("Dataset note {}: {}", i + 1, note));
    }
    if envelope.variables.is_empty() {
        display("No variable-level metadata.");
        return;
    }

    display("");
    display(&format!(
        "{:<32} | {:<32} | {:<20} | {:<10} | Notes",
        "Variable", "Label", "Value label", "Format"
    ));
    display(&"-".repeat(102));
    for (name, var) in &envelope.variables {
        display(&format!(
            "{:<32} | {:<32} | {:<20} | {:<10} | {}",
            name,
            var.label.as_deref().unwrap_or(""),
            var.value_label.as_deref().unwrap_or(""),
            var.format.as_deref().unwrap_or(""),
            var.notes.len(),
        ));
    }

    if !envelope.value_labels.is_empty() {
        display("");
        display("Value labels:");
        for (name, defn) in &envelope.value_labels {
            let pairs: Vec<String> = defn.iter().map(|(code, text)| format!("{code}={text}")).collect();
            display(&format!("  {name}: {}", pairs.join(", ")));
        }
    }
}

/// Pushes a read envelope back to Stata as indexed macros, mirroring
/// mapping::schema_with_stata_types's set_macro loop for column info.
pub fn push_metadata_to_macros(envelope: &StataMetadataEnvelope) {
    set_macro("pq_meta_present", "1", false);

    set_macro(
        "pq_meta_dataset_label",
        envelope.dataset_label.as_deref().unwrap_or(""),
        false,
    );
    write_indexed_list("pq_meta_dataset_note", &envelope.dataset_notes);

    set_macro("pq_meta_count", &envelope.variables.len().to_string(), false);
    for (i, (parquet_name, var)) in envelope.variables.iter().enumerate() {
        let idx = i + 1;
        set_macro(&format!("pq_meta_name_{idx}"), parquet_name, false);
        set_macro(
            &format!("pq_meta_label_{idx}"),
            var.label.as_deref().unwrap_or(""),
            false,
        );
        set_macro(
            &format!("pq_meta_vallabel_{idx}"),
            var.value_label.as_deref().unwrap_or(""),
            false,
        );
        set_macro(
            &format!("pq_meta_format_{idx}"),
            var.format.as_deref().unwrap_or(""),
            false,
        );
        write_indexed_list(&format!("pq_meta_note_{idx}"), &var.notes);
    }

    // Indexed by a small integer (m), not by name - see the matching note
    // in metadata_from_macros about the 32-character macro-name limit.
    set_macro("pq_meta_vallabel_count", &envelope.value_labels.len().to_string(), false);
    for (m, (name, defn)) in envelope.value_labels.iter().enumerate() {
        let m = m + 1;
        set_macro(&format!("pq_meta_vallabel_name_{m}"), name, false);
        set_macro(&format!("pq_meta_vallabel_defn_count_{m}"), &defn.len().to_string(), false);
        for (i, (code, text)) in defn.iter().enumerate() {
            let idx = i + 1;
            set_macro(&format!("pq_meta_vallabel_defn_code_{m}_{idx}"), code, false);
            set_macro(&format!("pq_meta_vallabel_defn_text_{m}_{idx}"), text, false);
        }
    }
}

pub fn clear_metadata_macro() {
    set_macro("pq_meta_present", "0", false);
    set_macro("pq_meta_count", "0", false);
}

fn write_indexed_list(prefix: &str, values: &[String]) {
    set_macro(&format!("{prefix}_count"), &values.len().to_string(), false);
    for (i, value) in values.iter().enumerate() {
        set_macro(&format!("{prefix}_{}", i + 1), value, false);
    }
}
