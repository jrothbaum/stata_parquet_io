use std::collections::{HashMap, HashSet};
use std::fs::File;

use polars_parquet::parquet::statistics::Statistics;

use crate::stata_metadata::resolve_all_parquet_files;

/// Reads the min/max range of every requested integer-family column directly
/// from Parquet row-group footer statistics - no data scan, just the file
/// footer(s) already paid for by other metadata reads.
///
/// A column is included in the result ONLY when every row group of every
/// resolved file (file, directory, or glob) carries present, non-null
/// statistics for it. `Statistics::deserialize` (in polars-parquet) already
/// clears min/max to `None` when a writer marked them non-exact, so "present
/// and non-null" here already implies exact. Anything less than that - a
/// missing column, absent stats, a malformed footer, a column not present in
/// every file - drops the column from the result entirely. Callers MUST
/// treat "not present" as "not safe to use" and fall back to the existing
/// conservative type mapping; this function never guesses.
pub fn integer_ranges_from_footer_stats(
    files: &[String],
    column_names: &[String],
) -> HashMap<String, (i64, i64)> {
    let mut ranges: HashMap<String, (i64, i64)> = HashMap::new();
    let mut untrustworthy: HashSet<String> = HashSet::new();

    for path in files {
        let mut file = match File::open(path) {
            Ok(f) => f,
            Err(_) => {
                // Can't verify anything for any column without the footer.
                for name in column_names {
                    untrustworthy.insert(name.clone());
                }
                continue;
            }
        };
        let metadata = match polars_parquet::read::read_metadata(&mut file) {
            Ok(m) => m,
            Err(_) => {
                for name in column_names {
                    untrustworthy.insert(name.clone());
                }
                continue;
            }
        };

        for name in column_names {
            if untrustworthy.contains(name) {
                continue;
            }

            let mut file_min: Option<i64> = None;
            let mut file_max: Option<i64> = None;
            let mut saw_row_group = false;

            for row_group in &metadata.row_groups {
                saw_row_group = true;
                let Some(mut chunks) = row_group.columns_under_root_iter(name) else {
                    untrustworthy.insert(name.clone());
                    break;
                };
                // A flat (non-nested) column has exactly one chunk per row
                // group; anything else (0, or >1 from a nested schema) can't
                // be safely reduced to a single range.
                let (Some(chunk), None) = (chunks.next(), chunks.next()) else {
                    untrustworthy.insert(name.clone());
                    break;
                };

                let stats = match chunk.statistics() {
                    Some(Ok(s)) => s,
                    _ => {
                        untrustworthy.insert(name.clone());
                        break;
                    }
                };

                let (mn, mx): (i64, i64) = match stats {
                    Statistics::Int32(s) => match (s.min_value, s.max_value) {
                        (Some(mn), Some(mx)) => (mn as i64, mx as i64),
                        _ => {
                            untrustworthy.insert(name.clone());
                            break;
                        }
                    },
                    Statistics::Int64(s) => match (s.min_value, s.max_value) {
                        (Some(mn), Some(mx)) => (mn, mx),
                        _ => {
                            untrustworthy.insert(name.clone());
                            break;
                        }
                    },
                    // Not an integer-family physical type (float/boolean/etc.) -
                    // out of scope for this function; caller shouldn't have
                    // asked, but be safe rather than misinterpret bytes.
                    _ => {
                        untrustworthy.insert(name.clone());
                        break;
                    }
                };

                file_min = Some(file_min.map_or(mn, |cur| cur.min(mn)));
                file_max = Some(file_max.map_or(mx, |cur| cur.max(mx)));
            }

            if untrustworthy.contains(name) {
                continue;
            }
            if !saw_row_group {
                // Zero-row-group file: nothing to widen for, but also
                // nothing unsafe about it - leave whatever range other
                // files already contributed untouched.
                continue;
            }
            if let (Some(mn), Some(mx)) = (file_min, file_max) {
                ranges
                    .entry(name.clone())
                    .and_modify(|(emn, emx)| {
                        *emn = (*emn).min(mn);
                        *emx = (*emx).max(mx);
                    })
                    .or_insert((mn, mx));
            } else {
                untrustworthy.insert(name.clone());
            }
        }
    }

    for name in &untrustworthy {
        ranges.remove(name);
    }
    ranges
}

/// Convenience wrapper: resolves `path` (file, directory, or glob) to its
/// Parquet file list, then delegates to [`integer_ranges_from_footer_stats`].
pub fn integer_ranges_for_path(path: &str, column_names: &[String]) -> HashMap<String, (i64, i64)> {
    let files = resolve_all_parquet_files(path);
    if files.is_empty() {
        return HashMap::new();
    }
    integer_ranges_from_footer_stats(&files, column_names)
}
