use std::sync::Mutex;
use polars::prelude::DataFrame;

#[derive(PartialEq, Clone)]
pub struct FastCacheKey {
    pub path: String,
    pub sql_if: String,
    pub columns: Vec<String>,   // sorted; empty = all columns
    pub format: String,
    pub parse_dates: bool,
    pub infer_schema_length: usize,
}

struct FastCache {
    key: FastCacheKey,
    df: DataFrame,
}

static CACHE: Mutex<Option<FastCache>> = Mutex::new(None);

/// Parse a space-separated varlist into a sorted Vec<String>.
/// Empty input returns an empty Vec (meaning "all columns").
pub fn parse_varlist(varlist: &str) -> Vec<String> {
    let trimmed = varlist.trim();
    if trimmed.is_empty() {
        return vec![];
    }
    let mut cols: Vec<String> = trimmed
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();
    cols.sort();
    cols
}

/// Returns true if `name` matches `pattern` using Stata-style wildcards:
///   `*`  matches any sequence of characters (including empty)
///   `?`  matches exactly one character
fn stata_glob_match(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return name.is_empty();
    }
    let mut p = pattern.chars();
    let first = p.next().unwrap();
    let rest_p = p.as_str();
    match first {
        '*' => {
            // '*' matches the empty prefix, or consume one char from name at a time
            if stata_glob_match(rest_p, name) {
                return true;
            }
            let mut n = name.chars();
            while n.next().is_some() {
                if stata_glob_match(rest_p, n.as_str()) {
                    return true;
                }
            }
            false
        }
        '?' => {
            if name.is_empty() {
                false
            } else {
                let mut n = name.chars();
                n.next();
                stata_glob_match(rest_p, n.as_str())
            }
        }
        c => {
            let mut n = name.chars();
            match n.next() {
                Some(nc) if nc == c => stata_glob_match(rest_p, n.as_str()),
                _ => false,
            }
        }
    }
}

/// Resolve a namelist (with Stata-style `*`/`?` wildcards) against `schema_cols`
/// (the columns in the file, in file order), then apply `drop_list`.
///
/// Matching rules (same as Stata's `pq_match_variables`):
///   - Patterns are processed in namelist order.
///   - For each pattern, matching columns are added in *file* order, deduped.
///   - Wildcard patterns that match nothing are silently skipped.
///   - Exact names that are not found produce an `Err`.
///   - Empty namelist or `"*"` means all columns.
/// Drop patterns support wildcards too.
///
/// Returns the ordered, deduplicated list of matched columns after drops.
pub fn resolve_varlist(
    namelist: &str,
    schema_cols: &[&str],
    drop_list: &str,
) -> Result<Vec<String>, String> {
    let trimmed = namelist.trim();

    let mut matched: Vec<String> = if trimmed.is_empty() || trimmed == "*" {
        schema_cols.iter().map(|s| s.to_string()).collect()
    } else {
        let mut result: Vec<String> = Vec::new();
        let mut unmatched_exact: Vec<&str> = Vec::new();

        for pattern in trimmed.split_whitespace() {
            let is_wildcard = pattern.contains('*') || pattern.contains('?');
            let mut found = false;
            for &col in schema_cols {
                let hit = if is_wildcard {
                    stata_glob_match(pattern, col)
                } else {
                    col == pattern
                };
                if hit && !result.iter().any(|m| m == col) {
                    result.push(col.to_string());
                    found = true;
                }
            }
            if !found && !is_wildcard {
                unmatched_exact.push(pattern);
            }
        }

        if !unmatched_exact.is_empty() {
            return Err(format!(
                "The following variable(s) were not found: {}",
                unmatched_exact.join(" ")
            ));
        }
        result
    };

    // Apply drop list (wildcards supported)
    let drop_trimmed = drop_list.trim();
    if !drop_trimmed.is_empty() {
        matched.retain(|col| {
            !drop_trimmed.split_whitespace().any(|dpat| {
                if dpat.contains('*') || dpat.contains('?') {
                    stata_glob_match(dpat, col)
                } else {
                    col == dpat
                }
            })
        });
    }

    Ok(matched)
}

/// Store a DataFrame in the cache, replacing any prior entry.
/// Called by describe when fast mode is active.
pub fn store(key: FastCacheKey, df: DataFrame) {
    let mut lock = CACHE.lock().unwrap();
    *lock = Some(FastCache { key, df });
}

/// Consume the cached DataFrame if the key matches.
/// Returns None if there is no cache or the key does not match.
/// A mismatch does NOT clear the cache — only a new describe or clear() does.
pub fn take(key: &FastCacheKey) -> Option<DataFrame> {
    let mut lock = CACHE.lock().unwrap();
    if lock.as_ref().map(|c| &c.key == key).unwrap_or(false) {
        lock.take().map(|c| c.df)
    } else {
        None
    }
}

/// Clear the cache unconditionally.
/// Called by describe when fast mode is NOT active, preventing a prior
/// fast-cached frame from being consumed by a subsequent read.
pub fn clear() {
    let mut lock = CACHE.lock().unwrap();
    *lock = None;
}
