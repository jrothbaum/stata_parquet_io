set varabbrev off

// Test: integer values that fall in Stata's reserved "extended missing" range
//
// Stata reserves the top of each integer type for missing values:
//   byte / Int8  : 101–127 reserved (.a=101 … .z=126, .=127)
//   int  / Int16 : 32741–32767 reserved
//   long / Int32 : 2147483621–2147483647 reserved
//
// A parquet file created externally (e.g. Python/Polars) may contain Int8/16/32
// values in those ranges that represent genuine data, not missing.  When pq use
// stores them into Stata via replace_number() the numeric value falls in the
// reserved range and Stata silently treats it as an extended missing (.a, .b, …).
//
// This file documents current behaviour.  A value of PASS means the value
// survived as a number; MISSING means it was converted to an extended missing.
// Run stata_null_range_setup.py first to generate stata_null_range.parquet.

local f "stata_null_range.parquet"

pq use "`f'", clear

// ── byte / Int8 ───────────────────────────────────────────────────────────────
// Row 1: i8_valid=50 (well inside range) — should always survive
assert i8_valid[1] == 50
di "PASS: i8_valid[1] == 50"

// Row 2: i8_valid=100 (maximum valid byte) — should survive
assert i8_valid[2] == 100
di "PASS: i8_valid[2] == 100"

// Row 1: i8_reserved=101 (first reserved value for byte) — stored as Stata int, safe
assert i8_reserved[1] == 101
di "PASS: i8_reserved[1] == 101 preserved as integer (stored as Stata int)"

// Row 2: i8_reserved=120 — stored as Stata int, safe
assert i8_reserved[2] == 120
di "PASS: i8_reserved[2] == 120 preserved as integer (stored as Stata int)"

// ── int / Int16 ───────────────────────────────────────────────────────────────
assert i16_valid[1] == 1000
di "PASS: i16_valid[1] == 1000"

assert i16_valid[2] == 32740
di "PASS: i16_valid[2] == 32740  (max valid int)"

// stored as Stata long, so 32741-32767 are valid integers
assert i16_reserved[1] == 32741
di "PASS: i16_reserved[1] == 32741 preserved as integer (stored as Stata long)"

assert i16_reserved[2] == 32760
di "PASS: i16_reserved[2] == 32760 preserved as integer (stored as Stata long)"

// ── long / Int32 ──────────────────────────────────────────────────────────────
assert i32_valid[1] == 1000
di "PASS: i32_valid[1] == 1000"

assert i32_valid[2] == 2147483620
di "PASS: i32_valid[2] == 2147483620  (max valid long)"

// stored as Stata double, so 2147483621-2147483647 are valid (f64 represents all Int32 exactly)
assert i32_reserved[1] == 2147483621
di "PASS: i32_reserved[1] == 2147483621 preserved as integer (stored as Stata double)"

assert i32_reserved[2] == 2147483640
di "PASS: i32_reserved[2] == 2147483640 preserved as integer (stored as Stata double)"

di ""
di "stata_null_range tests complete."
