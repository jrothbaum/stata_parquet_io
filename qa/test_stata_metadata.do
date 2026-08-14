*! test_stata_metadata.do - single-file Parquet Stata metadata round trips
*! Package: pq
*! Purpose: Functional and negative-path QA for statametadata
*! Author: Timothy P Copeland, Karolinska Institutet

version 16
capture log close _all
log using "test_stata_metadata.log", text replace

**# Setup
local qa_dir "`c(pwd)'"
local repo_dir = regexr("`qa_dir'", "/qa$", "")
local ado_dir "`repo_dir'/src/ado/p"
local plugin_file "`repo_dir'/target/release/libstata_parquet_io.so"
local metadata_file "`qa_dir'/stata_metadata_roundtrip.parquet"
local plain_file "`qa_dir'/stata_metadata_plain.parquet"
local empty_file "`qa_dir'/stata_metadata_empty.parquet"
local reject_file "`qa_dir'/stata_metadata_reject.parquet"
local long_file "`qa_dir'/stata_metadata_long_name.parquet"
local long_copy_file "`qa_dir'/stata_metadata_long_name_copy.parquet"
local malformed_file "`qa_dir'/stata_metadata_malformed.parquet"
local merge_file "`qa_dir'/stata_metadata_merge_scope.parquet"
local format_file "`qa_dir'/stata_metadata_formats.parquet"
local type_file "`qa_dir'/stata_metadata_types.parquet"
local widened_file "`qa_dir'/stata_metadata_widened.parquet"
local widened_ok "`qa_dir'/stata_metadata_widened.ok"
local generic_ok "`qa_dir'/stata_metadata_generic.ok"
local test_count = 0
local pass_count = 0

adopath ++ "`ado_dir'"
capture program drop polars_parquet_plugin
program polars_parquet_plugin, plugin using("`plugin_file'")

foreach path in "`metadata_file'" "`plain_file'" "`empty_file'" "`reject_file'" ///
	"`long_file'" "`long_copy_file'" "`malformed_file'" "`merge_file'" ///
	"`format_file'" "`type_file'" "`widened_file'" {
    capture erase `path'
}
capture erase "`generic_ok'"
capture erase "`widened_ok'"

**# Numeric and label round trip
local ++test_count
clear
set obs 3
generate byte status = cond(_n == 1, -1, 1)
generate byte status_copy = status
generate byte byte_value = -100 + _n
generate int int_value = 1000 + _n
generate long long_value = 1000000 + _n
generate float float_value = 1.25 * _n
generate double double_value = 1.125 * _n

mata: st_varlabel(st_varindex("status"), "Patient status – Ångström " + char(96) + "literal" + char(39) + " " + char(34) + "quoted" + char(34))
label variable status_copy "Shared status – 東京"
label define Status_Shared_Exact -1 "Negative – låg" 1 "Observed {one}" ///
    2 "Unused mapping" 3 "placeholder" .a "Extended missing Å" .z "Extended missing Ω"
mata: st_vlmodify("Status_Shared_Exact", 3, "Code says " + char(96) + "literal" + char(39) + " and " + char(34) + "quoted" + char(34))
label values status Status_Shared_Exact
label values status_copy Status_Shared_Exact

pq save using "`metadata_file'", replace statametadata
shell python3 "`qa_dir'/check_stata_metadata.py" roundtrip "`metadata_file'" "`generic_ok'"
confirm file "`generic_ok'"
erase "`generic_ok'"

pq use using "`metadata_file'", clear
assert _N == 3
confirm numeric variable status
confirm numeric variable status_copy
confirm numeric variable byte_value
confirm numeric variable int_value
confirm numeric variable long_value
confirm numeric variable float_value
confirm numeric variable double_value
assert status == cond(_n == 1, -1, 1)
assert status_copy == status
assert byte_value == -100 + _n
assert int_value == 1000 + _n
assert long_value == 1000000 + _n
assert float_value == float(1.25 * _n)
assert double_value == 1.125 * _n

local copy_variable_label : variable label status_copy
local status_value_label : value label status
local copy_value_label : value label status_copy
mata: assert(st_varlabel(st_varindex("status")) == "Patient status – Ångström " + char(96) + "literal" + char(39) + " " + char(34) + "quoted" + char(34))
assert `"`copy_variable_label'"' == "Shared status – 東京"
assert "`status_value_label'" == "Status_Shared_Exact"
assert "`copy_value_label'" == "Status_Shared_Exact"
mata: st_local("unused_mapping", st_vlmap("Status_Shared_Exact", 2))
mata: st_local("missing_a_mapping", st_vlmap("Status_Shared_Exact", .a))
mata: st_local("missing_z_mapping", st_vlmap("Status_Shared_Exact", .z))
assert "`unused_mapping'" == "Unused mapping"
assert "`missing_a_mapping'" == "Extended missing Å"
assert "`missing_z_mapping'" == "Extended missing Ω"
mata: assert(st_vlmap("Status_Shared_Exact", 3) == "Code says " + char(96) + "literal" + char(39) + " and " + char(34) + "quoted" + char(34))
local ++pass_count

**# Projected-column restore
local ++test_count
pq use status using "`metadata_file'", clear
assert _N == 3
confirm variable status, exact
capture confirm variable status_copy, exact
assert _rc == 111
local projected_value_label : value label status
assert "`projected_value_label'" == "Status_Shared_Exact"
mata: assert(st_varlabel(st_varindex("status")) == "Patient status – Ångström " + char(96) + "literal" + char(39) + " " + char(34) + "quoted" + char(34))
mata: assert(st_vlmap("Status_Shared_Exact", 2) == "Unused mapping")
local ++pass_count

**# Metadata opt-out and legacy compatibility
local ++test_count
pq use using "`metadata_file'", clear nostatametadata
local status_variable_label : variable label status
local status_value_label : value label status
assert `"`status_variable_label'"' == ""
assert "`status_value_label'" == ""
assert status == cond(_n == 1, -1, 1)

clear
set obs 2
generate int x = 1000 + _n
label variable x "Not embedded"
pq save using "`plain_file'", replace
pq use using "`plain_file'", clear
assert x == 1000 + _n
local plain_variable_label : variable label x
assert `"`plain_variable_label'"' == ""
local ++pass_count

**# Long physical Parquet name and safe metadata map
local ++test_count
clear
set obs 2
generate byte status_source = _n
local long_physical_name "status_with_a_name_far_beyond_thirty_two_characters"
char status_source[_pq_parquet_name] "`long_physical_name'"
label variable status_source "Long physical name – Å"
label define Long_Name_Label 1 "one" 2 "two" 9 "unused"
label values status_source Long_Name_Label
pq save using "`long_file'", replace statametadata
pq use using "`long_file'", clear
unab long_loaded_var : _all
local restored_physical_name : char `long_loaded_var'[_pq_parquet_name]
local restored_long_label : variable label `long_loaded_var'
local restored_long_value_label : value label `long_loaded_var'
assert "`restored_physical_name'" == "`long_physical_name'"
assert `"`restored_long_label'"' == "Long physical name – Å"
assert "`restored_long_value_label'" == "Long_Name_Label"
assert `long_loaded_var' == _n
pq save using "`long_copy_file'", replace statametadata
shell python3 "`qa_dir'/check_stata_metadata.py" column "`long_copy_file'" ///
    "`generic_ok'" "`long_physical_name'"
confirm file "`generic_ok'"
erase "`generic_ok'"
local ++pass_count

**# Malformed footer rejection and metadata opt-out
local ++test_count
shell python3 "`qa_dir'/check_stata_metadata.py" malformed "`plain_file'" ///
    "`malformed_file'" "`generic_ok'"
confirm file "`generic_ok'"
erase "`generic_ok'"
set varabbrev on
capture noisily pq use using "`malformed_file'", clear
local malformed_rc = _rc
assert `malformed_rc' == 198
assert "`c(varabbrev)'" == "on"
pq use using "`malformed_file'", clear nostatametadata
assert x == 1000 + _n
local ++pass_count

**# Merge carries labels from using dataset
local ++test_count
clear
set obs 2
generate byte id = _n
generate byte source_code = _n
label variable source_code "Source code label"
label define Merge_Scope_Label 1 "one" 2 "two"
label values source_code Merge_Scope_Label
pq save using "`merge_file'", replace statametadata
clear
set obs 2
generate byte id = _n
pq merge 1:1 id using "`merge_file'", nogenerate
assert source_code == _n
local merged_variable_label : variable label source_code
local merged_value_label : value label source_code
assert `"`merged_variable_label'"' == "Source code label"
assert "`merged_value_label'" == "Merge_Scope_Label"
mata: st_local("merged_mapping_1", st_vlmap("Merge_Scope_Label", 1))
mata: st_local("merged_mapping_2", st_vlmap("Merge_Scope_Label", 2))
assert "`merged_mapping_1'" == "one"
assert "`merged_mapping_2'" == "two"
local ++pass_count

**# Merge: master value-label definition wins over using
local ++test_count
clear
set obs 2
generate byte id = _n
generate byte source_code = _n
label define Merge_Scope_Label 1 "master-one" 2 "master-two"
label values source_code Merge_Scope_Label
pq merge 1:1 id using "`merge_file'", nogenerate
mata: st_local("master_wins_1", st_vlmap("Merge_Scope_Label", 1))
mata: st_local("master_wins_2", st_vlmap("Merge_Scope_Label", 2))
assert "`master_wins_1'" == "master-one"
assert "`master_wins_2'" == "master-two"
local ++pass_count

**# Empty dataset
local ++test_count
clear
set obs 1
generate byte empty_status = 1
label variable empty_status "Empty dataset – metadata"
label define Empty_Status_Exact 1 "Present but unused" .a "Empty missing Å"
label values empty_status Empty_Status_Exact
drop in 1
assert _N == 0

pq save using "`empty_file'", replace statametadata
pq use using "`empty_file'", clear
assert _N == 0
confirm numeric variable empty_status
local empty_variable_label : variable label empty_status
local empty_value_label : value label empty_status
assert `"`empty_variable_label'"' == "Empty dataset – metadata"
assert "`empty_value_label'" == "Empty_Status_Exact"
mata: st_local("empty_unused_mapping", st_vlmap("Empty_Status_Exact", 1))
mata: st_local("empty_missing_mapping", st_vlmap("Empty_Status_Exact", .a))
assert "`empty_unused_mapping'" == "Present but unused"
assert "`empty_missing_mapping'" == "Empty missing Å"
local ++pass_count

**# Rejected option composition and cleanup
local ++test_count
clear
set obs 1
generate byte rejected = 1
label define Rejected_Label 1 "one"
label values rejected Rejected_Label
set varabbrev on
capture noisily pq save using "`reject_file'", replace label statametadata
local reject_rc = _rc
assert `reject_rc' == 198
assert "`c(varabbrev)'" == "on"
assert !fileexists("`reject_file'")
local ++pass_count

**# Display formats, variable notes, dataset label, and dataset notes
local ++test_count
clear
set obs 3
generate double priced = 10.5 * _n
generate int counted = _n
generate str12 named = "row" + strofreal(_n)
format priced %12.4f
format counted %8.0gc
format named %-20s
label variable priced "Price"
notes priced: first note
mata: st_global("priced[note2]", "second note with " + char(34) + "quotes" + char(34) + ", a " + char(96) + "backtick" + char(39) + " and a $dollar")
mata: st_global("priced[note0]", "2")
notes named: string column note
label data `"Dataset label with "quotes" and 'apostrophes'"'
notes _dta: dataset level note one
notes _dta: dataset level note two

pq save using "`format_file'", replace statametadata
pq use using "`format_file'", clear

assert _N == 3
local got_format_priced : format priced
local got_format_counted : format counted
local got_format_named : format named
assert "`got_format_priced'" == "%12.4f"
assert "`got_format_counted'" == "%8.0gc"
assert "`got_format_named'" == "%-20s"

mata: st_local("priced_note_count", st_global("priced[note0]"))
mata: st_local("priced_note_1", st_global("priced[note1]"))
mata: st_local("priced_note_2", st_global("priced[note2]"))
mata: st_local("named_note_count", st_global("named[note0]"))
mata: st_local("named_note_1", st_global("named[note1]"))
assert "`priced_note_count'" == "2"
assert `"`priced_note_1'"' == "first note"
mata: st_local("expected_note_2", "second note with " + char(34) + "quotes" + char(34) + ", a " + char(96) + "backtick" + char(39) + " and a $dollar")
assert `"`priced_note_2'"' == `"`expected_note_2'"'
assert "`named_note_count'" == "1"
assert `"`named_note_1'"' == "string column note"

local got_data_label : data label
assert `"`got_data_label'"' == `"Dataset label with "quotes" and 'apostrophes'"'
mata: st_local("dta_note_count", st_global("_dta[note0]"))
mata: st_local("dta_note_1", st_global("_dta[note1]"))
mata: st_local("dta_note_2", st_global("_dta[note2]"))
assert "`dta_note_count'" == "2"
assert `"`dta_note_1'"' == "dataset level note one"
assert `"`dta_note_2'"' == "dataset level note two"
local ++pass_count

**# nostatametadata skips formats, notes, and the dataset label
local ++test_count
clear
pq use using "`format_file'", clear nostatametadata
assert _N == 3
local optout_format : format priced
assert "`optout_format'" != "%12.4f"
mata: st_local("optout_note_count", st_global("priced[note0]"))
assert "`optout_note_count'" == ""
local optout_data_label : data label
assert `"`optout_data_label'"' == ""
local ++pass_count

**# pq append keeps the existing dataset label and dataset notes
local ++test_count
clear
set obs 2
generate double priced = 1
generate int counted = 1
generate str12 named = "existing"
label data "EXISTING DATASET LABEL"
notes _dta: existing dataset note
pq append using "`format_file'"
assert _N == 5
local append_data_label : data label
assert `"`append_data_label'"' == "EXISTING DATASET LABEL"
mata: st_local("append_dta_note_count", st_global("_dta[note0]"))
mata: st_local("append_dta_note_1", st_global("_dta[note1]"))
assert "`append_dta_note_count'" == "1"
assert `"`append_dta_note_1'"' == "existing dataset note"
local ++pass_count

**# Storage types round trip
local ++test_count
clear
set obs 3
generate byte type_byte = _n
generate int type_int = 1000 + _n
generate long type_long = 100000 + _n
generate float type_float = 1.5 * _n
generate double type_double = 1.125 * _n
generate str12 type_str = "abc"
label variable type_byte "Trigger the capsule"

pq save using "`type_file'", replace statametadata
pq use using "`type_file'", clear

assert _N == 3
foreach v in type_byte type_int type_long type_float type_double type_str {
    local saved_type_`v' : type `v'
}
assert "`saved_type_type_byte'" == "byte"
assert "`saved_type_type_int'" == "int"
assert "`saved_type_type_long'" == "long"
assert "`saved_type_type_float'" == "float"
assert "`saved_type_type_double'" == "double"
assert "`saved_type_type_str'" == "str12"
assert type_byte == _n
assert type_int == 1000 + _n
assert type_long == 100000 + _n
assert type_str == "abc"
local ++pass_count

**# A saved type the data outgrew is refused, not forced
local ++test_count
clear
set obs 4
generate byte narrow = _n
generate byte keeper = _n
label variable narrow "Narrow column"
pq save using "`type_file'", replace statametadata

capture erase "`widened_ok'"
shell python3 "`qa_dir'/check_stata_metadata.py" widen-column ///
    "`type_file'" "`widened_file'" "narrow" "`widened_ok'"
confirm file "`widened_ok'"
erase "`widened_ok'"

pq use using "`widened_file'", clear
assert _N == 4
* The capsule declares byte; the data no longer fits, so the wider loaded
* type must be kept and every value must survive exactly.
local widened_type : type narrow
assert "`widened_type'" != "byte"
assert narrow == 1000000 + _n - 1
assert !missing(narrow)
* A column that still fits its saved type is unaffected by its neighbour.
local keeper_type : type keeper
assert "`keeper_type'" == "byte"
assert keeper == _n
* Non-type metadata is still restored on the same pass.
local widened_label : variable label narrow
assert `"`widened_label'"' == "Narrow column"
local ++pass_count

**# An explicit cast outranks the saved storage type
local ++test_count
clear
set obs 3
generate int cast_me = 100 + _n
label variable cast_me "Cast target"
pq save using "`type_file'", replace statametadata
pq use using "`type_file'", clear cast(`"{"cast_me":"float"}"')
assert _N == 3
local cast_type : type cast_me
assert "`cast_type'" != "int"
assert cast_me == 100 + _n
local cast_label : variable label cast_me
assert `"`cast_label'"' == "Cast target"
local ++pass_count

**# Format-only and type-only data still round-trips
local ++test_count
clear
set obs 3
* No variable label, no value label, no note, no dataset label anywhere.
generate byte bare_byte = _n
generate int bare_int = 500 + _n
generate double bare_priced = 1.5 * _n
format bare_priced %12.4f
pq save using "`type_file'", replace statametadata
pq use using "`type_file'", clear
assert _N == 3
local bare_byte_type : type bare_byte
local bare_int_type : type bare_int
local bare_format : format bare_priced
assert "`bare_byte_type'" == "byte"
assert "`bare_int_type'" == "int"
assert "`bare_format'" == "%12.4f"
assert bare_byte == _n
assert bare_int == 500 + _n
local ++pass_count

**# A targeted cast() exempts only its own columns
local ++test_count
clear
set obs 3
generate int cast_target = 100 + _n
generate int cast_neighbour = 200 + _n
label variable cast_target "Cast target"
pq save using "`type_file'", replace statametadata
pq use using "`type_file'", clear cast(`"{"cast_target":"float"}"')
assert _N == 3
local targeted_type : type cast_target
local neighbour_type : type cast_neighbour
* The named column keeps the caller's chosen type ...
assert "`targeted_type'" != "int"
* ... while an uncast neighbour still gets its saved type back.
assert "`neighbour_type'" == "int"
assert cast_target == 100 + _n
assert cast_neighbour == 200 + _n
local ++pass_count

**# A dataset label containing $ and backticks survives verbatim
local ++test_count
clear
set obs 2
generate byte labelled = _n
label variable labelled "Trigger"
global pq_should_not_expand SHOULD_NOT_APPEAR
mata: st_local("tricky", "keep " + char(36) + "pq_should_not_expand and " + char(96) + "tick" + char(39) + " intact")
label data `"`macval(tricky)'"'
pq save using "`type_file'", replace statametadata
pq use using "`type_file'", clear
local restored_label : data label
mata: st_local("expected_label", "keep " + char(36) + "pq_should_not_expand and " + char(96) + "tick" + char(39) + " intact")
mata: assert(st_local("restored_label") == st_local("expected_label"))
assert strpos(`"`macval(restored_label)'"', "SHOULD_NOT_APPEAR") == 0
macro drop pq_should_not_expand
local ++pass_count

**# compress also outranks the saved storage type
local ++test_count
clear
set obs 3
generate double wide_col = _n
label variable wide_col "Compress target"
pq save using "`type_file'", replace statametadata
pq use using "`type_file'", clear compress
assert _N == 3
* compress asked for the narrowest type that holds the data; restoring the
* saved double would undo exactly what the caller requested.
local compress_type : type wide_col
assert "`compress_type'" != "double"
assert wide_col == _n
local compress_label : variable label wide_col
assert `"`compress_label'"' == "Compress target"
local ++pass_count

foreach path in "`metadata_file'" "`plain_file'" "`empty_file'" "`reject_file'" ///
	"`long_file'" "`long_copy_file'" "`malformed_file'" "`merge_file'" ///
	"`format_file'" "`type_file'" "`widened_file'" {
    capture erase `path'
}
capture erase "`generic_ok'"
capture erase "`widened_ok'"
local fail_count = `test_count' - `pass_count'
display "RESULT: test_stata_metadata tests=`test_count' pass=`pass_count' fail=`fail_count'"
capture log close
exit, clear
