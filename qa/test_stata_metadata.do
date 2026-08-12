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
local generic_ok "`qa_dir'/stata_metadata_generic.ok"
local test_count = 0
local pass_count = 0

adopath ++ "`ado_dir'"
capture program drop polars_parquet_plugin
program polars_parquet_plugin, plugin using("`plugin_file'")

foreach path in "`metadata_file'" "`plain_file'" "`empty_file'" "`reject_file'" ///
	"`long_file'" "`long_copy_file'" "`malformed_file'" "`merge_file'" {
    capture erase `path'
}
capture erase "`generic_ok'"

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

**# Merge remains outside this metadata slice
local ++test_count
clear
set obs 2
generate byte id = _n
generate byte source_code = _n
label variable source_code "Must not transfer through pq merge"
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
assert `"`merged_variable_label'"' == ""
assert "`merged_value_label'" == ""
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

foreach path in "`metadata_file'" "`plain_file'" "`empty_file'" "`reject_file'" ///
	"`long_file'" "`long_copy_file'" "`malformed_file'" "`merge_file'" {
    capture erase `path'
}
capture erase "`generic_ok'"
local fail_count = `test_count' - `pass_count'
display "RESULT: test_stata_metadata tests=`test_count' pass=`pass_count' fail=`fail_count'"
capture log close
exit, clear
