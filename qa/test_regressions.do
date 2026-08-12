*! test_regressions.do - regressions for Stata metadata review findings
*! Package: pq
*! Purpose: Adversarial round trips for metadata-only and strL paths
*! Author: Timothy P Copeland, Karolinska Institutet

version 16
clear all
set varabbrev off
capture log close _all
log using "test_regressions.log", text replace

**# Setup

local test_count = 0
local pass_count = 0
local fail_count = 0
local failed_tests ""

local qa_dir "`c(pwd)'"
local repo_dir = regexr("`qa_dir'", "/qa$", "")
local ado_dir "`repo_dir'/src/ado/p"
local plugin_file "`repo_dir'/target/release/libstata_parquet_io.so"
confirm file "`plugin_file'"

adopath ++ "`ado_dir'"
capture program drop polars_parquet_plugin
program polars_parquet_plugin, plugin using("`plugin_file'")

tempfile variable_label_base drop_strl_base long_name_base extensionless_file
local variable_label_file "`variable_label_base'.parquet"
local drop_strl_file "`drop_strl_base'.parquet"
local long_name_file "`long_name_base'.parquet"

**# Metadata regressions

**## Extensionless single-file round trip
local ++test_count
capture noisily {
    clear
    set obs 3
    generate byte id = _n

    pq save using "`extensionless_file'", replace
    capture noisily pq use using "`extensionless_file'", clear
    local default_rc = _rc
    capture noisily pq use using "`extensionless_file'", clear nostatametadata
    local optout_rc = _rc

    assert `default_rc' == 0
    assert `optout_rc' == 0
    assert _N == 3
    assert id == _n
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: extensionless single-file round trip"
    local ++pass_count
}
else {
    display as error "  FAIL: extensionless single-file round trip (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' extensionless_file"
}

**## Variable label without a value label
local ++test_count
capture noisily {
    clear
    set obs 2
    generate byte id = _n
    label variable id "Identifier only – no value label"
    set varabbrev on

    pq save using "`variable_label_file'", replace statametadata
    assert "`c(varabbrev)'" == "on"
    pq use using "`variable_label_file'", clear

    assert id == _n
    assert "`c(varabbrev)'" == "on"
    local restored_label : variable label id
    local restored_value_label : value label id
    assert `"`restored_label'"' == "Identifier only – no value label"
    assert "`restored_value_label'" == ""
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: variable-label-only metadata round trip"
    local ++pass_count
}
else {
    display as error "  FAIL: variable-label-only metadata round trip (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' variable_label_only"
}

**## drop_strl removes metadata targets as well as data columns
local ++test_count
capture noisily {
    clear
    set obs 2
    generate byte id = _n
    generate strL payload = "x"
    forvalues i = 1/12 {
        quietly replace payload = payload + payload
    }
    label variable payload "Deliberately dropped long payload"

    pq save using "`drop_strl_file'", replace statametadata
    pq use using "`drop_strl_file'", clear drop_strl

    assert _N == 2
    assert id == _n
    capture confirm variable payload, exact
    assert _rc == 111
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: drop_strl excludes stale metadata targets"
    local ++pass_count
}
else {
    display as error "  FAIL: drop_strl excludes stale metadata targets (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' drop_strl_metadata"
}

**## Long physical Parquet name on a strL column
local ++test_count
capture noisily {
    clear
    set obs 2
    generate strL payload = "y"
    forvalues i = 1/12 {
        quietly replace payload = payload + payload
    }
    local physical_name "payload_with_a_physical_name_longer_than_thirty_two_characters"
    char payload[_pq_parquet_name] "`physical_name'"
    label variable payload "Long-name strL payload"

    pq save using "`long_name_file'", replace statametadata
    pq use using "`long_name_file'", clear

    unab loaded_var : _all
    local loaded_count : word count `loaded_var'
    assert `loaded_count' == 1
    local restored_name : char `loaded_var'[_pq_parquet_name]
    local restored_label : variable label `loaded_var'
    assert "`restored_name'" == "`physical_name'"
    assert `"`restored_label'"' == "Long-name strL payload"
    assert strlen(`loaded_var') == 4096
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: long physical name survives the strL load path"
    local ++pass_count
}
else {
    display as error "  FAIL: long physical name survives the strL load path (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' long_name_strl"
}

**# Cleanup and summary

foreach path in "`variable_label_file'" "`drop_strl_file'" "`long_name_file'" {
    capture erase "`path'"
}

display as result "Test Results: `pass_count'/`test_count' passed, `fail_count' failed"
display "RESULT: test_regressions tests=`test_count' pass=`pass_count' fail=`fail_count'"
capture log close
if (`fail_count' > 0) {
    display as error "FAILED TESTS:`failed_tests'"
    exit 1
}
exit, clear
