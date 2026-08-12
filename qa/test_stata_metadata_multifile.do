*! test_stata_metadata_multifile.do - multi-fragment Stata metadata contracts
*! Package: pq
*! Purpose: Writer-mode, preflight, transaction, and generic-reader QA
*! Author: Timothy P Copeland, Karolinska Institutet

version 16
set processors 1
capture log close _all
log using "test_stata_metadata_multifile.log", text replace

**# Fixture helper

capture program drop _pq_mf_fixture
program define _pq_mf_fixture, nclass
    version 16.0
    local _orig_varabbrev = c(varabbrev)
    set varabbrev off
    capture noisily {
        syntax, START(integer) N(integer) [VARLabel(string asis) CODEThree(string asis)]
        if (`n' < 0) error 198
        if (`"`varlabel'"' == "") local varlabel "Status – Ångström"
        if (`"`codethree'"' == "") local codethree "Unused mapping"

        clear
        if (`n' > 0) set obs `n'
        generate long id = `start' + _n - 1
        generate byte group = mod(id, 2)
        generate byte status = cond(mod(id, 3) == 0, -1, 1)
        generate byte status_copy = status
        label variable status `"`varlabel'"'
        label variable status_copy "Shared status – 東京"
        label define PQ_Status_Exact -1 "Negative – låg" 1 "Observed {one}" ///
            3 `"`codethree'"' .a "Extended missing Å" .z "Extended missing Ω", replace
        label values status PQ_Status_Exact
        label values status_copy PQ_Status_Exact
    }
    local rc = _rc
    set varabbrev `_orig_varabbrev'
    if `rc' exit `rc'
end

**# Setup

local qa_dir "`c(pwd)'"
local repo_dir = regexr("`qa_dir'", "/qa$", "")
local ado_dir "`repo_dir'/src/ado/p"
local plugin_file "`repo_dir'/target/release/libstata_parquet_io.so"
local checker "`qa_dir'/check_stata_metadata.py"
local work_dir "`c(tmpdir)'/pq_metadata_multifile_wave0"
local marker "`work_dir'/check.ok"
local snapshot "`work_dir'/before.json"
local test_count = 0
local pass_count = 0
local fail_count = 0
local failed_tests ""

adopath ++ "`ado_dir'"
capture program drop polars_parquet_plugin
program polars_parquet_plugin, plugin using("`plugin_file'")

shell python3 "`checker'" reset-workdir "`work_dir'"
mata: assert(direxists(st_local("work_dir")))

**# Data-only partition plus chunk regression

local ++test_count
capture noisily {
    local output "`work_dir'/plain_partition_chunk"
    _pq_mf_fixture, start(1) n(12)
    pq save using "`output'", replace partition_by(group) chunk(3)
    capture erase "`marker'"
    shell python3 "`checker'" inspect-plain-dataset "`output'" "`marker'" . 12
    confirm file "`marker'"
    pq use using "`output'", clear
    assert _N == 12
    sort id
    assert id == _n
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: partition_by()+chunk() keeps every row exactly once"
    local ++pass_count
}
else {
    display as error "  FAIL: partition_by()+chunk() data regression (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' data-partition-chunk"
}

**# Partition writer and directory/glob reader

local ++test_count
capture noisily {
    local output "`work_dir'/partitioned"
    _pq_mf_fixture, start(1) n(12)
    pq save using "`output'", replace partition_by(group) statametadata
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`output'" "`marker'" 2 12
    confirm file "`marker'"

    pq use using "`output'", clear
    assert _N == 12
    local restored_label : variable label status
    local restored_vl : value label status
    assert `"`restored_label'"' == "Status – Ångström"
    assert "`restored_vl'" == "PQ_Status_Exact"
    mata: assert(st_vlmap("PQ_Status_Exact", 3) == "Unused mapping")

    pq use using "`output'/**/*.parquet", clear
    assert _N == 12
    local glob_vl : value label status
    assert "`glob_vl'" == "PQ_Status_Exact"

    clear
    set obs 1
    generate long id = 0
    generate byte group = 0
    generate byte status = 1
    generate byte status_copy = 1
    label define PQ_Append_Master 1 "Master definition", replace
    label values status PQ_Append_Master
    label values status_copy PQ_Append_Master
    pq append using "`output'"
    sort id
    assert _N == 13
    assert id == _n - 1
    local append_vl : value label status
    assert "`append_vl'" == "PQ_Append_Master"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: partition fragments and directory/glob restoration"
    local ++pass_count
}
else {
    display as error "  FAIL: partition fragments and directory/glob restoration (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' partition-metadata"
}

**# Chunk writer and glob reader

local ++test_count
capture noisily {
    local output "`work_dir'/chunked"
    _pq_mf_fixture, start(1) n(12)
    pq save using "`output'", replace chunk(3) statametadata
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`output'" "`marker'" 4 12
    confirm file "`marker'"
    pq use using "`output'/*.parquet", clear
    sort id
    assert _N == 12
    assert id == _n
    local restored_vl : value label status_copy
    assert "`restored_vl'" == "PQ_Status_Exact"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: chunk fragments carry and restore one envelope"
    local ++pass_count
}
else {
    display as error "  FAIL: chunk fragments carry and restore one envelope (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' chunk-metadata"
}

**# Stream reload and do_not_reload state

local ++test_count
capture noisily {
    local reload_output "`work_dir'/stream_reload"
    local clear_output "`work_dir'/stream_clear"
    _pq_mf_fixture, start(1) n(12)
    pq save using "`reload_output'", replace chunk(3) stream statametadata
    assert _N == 12
    assert id == _n
    local source_vl : value label status
    assert "`source_vl'" == "PQ_Status_Exact"
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`reload_output'" "`marker'" 4 12
    confirm file "`marker'"

    _pq_mf_fixture, start(1) n(12)
    pq save using "`clear_output'", replace chunk(3) stream do_not_reload statametadata
    assert _N == 0
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`clear_output'" "`marker'" 4 12
    confirm file "`marker'"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: stream state contract with and without reload"
    local ++pass_count
}
else {
    display as error "  FAIL: stream state contract (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' stream-state"
}

**# Consolidated output

local ++test_count
capture noisily {
    local output "`work_dir'/consolidated.parquet"
    _pq_mf_fixture, start(1) n(12)
    pq save using "`output'", replace chunk(3) consolidate statametadata
    assert fileexists("`output'")
    mata: assert(!direxists(st_local("output")))
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`output'" "`marker'" 1 12
    confirm file "`marker'"
    pq use using "`output'", clear
    assert _N == 12
    local restored_vl : value label status
    assert "`restored_vl'" == "PQ_Status_Exact"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: consolidation carries and restores metadata"
    local ++pass_count
}
else {
    display as error "  FAIL: consolidation carries and restores metadata (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' consolidation"
}

**# Combined partition and chunk metadata

local ++test_count
capture noisily {
    local output "`work_dir'/partition_chunk_metadata"
    _pq_mf_fixture, start(1) n(12)
    pq save using "`output'", replace partition_by(group) chunk(3) statametadata
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`output'" "`marker'" . 12
    confirm file "`marker'"
    pq use using "`output'", clear
    sort id
    assert _N == 12
    assert id == _n
    local restored_vl : value label status
    assert "`restored_vl'" == "PQ_Status_Exact"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: partition plus chunk metadata"
    local ++pass_count
}
else {
    display as error "  FAIL: partition plus chunk metadata (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' partition-chunk-metadata"
}

**# Multi-file selection and data-path transformations

local ++test_count
capture noisily {
    local output "`work_dir'/read_modes"
    _pq_mf_fixture, start(1) n(20)
    pq save using "`output'", replace partition_by(group) statametadata

    pq use status using "`output'", clear
    assert _N == 20
    confirm variable status, exact
    capture confirm variable status_copy, exact
    assert _rc == 111
    local projected_vl : value label status
    assert "`projected_vl'" == "PQ_Status_Exact"

    pq use using "`output'", clear drop(status_copy)
    assert _N == 20
    capture confirm variable status_copy, exact
    assert _rc == 111
    local dropped_vl : value label status
    assert "`dropped_vl'" == "PQ_Status_Exact"

    pq use using "`output'", clear if(id >= 8)
    assert _N == 13
    assert id >= 8
    local filtered_vl : value label status
    assert "`filtered_vl'" == "PQ_Status_Exact"

    pq use using "`output'", clear in(3/7)
    assert _N == 5
    local ranged_vl : value label status
    assert "`ranged_vl'" == "PQ_Status_Exact"

    pq use using "`output'", clear random_n(5) random_seed(8675309)
    assert _N == 5
    local sampled_vl : value label status
    assert "`sampled_vl'" == "PQ_Status_Exact"

    pq use using "`output'", clear cast(`"{"id":"string"}"')
    confirm string variable id
    local cast_vl : value label status
    assert "`cast_vl'" == "PQ_Status_Exact"

    pq use using "`output'", clear compress
    assert _N == 20
    local compressed_vl : value label status
    assert "`compressed_vl'" == "PQ_Status_Exact"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: multi-file selection/filter/sample/cast/compress restoration"
    local ++pass_count
}
else {
    display as error "  FAIL: multi-file read transformations (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' read-transformations"
}

**# Compatible relaxed schemas keep one metadata identity

local ++test_count
capture noisily {
    local first_file "`work_dir'/relaxed_int.parquet"
    local second_file "`work_dir'/relaxed_long.parquet"
    local relaxed_dir "`work_dir'/relaxed_dataset"
    _pq_mf_fixture, start(1) n(4)
    recast int id
    pq save using "`first_file'", replace statametadata
    _pq_mf_fixture, start(5) n(4)
    recast long id
    pq save using "`second_file'", replace statametadata
    capture erase "`marker'"
    shell python3 "`checker'" assemble "`relaxed_dir'" "`marker'" ///
        "`first_file'" "`second_file'"
    confirm file "`marker'"

    pq use using "`relaxed_dir'", clear relaxed
    assert _N == 8
    sort id
    assert id == _n
    local relaxed_vl : value label status
    assert "`relaxed_vl'" == "PQ_Status_Exact"
    mata: assert(st_vlmap("PQ_Status_Exact", 3) == "Unused mapping")
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: relaxed schemas retain compatible metadata"
    local ++pass_count
}
else {
    display as error "  FAIL: compatible relaxed metadata (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' relaxed-compatible"
}

**# Subset writer and long physical-name multi-file mapping

local ++test_count
capture noisily {
    local subset_output "`work_dir'/subset_chunked"
    local long_output "`work_dir'/long_partitioned"
    local long_physical_name "status_with_a_name_far_beyond_thirty_two_characters"

    _pq_mf_fixture, start(1) n(8)
    pq save id status using "`subset_output'", replace chunk(2) statametadata
    pq use using "`subset_output'", clear
    assert _N == 8
    confirm variable id, exact
    confirm variable status, exact
    capture confirm variable status_copy, exact
    assert _rc == 111
    local subset_vl : value label status
    assert "`subset_vl'" == "PQ_Status_Exact"

    _pq_mf_fixture, start(1) n(8)
    char status[_pq_parquet_name] "`long_physical_name'"
    pq save using "`long_output'", replace partition_by(group) statametadata
    pq use using "`long_output'", clear
    local found_long = 0
    unab loaded_vars : _all
    foreach variable of local loaded_vars {
        local physical_name : char `variable'[_pq_parquet_name]
        if ("`physical_name'" == "`long_physical_name'") {
            local found_long = 1
            local long_label : variable label `variable'
            local long_vl : value label `variable'
            assert `"`long_label'"' == "Status – Ångström"
            assert "`long_vl'" == "PQ_Status_Exact"
        }
    }
    assert `found_long' == 1
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: subset writer and long physical-name multi-file map"
    local ++pass_count
}
else {
    display as error "  FAIL: subset/long-name metadata mapping (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' subset-long-name"
}

**# Remaining writer-mode and edge-state matrix

local ++test_count
capture noisily {
    local stream_consolidated "`work_dir'/stream_consolidated.parquet"
    local partition_stream "`work_dir'/partition_chunk_stream"
    local small_output "`work_dir'/small_chunk_branch.parquet"
    local unlabeled_output "`work_dir'/zero_labeled_variables"

    _pq_mf_fixture, start(1) n(10)
    pq save using "`stream_consolidated'", replace chunk(3) stream ///
        consolidate statametadata
    assert _N == 10
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`stream_consolidated'" ///
        "`marker'" 1 10
    confirm file "`marker'"
    pq use using "`stream_consolidated'", clear
    local stream_consolidated_vl : value label status
    assert "`stream_consolidated_vl'" == "PQ_Status_Exact"

    _pq_mf_fixture, start(1) n(10)
    pq save using "`partition_stream'", replace partition_by(group) chunk(3) ///
        stream statametadata
    assert _N == 10
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`partition_stream'" ///
        "`marker'" . 10
    confirm file "`marker'"
    pq use using "`partition_stream'", clear
    sort id
    assert _N == 10 & id == _n
    local partition_stream_vl : value label status_copy
    assert "`partition_stream_vl'" == "PQ_Status_Exact"

    _pq_mf_fixture, start(1) n(3)
    pq save using "`small_output'", replace chunk(100) stream consolidate ///
        statametadata
    assert fileexists("`small_output'")
    assert _N == 3
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`small_output'" "`marker'" 1 3
    confirm file "`marker'"

    clear
    set obs 5
    generate long id = _n
    generate byte group = mod(id, 2)
    pq save using "`unlabeled_output'", replace partition_by(group) ///
        statametadata
    capture erase "`marker'"
    shell python3 "`checker'" inspect-plain-dataset "`unlabeled_output'" ///
        "`marker'" . 5
    confirm file "`marker'"
    pq use using "`unlabeled_output'", clear
    assert _N == 5
    sort id
    assert id == _n
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: stream/consolidate, partition-stream, small, and unlabeled modes"
    local ++pass_count
}
else {
    display as error "  FAIL: remaining writer-mode/state matrix (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' writer-mode-matrix"
}

**# Compatible and conflicting partition extension preflight

local ++test_count
capture noisily {
    local output "`work_dir'/partition_extend"
    _pq_mf_fixture, start(1) n(6)
    pq save using "`output'", replace partition_by(group) statametadata
    _pq_mf_fixture, start(7) n(6)
    pq save using "`output'", replace partition_by(group) nopartitionoverwrite statametadata
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`output'" "`marker'" 4 12
    confirm file "`marker'"

    shell python3 "`checker'" snapshot "`output'" "`snapshot'"
    confirm file "`snapshot'"
    _pq_mf_fixture, start(13) n(2) varlabel("Conflicting definition")
    capture noisily pq save using "`output'", replace partition_by(group) ///
        nopartitionoverwrite statametadata
    local conflict_rc = _rc
    assert `conflict_rc' == 198
    capture erase "`marker'"
    shell python3 "`checker'" compare-snapshot "`output'" "`snapshot'" "`marker'"
    confirm file "`marker'"

    local plain_output "`work_dir'/plain_partition_chunk"
    shell python3 "`checker'" snapshot "`plain_output'" "`snapshot'"
    confirm file "`snapshot'"
    _pq_mf_fixture, start(20) n(2)
    capture noisily pq save using "`plain_output'", replace partition_by(group) ///
        nopartitionoverwrite statametadata
    local plain_rc = _rc
    assert `plain_rc' == 198
    capture erase "`marker'"
    shell python3 "`checker'" compare-snapshot "`plain_output'" "`snapshot'" "`marker'"
    confirm file "`marker'"
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: partition extension preflight is transactional"
    local ++pass_count
}
else {
    display as error "  FAIL: partition extension preflight (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' partition-extension"
}

**# Mixed footer rejection before data mutation and explicit opt-out

local ++test_count
capture noisily {
    local metadata_file "`work_dir'/mixed_metadata.parquet"
    local plain_file "`work_dir'/mixed_plain.parquet"
    local mixed_dir "`work_dir'/mixed_dataset"
    _pq_mf_fixture, start(1) n(4)
    pq save using "`metadata_file'", replace statametadata
    pq save using "`plain_file'", replace
    capture erase "`marker'"
    shell python3 "`checker'" assemble "`mixed_dir'" "`marker'" ///
        "`metadata_file'" "`plain_file'"
    confirm file "`marker'"
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`metadata_file'" "`marker'" 1 4
    confirm file "`marker'"
    capture erase "`marker'"
    shell python3 "`checker'" inspect-plain-dataset "`plain_file'" "`marker'" 1 4
    confirm file "`marker'"

    shell python3 "`checker'" snapshot "`mixed_dir'" "`snapshot'"
    confirm file "`snapshot'"
    _pq_mf_fixture, start(20) n(2)
    capture noisily pq save using "`mixed_dir'", replace partition_by(group) ///
        nopartitionoverwrite statametadata
    local mixed_write_rc = _rc
    assert `mixed_write_rc' == 198
    capture erase "`marker'"
    shell python3 "`checker'" compare-snapshot "`mixed_dir'" "`snapshot'" "`marker'"
    confirm file "`marker'"

    clear
    set obs 1
    generate byte sentinel = 42
    set varabbrev on
    capture noisily pq use using "`mixed_dir'", clear
    local mixed_rc = _rc
    assert `mixed_rc' == 198
    assert "`c(varabbrev)'" == "on"
    confirm variable sentinel, exact
    assert _N == 1 & sentinel[1] == 42

    capture noisily pq append using "`mixed_dir'"
    local mixed_append_rc = _rc
    assert `mixed_append_rc' == 198
    confirm variable sentinel, exact
    assert _N == 1 & sentinel[1] == 42

    pq use using "`mixed_dir'", clear nostatametadata
    assert _N == 8
    local mixed_vl : value label status
    assert "`mixed_vl'" == ""
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: mixed footer preflight and nostatametadata escape hatch"
    local ++pass_count
}
else {
    display as error "  FAIL: mixed footer preflight and opt-out (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' mixed-footer"
}

**# Semantically conflicting footer rejection

local ++test_count
capture noisily {
    local first_file "`work_dir'/conflict_first.parquet"
    local second_file "`work_dir'/conflict_second.parquet"
    local conflict_dir "`work_dir'/conflict_dataset"
    _pq_mf_fixture, start(1) n(4)
    pq save using "`first_file'", replace statametadata
    _pq_mf_fixture, start(5) n(4) codethree("Changed unused mapping")
    pq save using "`second_file'", replace statametadata
    capture erase "`marker'"
    shell python3 "`checker'" assemble "`conflict_dir'" "`marker'" ///
        "`first_file'" "`second_file'"
    confirm file "`marker'"
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`first_file'" "`marker'" 1 4
    confirm file "`marker'"
    capture erase "`marker'"
    shell python3 "`checker'" inspect-dataset "`second_file'" "`marker'" 1 4 5
    confirm file "`marker'"
    capture erase "`marker'"
    shell python3 "`checker'" compare-envelopes "`first_file'" "`second_file'" ///
        different "`marker'"
    confirm file "`marker'"

    clear
    set obs 1
    generate byte sentinel = 43
    capture noisily pq use using "`conflict_dir'", clear relaxed
    local conflict_rc = _rc
    assert `conflict_rc' == 198
    confirm variable sentinel, exact
    assert _N == 1 & sentinel[1] == 43
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: relaxed read still rejects changed unused mappings"
    local ++pass_count
}
else {
    display as error "  FAIL: conflicting footer rejection (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' conflicting-footer"
}

**# Invalid combinations and empty partition policy do not mutate output

local ++test_count
capture noisily {
    local invalid_output "`work_dir'/invalid_partition_consolidate.parquet"
    local empty_output "`work_dir'/empty_partition"
    _pq_mf_fixture, start(1) n(4)
    pq save using "`invalid_output'", replace statametadata
    shell python3 "`checker'" snapshot "`invalid_output'" "`snapshot'"
    confirm file "`snapshot'"
    capture noisily pq save using "`invalid_output'", replace partition_by(group) ///
        chunk(2) consolidate statametadata
    local invalid_rc = _rc
    assert `invalid_rc' == 198
    capture erase "`marker'"
    shell python3 "`checker'" compare-snapshot "`invalid_output'" "`snapshot'" "`marker'"
    confirm file "`marker'"

    _pq_mf_fixture, start(1) n(0)
    capture noisily pq save using "`empty_output'", replace partition_by(group) statametadata
    local empty_rc = _rc
    assert `empty_rc' == 198
    assert !fileexists("`empty_output'")
    mata: assert(!direxists(st_local("empty_output")))

    local empty_input "`work_dir'/empty_input"
    capture mkdir "`empty_input'"
    clear
    set obs 1
    generate byte sentinel = 44
    capture noisily pq use using "`empty_input'", clear
    local empty_input_rc = _rc
    assert `empty_input_rc' == 198
    confirm variable sentinel, exact
    assert _N == 1 & sentinel[1] == 44
    capture noisily pq use using "`work_dir'/no_match_*.parquet", clear
    local empty_glob_rc = _rc
    assert `empty_glob_rc' == 198
    confirm variable sentinel, exact
    assert _N == 1 & sentinel[1] == 44
}
local _test_rc = _rc
if (`_test_rc' == 0) {
    display as result "  PASS: invalid combination and empty partition are no-mutation errors"
    local ++pass_count
}
else {
    display as error "  FAIL: invalid/empty no-mutation contract (error `_test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' invalid-empty"
}

**# Summary and cleanup

shell python3 "`checker'" remove-workdir "`work_dir'"
local fail_count = `test_count' - `pass_count'
display "RESULT: test_stata_metadata_multifile tests=`test_count' pass=`pass_count' fail=`fail_count'"
capture log close
if (`fail_count' > 0) {
    display as error "FAILED TESTS:`failed_tests'"
    exit 1
}
exit, clear
