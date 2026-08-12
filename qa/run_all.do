*! run_all.do - curated QA runner for pq
*! Author: Timothy P Copeland, Karolinska Institutet

version 16
capture log close _all
local runner_log "run_all.log"
log using "`runner_log'", text replace

args mode extra
if (`"`extra'"' != "") {
    display as error "run_all.do accepts one lane: quick, core, or full"
    capture log close
    exit 198
}
if ("`mode'" == "") local mode "full"
if !inlist("`mode'", "quick", "core", "full") {
    display as error "Unknown QA lane: `mode'"
    capture log close
    exit 198
}

local suites "test_regressions.do"
if inlist("`mode'", "core", "full") {
    local suites "`suites' test_stata_metadata.do test_stata_metadata_multifile.do test_package_release.do"
}

local test_count = 0
local pass_count = 0
local fail_count = 0
local failed_suites ""

foreach suite of local suites {
    local ++test_count
    display as text "Running `suite'"
    capture noisily do "`suite'"
    local suite_rc = _rc
    capture log close _all
    log using "`runner_log'", text append
    if (`suite_rc' == 0) {
        display as result "  PASS: `suite'"
        local ++pass_count
    }
    else {
        display as error "  FAIL: `suite' (error `suite_rc')"
        local ++fail_count
        local failed_suites "`failed_suites' `suite'"
    }
}

display "RESULT: run_all tests=`test_count' pass=`pass_count' fail=`fail_count'"
capture log close
if (`fail_count' > 0) {
    display as error "FAILED SUITES:`failed_suites'"
    exit 1
}
exit, clear
