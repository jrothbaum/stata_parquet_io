*! test_package_release.do - package, documentation, and source contracts
*! Package: pq
*! Purpose: Release-surface regressions for reviewed metadata changes
*! Author: Timothy P Copeland, Karolinska Institutet

version 16
clear all
set varabbrev off
capture log close _all
log using "test_package_release.log", text replace

**# Render oracle

capture program drop _qa_sthlp_render
program define _qa_sthlp_render, rclass
    version 16.0
    local _orig_varabbrev = c(varabbrev)
    set varabbrev off
    local result_nbad = .
    local result_badfiles ""
    capture noisily {
        syntax anything(name=files id="help files")
        local files = subinstr(`"`files'"', char(34), "", .)
        local nbad = 0
        local badfiles ""

        foreach f of local files {
            capture confirm file "`f'"
            if (_rc) {
                display as error "  render: file not found: `f'"
                local ++nbad
                local badfiles "`badfiles' `f'"
                continue
            }

            tempfile rlog
            capture log off
            log using "`rlog'", replace text name(_qarender)
            type "`f'", smcl
            log close _qarender
            capture log on

            local hits = 0
            local nlines = 0
            tempname fh
            file open `fh' using "`rlog'", read text
            file read `fh' line
            while (r(eof) == 0) {
                local ++nlines
                if regexm(`"`line'"', "\{(pstd|phang|pmore|pin|p_end|psee|synopt|p2col|cmd:|it:|bf:|opt |opth |helpb |hline|title:|marker |dlgtab:|break)") {
                    local shown = subinstr(`"`line'"', "{", char(1), .)
                    local shown = subinstr(`"`shown'"', "}", char(2), .)
                    local shown = subinstr(`"`shown'"', char(1), "{c -(}", .)
                    local shown = subinstr(`"`shown'"', char(2), "{c )-}", .)
                    display as error "  literal SMCL: `shown'"
                    local ++hits
                }
                file read `fh' line
            }
            file close `fh'

            if (`nlines' == 0) {
                display as error "  render produced no output for `f' -- FAILING"
                local ++nbad
                local badfiles "`badfiles' `f'"
                continue
            }
            if (`hits' > 0) {
                local ++nbad
                local badfiles "`badfiles' `f'"
            }
        }
        local result_nbad = `nbad'
        local result_badfiles "`badfiles'"
    }
    local rc = _rc
    set varabbrev `_orig_varabbrev'
    if (`rc') exit `rc'
    return scalar nbad = `result_nbad'
    return local badfiles "`result_badfiles'"
end

**# Setup

local test_count = 0
local pass_count = 0
local fail_count = 0
local failed_tests ""

local qa_dir "`c(pwd)'"
local repo_dir = regexr("`qa_dir'", "/qa$", "")
local help_file "`repo_dir'/src/ado/p/pq.sthlp"
local ado_file "`repo_dir'/src/ado/p/pq.ado"
local pkg_file "`repo_dir'/src/ado/p/pq.pkg"
local cargo_file "`repo_dir'/Cargo.toml"
local readme_file "`repo_dir'/README.md"
local decisions_file "`repo_dir'/docs/stata_metadata_decisions.md"
local rust_file "`repo_dir'/src/lib.rs"

**# Documentation contracts

**## binary_to_string warning belongs to binary_to_string
local ++test_count
capture noisily {
    tempname help_fh
    local line_number = 0
    local binary_line = 0
    local warning_line = 0
    local nostata_line = 0
    file open `help_fh' using "`help_file'", read text
    file read `help_fh' line
    while (r(eof) == 0) {
        local ++line_number
        if strpos(`"`line'"', "{opt binary_to_string} decodes binary columns") {
            local binary_line = `line_number'
        }
        if strpos(`"`line'"', "Without this option, binary columns") {
            local warning_line = `line_number'
        }
        if strpos(`"`line'"', "{opt nostatametadata} suppresses") {
            local nostata_line = `line_number'
        }
        file read `help_fh' line
    }
    file close `help_fh'
    assert `binary_line' > 0
    assert `warning_line' > `binary_line'
    assert `nostata_line' > `warning_line'
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: binary warning is attached to binary_to_string"
    local ++pass_count
}
else {
    display as error "  FAIL: binary warning is attached to binary_to_string (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' help_option_association"
}

**## Help file renders and the oracle fails its positive control
local ++test_count
capture noisily {
    _qa_sthlp_render `help_file'
    assert r(nbad) == 0

    tempfile broken_help
    tempname broken_fh
    file open `broken_fh' using "`broken_help'", write replace text
    file write `broken_fh' "{smcl}" _n
    file write `broken_fh' "{title:Render probe}" _n _n
    file write `broken_fh' "{pstd}" _n
    file write `broken_fh' "A directive split across a source newline: {bf:broken" _n
    file write `broken_fh' "directive} renders as literal markup." _n
    file close `broken_fh'
    _qa_sthlp_render `broken_help'
    assert r(nbad) == 1
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: help render oracle is live and pq.sthlp is clean"
    local ++pass_count
}
else {
    display as error "  FAIL: help render oracle (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' help_render"
}

**## Version/date and supported-mode documentation are synchronized
local ++test_count
capture noisily {
    local ado_version = 0
    local help_version_top = 0
    local help_version_bottom = 0
    local cargo_version = 0
    local pkg_date = 0
    local stale_contract = 0
    foreach path in "`ado_file'" "`help_file'" "`cargo_file'" "`pkg_file'" ///
        "`readme_file'" "`decisions_file'" {
        tempname fh
        file open `fh' using "`path'", read text
        file read `fh' line
        while (r(eof) == 0) {
            mata: st_local("ado_version", strpos(st_local("line"), "pq Version 3.0.10 12aug2026") ? "1" : st_local("ado_version"))
            mata: st_local("help_version_top", strpos(st_local("line"), "version 3.0.10 August 2026") ? "1" : st_local("help_version_top"))
            mata: st_local("help_version_bottom", strpos(st_local("line"), "package. Version 3.0.10") ? "1" : st_local("help_version_bottom"))
            mata: st_local("cargo_version", strpos(st_local("line"), "version = " + char(34) + "3.0.10" + char(34)) ? "1" : st_local("cargo_version"))
            mata: st_local("pkg_date", strpos(st_local("line"), "Distribution-Date: 20260812") ? "1" : st_local("pkg_date"))
            mata: st_local("stale_contract", strpos(st_local("line"), "single-file only") ? "1" : st_local("stale_contract"))
            mata: st_local("stale_contract", strpos(st_local("line"), "intentionally deferred") ? "1" : st_local("stale_contract"))
            mata: st_local("stale_contract", strpos(st_local("line"), "currently limited to one physical Parquet file") ? "1" : st_local("stale_contract"))
            mata: st_local("stale_contract", strpos(st_local("line"), "does not support partitioned") ? "1" : st_local("stale_contract"))
            file read `fh' line
        }
        file close `fh'
    }
    assert `ado_version' == 1
    assert `help_version_top' == 1
    assert `help_version_bottom' == 1
    assert `cargo_version' == 1
    assert `pkg_date' == 1
    assert `stale_contract' == 0
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: version/date and metadata support documentation are synchronized"
    local ++pass_count
}
else {
    display as error "  FAIL: version/date or metadata documentation sync (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' version_docs_sync"
}

**## Package manifest lists only shipped Stata files and every plugin target
local ++test_count
capture noisily {
    local pkg_file_count = 0
    local plugin_target_count = 0
    local plugin_targets ""
    tempname pkg_fh
    file open `pkg_fh' using "`pkg_file'", read text
    file read `pkg_fh' line
    while (r(eof) == 0) {
        if regexm(`"`line'"', "^f ([^ ]+)$") {
            local ++pkg_file_count
            local artifact = regexs(1)
            confirm file "`repo_dir'/src/ado/p/`artifact'"
        }
        if regexm(`"`line'"', "^g (LINUX64|MACINTEL64|MACARM64|WIN64) ") {
            local ++plugin_target_count
            local plugin_targets "`plugin_targets' `=regexs(1)'"
        }
        file read `pkg_fh' line
    }
    file close `pkg_fh'
    assert `pkg_file_count' == 2
    assert `plugin_target_count' == 4
    foreach target in LINUX64 MACINTEL64 MACARM64 WIN64 {
        local found : list target in plugin_targets
        assert `found' == 1
    }
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: package manifest covers shipped files and plugin targets"
    local ++pass_count
}
else {
    display as error "  FAIL: package manifest artifact contract (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' package_manifest"
}

**# QA structure

**## Curated runner and contributor runbook exist
local ++test_count
capture noisily {
    confirm file "`qa_dir'/run_all.do"
    confirm file "`qa_dir'/README.md"
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: QA runner and runbook are present"
    local ++pass_count
}
else {
    display as error "  FAIL: QA runner and runbook are present (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' qa_structure"
}

**# Rust source contract

**## New return code does not introduce an unnecessary cast
local ++test_count
capture noisily {
    tempname rust_fh
    local unnecessary_cast = 0
    file open `rust_fh' using "`rust_file'", read text
    file read `rust_fh' line
    while (r(eof) == 0) {
        if strpos(`"`line'"', "198 as i32") local unnecessary_cast = 1
        file read `rust_fh' line
    }
    file close `rust_fh'
    assert `unnecessary_cast' == 0
}
local test_rc = _rc
if (`test_rc' == 0) {
    display as result "  PASS: metadata write error code has no unnecessary cast"
    local ++pass_count
}
else {
    display as error "  FAIL: metadata write error code has no unnecessary cast (error `test_rc')"
    local ++fail_count
    local failed_tests "`failed_tests' rust_unnecessary_cast"
}

**# Summary

display as result "Test Results: `pass_count'/`test_count' passed, `fail_count' failed"
display "RESULT: test_package_release tests=`test_count' pass=`pass_count' fail=`fail_count'"
capture log close
if (`fail_count' > 0) {
    display as error "FAILED TESTS:`failed_tests'"
    exit 1
}
exit, clear
