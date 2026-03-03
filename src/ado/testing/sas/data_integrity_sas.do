version 16
set more off
set varabbrev off

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\sas\data\data_poe\cars.sas7bdat"

capture confirm file "`sas_file'"
if _rc != 0 {
	di as error "SAS test file not found: `sas_file'"
	exit 601
}

// Establish row/variable metadata from native import.
import sas using "`sas_file'", clear
count
local n_rows = r(N)
ds
local all_vars `r(varlist)'
local n_all : word count `all_vars'

assert `n_rows' > 0
assert `n_all' >= 2

local master_count = cond(`n_all' >= 4, 2, 1)
local using_count = cond(`n_all' - `master_count' >= 2, 2, 1)

local master_vars
forvalues i = 1/`master_count' {
	local vi : word `i' of `all_vars'
	local master_vars `master_vars' `vi'
}

local using_vars
local using_start = `master_count' + 1
local using_end = `using_start' + `using_count' - 1
forvalues i = `using_start'/`using_end' {
	local vi : word `i' of `all_vars'
	local using_vars `using_vars' `vi'
}

local n_append = floor(`n_rows' / 2)
if (`n_append' < 1) local n_append = 1
local append_start2 = `n_append' + 1
local append_end2 = `n_rows'
local n_append_expected = `n_append' + (`append_end2' - `append_start2' + 1)

local n_merge = cond(`n_rows' < 200, `n_rows', 200)

tempfile exp_use exp_append exp_merge master_dta using_dta

// Expected for use: native full import.
import sas using "`sas_file'", clear
save "`exp_use'", replace

// Expected for append: native import split + append.
import sas using "`sas_file'", clear
keep in 1/`n_append'
save "`master_dta'", replace

import sas using "`sas_file'", clear
keep in `append_start2'/`append_end2'
save "`using_dta'", replace

use "`master_dta'", clear
append using "`using_dta'"
assert _N == `n_append_expected'
save "`exp_append'", replace

// Expected for merge: native subset import with same ranges and varlists.
import sas `master_vars' using "`sas_file'", clear
keep in 1/`n_merge'
save "`master_dta'", replace

import sas `using_vars' using "`sas_file'", clear
keep in 1/`n_merge'
save "`using_dta'", replace

use "`master_dta'", clear
merge 1:1 _n using "`using_dta'"
assert _N == `n_merge'
quietly count if _merge == 3
assert r(N) == `n_merge'
save "`exp_merge'", replace

// 1) pq use_sas integrity (full file vs native import)
pq use_sas "`sas_file'", clear preserve_order
assert _N == `n_rows'
cf _all using "`exp_use'", all
di as text "SAS use integrity: PASSED"

// 2) pq append integrity (split ranges vs native append expected)
pq use_sas "`sas_file'", clear in(1/`n_append') preserve_order
pq append "`sas_file'", format(sas) in(`append_start2'/`append_end2') preserve_order
assert _N == `n_append_expected'
cf _all using "`exp_append'", all
di as text "SAS append integrity: PASSED"

// 3) pq merge integrity (subset varlists vs native merge expected)
pq use_sas `master_vars' using "`sas_file'", clear in(1/`n_merge') preserve_order
pq merge 1:1 _n using "`sas_file'", format(sas) keepusing(`using_vars') in(1/`n_merge') preserve_order
assert _N == `n_merge'
quietly count if _merge == 3
assert r(N) == `n_merge'
cf _all using "`exp_merge'", all
di as text "SAS merge integrity: PASSED"

di as result "All SAS data-integrity tests PASSED"
