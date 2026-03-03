version 16
set more off
set varabbrev off

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat\crates\polars_readstat_rs\tests\sas\data\too_big\hhpub25.sas7bdat"
local n_check = 10000

capture confirm file "`sas_file'"
if _rc != 0 {
	di as error "SAS test file not found: `sas_file'"
	exit 601
}

tempfile first_read

pq use_sas using "`sas_file'", clear in(1/`n_check') preserve_order batch_size(64)
assert _N == `n_check'

ds
local all_vars `r(varlist)'
local c1 : word 1 of `all_vars'
local c2 : word 2 of `all_vars'
local c3 : word 3 of `all_vars'
assert "`c1'" != ""
assert "`c2'" != ""
assert "`c3'" != ""

gen long __rowid = _n
keep __rowid `c1' `c2' `c3'
save "`first_read'", replace

pq use_sas using "`sas_file'", clear in(1/`n_check') preserve_order batch_size(64)
assert _N == `n_check'
gen long __rowid = _n
keep __rowid `c1' `c2' `c3'

cf _all using "`first_read'", all

di as result "preserve_order hhpub25 deterministic read check: PASSED"
