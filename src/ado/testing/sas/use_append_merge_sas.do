set more off
set varabbrev off

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\sas\data\data_poe\cars.sas7bdat"

capture confirm file "`sas_file'"
if _rc != 0 {
	di as error "SAS test file not found: `sas_file'"
	exit 601
}

// Load once to establish row count and build dynamic variable subsets.
pq use_sas "`sas_file'", clear
count
local n_rows = r(N)
ds
local all_vars `r(varlist)'
local n_all : word count `all_vars'

if (`n_rows' <= 0) {
	di as error "Expected positive row count from SAS file; got `n_rows'"
	exit 9
}
if (`n_all' < 2) {
	di as error "Need at least 2 variables for subset/merge checks; got `n_all'"
	exit 9
}

local master_count = cond(`n_all' >= 3, 2, 1)
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

local used_vars `master_vars' `using_vars'
local excluded_var
foreach v of local all_vars {
	local p : list posof "`v'" in used_vars
	if (`p' == 0) {
		local excluded_var `v'
		continue, break
	}
}

// 1) use + append against the same SAS source file.
pq use_sas "`sas_file'", clear
pq append "`sas_file'", format(sas)
if (_N != 2 * `n_rows') {
	di as error "Append expected `=2*`n_rows'' rows; got `=_N'"
	exit 9
}
di as text "Append test passed (_N=`=_N')"

// 2) use subset + merge in a different subset from same SAS source.
pq use_sas `master_vars' using "`sas_file'", clear
if (_N != `n_rows') {
	di as error "Subset use expected `n_rows' rows; got `=_N'"
	exit 9
}
pq merge 1:1 _n using "`sas_file'", format(sas) keepusing(`using_vars')

if (_N != `n_rows') {
	di as error "Merge expected `n_rows' rows; got `=_N'"
	exit 9
}
quietly count if _merge == 3
if (r(N) != `n_rows') {
	di as error "Merge expected all rows matched (_merge==3); got `=r(N)'"
	exit 9
}

foreach v of local master_vars {
	confirm variable `v'
}
foreach v of local using_vars {
	confirm variable `v'
}
if ("`excluded_var'" != "") {
	capture confirm variable `excluded_var'
	assert _rc != 0
}

di as result "SAS use/append/merge tests PASSED"

