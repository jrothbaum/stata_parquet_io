set varabbrev off
set more off

// Build master data
clear
set seed 20260305
set obs 50
gen long id = _n
gen double x = runiform()

tempfile master f_csv f_spss f_missing_sas
save "`master'", replace

// Build using data and save to CSV/SPSS
clear
set obs 50
gen long id = _n
gen double z = _n * 2
gen str10 dt_str = string(mod(_n, 28) + 1, "%02.0f") + "jan2020"

pq save_csv using "`f_csv'.csv", replace
pq save_spss using "`f_spss'.sav", replace

// merge_csv wrapper should accept CSV-specific read options
use "`master'", clear
pq merge_csv 1:1 id using "`f_csv'.csv", infer_schema_length(0) parse_dates
assert _N == 50
confirm variable z
capture confirm variable _merge
assert _rc == 0
drop _merge z dt_str

// merge_spss wrapper should pass through SPSS read options
use "`master'", clear
pq merge_spss 1:1 id using "`f_spss'.sav", preserve_order
assert _N == 50
confirm variable z
capture confirm variable _merge
assert _rc == 0
drop _merge z dt_str

// merge_sas wrapper should dispatch to pq merge with format(sas)
// (smoke test via expected file-not-found error on missing file)
use "`master'", clear
capture noisily pq merge_sas 1:1 id using "`f_missing_sas'.sas7bdat", preserve_order
assert _rc == 601

capture erase "`f_csv'.csv"
capture erase "`f_spss'.sav"

di as result "merge format wrapper tests passed"
