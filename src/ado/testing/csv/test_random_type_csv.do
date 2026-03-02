// CSV mirror of random type round-trip coverage.

capture program drop compare_files
program compare_files
	syntax varlist

	local var_count = 0
	foreach vari in `varlist' {
		local var_count = `var_count' + 1
		local t : type `vari'
		if (substr("`t'", 1, 3) == "str") {
			quietly count if (`vari' != `vari'_csv) | (missing(`vari'_csv) & !missing(`vari'))
		}
		else {
			quietly count if !((missing(`vari') & missing(`vari'_csv)) | (abs(`vari' - `vari'_csv) <= 1e-5 * cond(abs(`vari') > 1, abs(`vari'), 1)))
		}
		local n_disagree = r(N)
		assert `n_disagree' == 0
	}
end

clear
set seed 314159
set obs 500

gen byte   v_byte   = mod(_n, 100)
replace v_byte = . if mod(_n, 11) == 0

gen int    v_int    = floor(runiform()*30000) - 15000
replace v_int = . if mod(_n, 13) == 0

gen long   v_long   = _n * 1000
replace v_long = . if mod(_n, 17) == 0

gen float  v_float  = rnormal()
replace v_float = . if mod(_n, 19) == 0

gen double v_double = runiform() * 1e8
replace v_double = . if mod(_n, 23) == 0

gen str20  v_str20  = "row_" + string(_n)
replace v_str20 = "" if mod(_n, 7) == 0

gen strL   v_strL   = "payload_" + string(_n) + "_" + "x" * 2500
replace v_strL = "" if mod(_n, 9) == 0

tempfile roundtrip
save "`roundtrip'.dta", replace

pq save_csv "`roundtrip'.csv", replace
pq use_csv "`roundtrip'.csv", clear

unab all_vars: *
rename * *_csv
quietly merge 1:1 _n using "`roundtrip'.dta", nogen
compare_files `all_vars'

// basic CSV glob read coverage
local part1 "`roundtrip'_part1.csv"
local part2 "`roundtrip'_part2.csv"
pq use_csv "`roundtrip'.csv", clear in(1/250)
pq save_csv "`part1'", replace

pq use_csv "`roundtrip'.csv", clear in(251/500)
pq save_csv "`part2'", replace

pq use_csv "`roundtrip'_part*.csv", clear
assert _N == 500

di as result "CSV random-type round-trip tests PASSED"
