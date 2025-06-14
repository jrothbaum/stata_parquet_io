
timer clear

//	local path C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample
//	local path C:\Users\jonro\Downloads\flights-1m
//	local path C:\Users\jonro\Downloads\fhv_tripdata_2025-01
//	local path C:\Users\jonro\Downloads\fhvhv_tripdata_2024-12

//	local path C:\Users\jonro\Downloads\LLM_match_formulas_all
local path C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_data\econ_data\tm_assignee


capture log close
log using "`path'.log", replace

shell set POLARS_MAX_THREADS="1"

tempfile tsave
use using "`path'.dta", clear

pq save "`tsave'.parquet", replace
capture log close
;
timer on 1
pq use using "`path'.parquet", clear
timer off 1
sum


pq save using "`tsave'.parquet", replace


;
timer on 4
save "`path'.dta", replace
timer off 4
timer on 2
use "`path'", clear
timer off 2
sum


timer on 3
pq save * using "C:/Users/jonro/Downloads/test2.parquet", replace
timer off 3
//	pq describe using "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet"
//	return list
//	pq use using "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet", // in(2/3) //	if(mynum > 0 | missing(mynum) | mytime > 1.1)





di "Confirm the two files are the same"
local allvars
foreach vari of varlist * {
	local allvars `allvars' `vari'
}
rename * *_pq
merge 1:1 _n using `path'.dta, nogen

di "Check if equal after first load to after save+load"
foreach vari in `allvars' {
	
	quietly count if `vari' != `vari'_pq
	local n_all_match = r(N) == 0
	
	di "	`vari': `n_all_match'"
}
quietly {
	noisily di "1: parquet load"
	noisily di "2: stata load"
	noisily di "3: parquet save"
	noisily di "4: stata save"
}
timer list
;
pq use * using "C:/Users/jonro/Downloads/test2.parquet", clear
sum
;

sum

local row_to_show = ceil(runiform()*_N)
di "row_to_show: `row_to_show'"
list in `row_to_show'/`row_to_show'
//	pq use "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet", n(100) offset(1) //	if(a > 2)

capture log close
//	cap program drop polars_parquet_plugin