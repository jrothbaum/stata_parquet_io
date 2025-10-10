tempfile pq1
tempfile pq2

tempfile f_append1
tempfile f_append2
tempfile f_append3


local with_long_string = 1

sysuse sp500, clear
gen nr=_n
//	keep if (nr<5 | nr>244)
save "`pq1'.dta", replace

preserve	
	keep nr date open high volume 
    order nr date open high volume
	list if _n < 5 | _n > 244  // list the selected variables to compare with the 3 examples
restore

list if _n < 5 | _n > 244

local keep_long_string
if (`with_long_string ') {
	gen long_string = string(_n) + 3000*"a"
	local keep_long_string long_string 
}

*2. Gen two parquet datasets
preserve
	keep if _n<5
	pq save using "`pq1'.parquet", replace
restore

preserve
	keep if _n>244
	pq save using "`pq2'.parquet", replace
restore

pq use nr date open high volume `keep_long_string' using "`pq1'.parquet", clear
pq append nr date open high volume `keep_long_string' using "`pq2'.parquet"
order nr date open high volume `keep_long_string'

if (`with_long_string ')	replace long_string = substr(long_string,1,10)
save "`f_append1'.dta", replace
list


pq use using "`pq1'.parquet", clear
pq append nr date open high volume `keep_long_string' using "`pq2'.parquet"
keep nr date open high volume `keep_long_string' 
order nr date open high volume `keep_long_string' 
if (`with_long_string ')	replace long_string = substr(long_string,1,10)
list
save "`f_append2'.dta", replace


pq use using "`pq1'.parquet", clear
pq append using "`pq2'.parquet"
keep nr date open high volume `keep_long_string'
order nr date open high volume `keep_long_string'
if (`with_long_string ')	replace long_string = substr(long_string,1,10)
list
save "`f_append3'.dta", replace


use "`f_append1'.dta", clear
cf _all using "`f_append2'.dta"
cf _all using "`f_append3'.dta"
