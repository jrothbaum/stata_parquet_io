set varabbrev off

//	Dedicated date and datetime round-trip test.
//	Verifies that %td (date) and %tc (datetime) variables survive
//	pq save → pq use with exact equality.

tempfile pq1

//	----------------------------------------------------------------------
//	Build reference dataset
//	----------------------------------------------------------------------
clear
set obs 10

//	%td: days since 01jan1960
gen long date_var = td(01jan2020) + (_n - 1)
format date_var %td

//	%tc: milliseconds since 01jan1960 00:00:00
//	Add one day (86,400,000 ms) per row
gen double datetime_var = clock("01jan2020 00:00:00", "DMYhms") + ((_n - 1) * 86400000)
format datetime_var %tc

//	A date column with some missing values
gen long date_missing = td(15mar2023) + (_n - 1)
format date_missing %td
replace date_missing = . if mod(_n, 3) == 0

//	Store reference sums before save
quietly sum date_var
local ref_sum_date = r(sum)
quietly sum datetime_var
local ref_sum_dt = r(sum)
quietly count if missing(date_missing)
local ref_n_missing = r(N)

pq save "`pq1'.parquet", replace


//	----------------------------------------------------------------------
//	Test 1: full load — values must be bit-for-bit identical
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear

assert _N == 10

//	Spot-check specific dates
assert date_var[1]  == td(01jan2020)
assert date_var[10] == td(10jan2020)

//	Spot-check specific datetimes
assert datetime_var[1]  == clock("01jan2020 00:00:00", "DMYhms")
assert datetime_var[2]  == clock("02jan2020 00:00:00", "DMYhms")
assert datetime_var[10] == clock("10jan2020 00:00:00", "DMYhms")

//	Sum equality (verifies no systematic shift)
quietly sum date_var
assert r(sum) == `ref_sum_date'

quietly sum datetime_var
assert abs(r(sum) - `ref_sum_dt') < 1	//	allow 1ms floating-point tolerance

//	Missing pattern preserved
quietly count if missing(date_missing)
assert r(N) == `ref_n_missing'

di as text "Test 1 (full load round-trip): PASSED"


//	----------------------------------------------------------------------
//	Test 2: if filter on date column
//	Parquet stores dates as Unix epoch (days since 01jan1970).  Stata's td()
//	is days since 01jan1960, so td() values cannot be passed raw to SQL.
//	Use the Polars date() function instead: date('ddmonyyyy','%d%b%Y').
//	See: https://github.com/jrothbaum/stata_parquet_io/issues/37
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear if(date_var >= date('05jan2020','%d%b%Y'))

assert _N == 6
local cutoff = td(05jan2020)
quietly count if date_var < `cutoff'
assert r(N) == 0

di as text "Test 2 (if filter on date column using date() syntax): PASSED"


//	----------------------------------------------------------------------
//	Test 3: save with if on date, then reload
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear
tempfile pq2
pq save "`pq2'.parquet", replace if(date_var <= date('03jan2020','%d%b%Y'))

pq use "`pq2'.parquet", clear
assert _N == 3
assert date_var[1] == td(01jan2020)
assert date_var[3] == td(03jan2020)

di as text "Test 3 (save if on date + reload using date() syntax): PASSED"


//	----------------------------------------------------------------------
//	Test 4: td() in if() must be rejected with rc=198
//	----------------------------------------------------------------------
capture pq use "`pq1'.parquet", clear if(date_var >= td(05jan2020))
assert _rc == 198
di as text "Test 4 (td() in if() correctly rejected rc=198): PASSED"

//	----------------------------------------------------------------------
//	Test 5: datetime filter using TIMESTAMP literal
//	datetime_var[3] = 03jan2020 00:00:00; load rows with dt >= that value
//	----------------------------------------------------------------------
pq use "`pq1'.parquet", clear if(datetime_var >= TIMESTAMP '2020-01-03 00:00:00')

assert _N == 8
assert datetime_var[1] == clock("03jan2020 00:00:00", "DMYhms")
assert datetime_var[8] == clock("10jan2020 00:00:00", "DMYhms")

di as text "Test 5 (datetime filter using TIMESTAMP literal): PASSED"


//	----------------------------------------------------------------------
//	Test 6: tc() in if() must be rejected with rc=198
//	----------------------------------------------------------------------
capture pq use "`pq1'.parquet", clear if(datetime_var >= tc(03jan2020 00:00:00))
assert _rc == 198
di as text "Test 6 (tc() in if() correctly rejected rc=198): PASSED"

di as result "All date/datetime round-trip tests PASSED"
