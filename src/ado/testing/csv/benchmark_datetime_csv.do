set more off
set varabbrev off

//	Benchmark: datetime column read performance via pq use_csv
//	Uses parse_dates to ensure Polars reads columns as Datetime (Int64 physical),
//	which exercises the typed DatetimeChunked fast path added in the optimisation.

capture program drop build_datetime_data
program define build_datetime_data
	version 16
	syntax, n_rows(integer)

	clear
	set obs `n_rows'

	//	Two datetime columns (%tc = ms since 01jan1960)
	gen double dt1 = clock("01jan2020 00:00:00", "DMYhms") + (_n - 1) * 60000
	format dt1 %tc
	gen double dt2 = clock("01jan2015 12:00:00", "DMYhms") + (_n - 1) * 3600000
	format dt2 %tc

	//	One date column (%td = days since 01jan1960)
	gen long   d1  = td(01jan2000) + mod(_n - 1, 365 * 40)
	format d1 %td

	//	Mixed numeric/string filler
	gen double x1  = rnormal()
	gen double x2  = runiform() * 1000
	gen int    i1  = floor(runiform() * 32740)
	gen str20  s1  = "row_" + string(_n)
end

local sizes "10000 100000 1000000"
local reps = 3

tempfile base csv_tmp

set seed 20260304

di as text "Benchmark: pq use_csv with datetime columns (parse_dates)"
di as text "n_rows | pq_avg(s)"
di as text "-------------------------"

foreach n of local sizes {
	build_datetime_data, n_rows(`n')
	pq save_csv "`csv_tmp'.csv", replace

	timer clear

	//	Warmup (not timed)
	pq use_csv "`csv_tmp'.csv", clear parse_dates
	assert _N == `n'

	//	Timed reads
	forvalues r = 1/`reps' {
		clear
		timer on 1
		pq use_csv "`csv_tmp'.csv", clear parse_dates
		timer off 1
		assert _N == `n'
	}

	timer list 1
	local pq_avg = r(t1) / `reps'

	//	Confirm variables are actually numeric (not strings)
	//	If parse_dates failed these would be string type and confirm would error
	confirm numeric variable dt1 dt2 d1

	//	Verify datetime values round-tripped correctly
	//	dt1[1] should equal clock("01jan2020 00:00:00","DMYhms")
	local expected_dt1 = clock("01jan2020 00:00:00", "DMYhms")
	assert abs(dt1[1] - `expected_dt1') < 1000	// within 1 second tolerance

	//	Verify date value
	assert d1[1] == td(01jan2000)

	di as result ///
		"`=strtrim(string(`n',"%12.0gc"))' | " ///
		%8.4f `pq_avg'
}

capture erase "`csv_tmp'.csv"

di as result _newline "Done."
