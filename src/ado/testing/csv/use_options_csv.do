set varabbrev off

capture program drop create_data
program define create_data
	version 16
	syntax		, 	n_cols(integer)			///
					n_rows(integer)
	
	clear
	set obs `n_rows'
	local cols_created = 0

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen long c_`cols_created' = _n
	}

	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = char(65 + floor(runiform()*5))
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = rnormal()
		quietly tostring c_`cols_created', replace force
	}
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = floor(runiform()*100)
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		quietly gen c_`cols_created' = 100 + floor(runiform()*60)
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end

set seed 1000
create_data, n_rows(100000) n_cols(10)
tempfile tcsv
compress
pq save_csv "`tcsv'.csv", replace

clear
pq use_csv "`tcsv'.csv", clear parallelize(columns) compress
assert _N == 100000

clear
pq use_csv "`tcsv'.csv", clear parallelize(rows)
assert _N == 100000

clear
pq use_csv "`tcsv'.csv", clear batch_size(64)
assert _N == 100000

clear
pq use_csv "`tcsv'.csv", clear infer_schema_length(0)
assert _N == 100000

pq use_csv * using "`tcsv'.csv", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10

pq use_csv c_* using "`tcsv'.csv", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10

pq use_csv c_1* c_4 using "`tcsv'.csv", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10
confirm variable c_4
capture confirm variable c_2
assert _rc != 0

pq use_csv "`tcsv'.csv", clear sort(c_2 c_1)
assert c_2[1] == "A"

pq use_csv "`tcsv'.csv", clear sort(-c_2 -c_1)
assert c_2[1] == "E"

pq use_csv * using "`tcsv'.csv", clear compress
assert _N == 100000
local c4type: type c_4
assert "`c4type'" == "byte"

// glob reads should work for CSV
create_data, n_rows(100) n_cols(10)
pq save_csv "`tcsv'_2018.csv", replace
create_data, n_rows(100) n_cols(10)
pq save_csv "`tcsv'_2019.csv", replace

pq use_csv "`tcsv'_*.csv", clear
assert _N == 200

// append should work when format(csv) is provided
pq use_csv "`tcsv'_2018.csv", clear
pq append "`tcsv'_2019.csv", format(csv)
assert _N == 200

// parquet-only asterisk_to_variable() should fail for CSV
capture pq describe "`tcsv'_*.csv", asterisk_to_variable(year)
assert _rc != 0

capture pq use_csv "`tcsv'_*.csv", clear asterisk_to_variable(year)
assert _rc != 0

// merge coverage on CSV sources
create_data, n_rows(100) n_cols(10)
forvalues i = 2/10 {
	rename c_`i' c_`=`i'+10'
}
pq save_csv "`tcsv'_merge.csv", replace

pq use_csv "`tcsv'_2018.csv", clear
pq merge 1:1 c_1 using "`tcsv'_merge.csv", format(csv)
assert _N == 100

capture erase `tcsv'.csv
capture erase `tcsv'_2018.csv
capture erase `tcsv'_2019.csv
capture erase `tcsv'_merge.csv

clear
