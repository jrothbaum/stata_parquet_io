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
pq save_spss "`tcsv'.sav", replace

clear
pq use_spss "`tcsv'.sav", clear parallelize(columns) compress
assert _N == 100000

clear
pq use_spss "`tcsv'.sav", clear parallelize(rows)
assert _N == 100000

pq use_spss * using "`tcsv'.sav", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10

pq use_spss c_* using "`tcsv'.sav", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10

pq use_spss c_1* c_4 using "`tcsv'.sav", clear
assert _N == 100000
confirm variable c_1
confirm variable c_10
confirm variable c_4
capture confirm variable c_2
assert _rc != 0

pq use_spss "`tcsv'.sav", clear sort(c_2 c_1)
assert c_2[1] == "A"

pq use_spss "`tcsv'.sav", clear sort(-c_2 -c_1)
assert c_2[1] == "E"

pq use_spss * using "`tcsv'.sav", clear compress
assert _N == 100000
local c4type: type c_4
assert "`c4type'" == "byte"

// glob reads should work for SPSS
create_data, n_rows(100) n_cols(10)
pq save_spss "`tcsv'_2018.sav", replace
create_data, n_rows(100) n_cols(10)
pq save_spss "`tcsv'_2019.sav", replace

pq use_spss "`tcsv'_*.sav", clear
assert _N == 200

// append should work when format(spss) is provided
pq use_spss "`tcsv'_2018.sav", clear
pq append "`tcsv'_2019.sav", format(spss)
assert _N == 200

// parquet-only asterisk_to_variable() should fail for SPSS
capture pq describe "`tcsv'_*.sav", asterisk_to_variable(year)
assert _rc != 0

capture pq use_spss "`tcsv'_*.sav", clear asterisk_to_variable(year)
assert _rc != 0

// merge coverage on SPSS sources
create_data, n_rows(100) n_cols(10)
forvalues i = 2/10 {
	rename c_`i' c_`=`i'+10'
}
pq save_spss "`tcsv'_merge.sav", replace

pq use_spss "`tcsv'_2018.sav", clear
pq merge 1:1 c_1 using "`tcsv'_merge.sav", format(spss)
assert _N == 100

capture erase `tcsv'.sav
capture erase `tcsv'_2018.sav
capture erase `tcsv'_2019.sav
capture erase `tcsv'_merge.sav

clear


