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
	
	
	
	// DATE variable (days since 01jan1960)
    if `n_cols' > `cols_created' {
        local cols_created = `cols_created' + 1
        quietly gen c_`cols_created' = mdy(1, 1, 2020) + floor(runiform()*365*5)
        format c_`cols_created' %td
    }
    
    // TIME variable (milliseconds since midnight)
    if `n_cols' > `cols_created' {
        local cols_created = `cols_created' + 1
        quietly gen c_`cols_created' = floor(runiform()*86400000)
        format c_`cols_created' %tc
    }
    
    // DATETIME variable (milliseconds since 01jan1960 00:00:00.000)
    if `n_cols' > `cols_created' {
        local cols_created = `cols_created' + 1
        quietly gen double c_`cols_created' = mdyhms(1, 1, 2020, 0, 0, 0) + ///
            floor(runiform()*86400000*365*5)
        format c_`cols_created' %tc
    }
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end

set seed 1000

local tparquet C:\Users\jonro\Downloads\test_java.parquet
local tparquet_java C:\Users\jonro\Downloads\test_java_write.parquet


create_data, n_rows(10000000) n_cols(10) 
compress
timer clear
timer on 1
pq save "`tparquet'.parquet", replace
timer off 1
timer on 2
pq save_java "`tparquet_java'.parquet", replace
timer off 2
local tparquet `tparquet_java'


timer list
;
di "Ignore the initial jar file load time"
//	quietly {
{
	timer on 1
	pq use_java "`tparquet'.parquet", clear in(1/2)
	timer off 1
}


clear
timer clear
count

timer on 1
pq use_java "`tparquet'.parquet", clear
timer off 1
count
sum

save `tparquet', replace
clear

timer on 2
pq use "`tparquet'.parquet", clear
timer off 2
sum


timer on 3
use "`tparquet'", clear
timer off 3
timer list