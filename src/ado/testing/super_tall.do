set varabbrev off

capture program drop create_data
program define create_data
	version 16
	syntax		, 	n_cols(integer)			///
					n_rows(integer)
	
	clear
	set obs `n_rows'
	
	forvalues ci = 1/`n_cols' {
		quietly gen byte c_`ci' = floor(runiform()*100)
	}
end


create_data, n_cols(5) n_rows(1E9)
tempfile tparquet

pq save "`tparquet'.parquet", replace
clear

timer clear

local timer_number = 0
forvalues batch_e = 6/8 {
	local timer_number = `timer_number' + 1
	di "`timer_number'"
	di 1E`batch_e'

	clear
	timer on `timer_number'
	pq use "`tparquet'.parquet", clear batch_size(1E`batch_e')
	timer off `timer_number'
}

timer list


//	memory

