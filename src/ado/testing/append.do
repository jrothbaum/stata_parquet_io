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
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end

local cols_created = 4
local n_rows = 50
set seed 100
create_data, n_rows(`n_rows') n_cols(`cols_created') 


local var_types byte int long float double
//	local var_types byte float 
local gen_byte = "floor(runiform()*(2^3))"
local gen_int = "floor(runiform()*(2^15-1))"
local gen_long = "floor(runiform()*(2^31-1))"
local gen_float = "runiform()"
local gen_double = "runiform()"

foreach typei in `var_types' {
	foreach typej in `var_types' {
		set seed 100
		gen `typei' `typei'_`typej' = `gen_`typei''
	}
}


local var_types_str strL str1 str10 
local gen_str1 = "length(1)"
local gen_str10 = "length(10)"
local gen_strL = "length(1500)"

foreach typei in `var_types_str' {
	foreach typej in `var_types_str' {
		local concat
		set seed 100
		if ("`typei'" == "strL") {
				forvalues i = 1/2 {
					tempvar v`i'
					ralpha `v`i'', `gen_`typei''
					
					if ("`concat'" != "")	local concat `concat'+
					local concat `concat'`v`i''
				}
				
				gen `typei'_`typej' = `concat'
				
				forvalues i = 1/2 {
					quietly drop `v`i''
				}
		}
		else {
			ralpha `typei'_`typej', `gen_`typei''
		}
		if "`typei'" == "strL" recast strL `typei'_`typej'
	}
}

describe
tempfile t_save
pq save "`t_save'.parquet", replace

clear
set seed 100
create_data, n_rows(`n_rows') n_cols(`cols_created') 


foreach typei in `var_types_str' {
	foreach typej in `var_types_str' {
		set seed 100
		if ("`typej'" == "strL") {
				local concat
				forvalues i = 1/2 {
					tempvar v`i'
					ralpha `v`i'', `gen_`typej''
					
					if ("`concat'" != "")	local concat `concat'+
					local concat `concat'`v`i''
				}
				gen `typei'_`typej' = `concat'
				
				forvalues i = 1/2 {
					quietly drop `v`i''
				}
		}
		else {
			ralpha `typei'_`typej', `gen_`typej''
		}
		if "`typej'" == "strL" recast strL `typei'_`typej'
	}
}


foreach typei in `var_types' {
	foreach typej in `var_types' {
		set seed 100
		gen `typej' `typei'_`typej' = `gen_`typej''
	}
}
count

pq save "`t_save'_append.parquet", replace

pq use "`t_save'.parquet", clear
count
pq append "`t_save'_append.parquet"
count

sum
describe


foreach typei in `var_types' {
	foreach typej in `var_types' {
		quietly count if missing(`typei'_`typej')
		di "`typei'_`typej': " r(N)
		sum `typei'_`typej' if _n <= `n_rows'
		sum `typei'_`typej' if _n > `n_rows'
	}
}

foreach typei in `var_types_str' {
	foreach typej in `var_types_str' {
		quietly count if missing(`typei'_`typej')
		di "`typei'_`typej': " r(N)
	}
}


capture erase "`t_save'.parquet"
capture erase "`t_save'_append.parquet"