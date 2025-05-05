*! read_parquet: Import parquet files into Stata
*! Version 1.0.0
capture program drop pq_use
program define pq_use, rclass
    version 17.0
    
    // Properly handle compound quotes
    local input_args = `"`0'"'
    di `"`input_args'"'
    // Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local namelist = substr(`"`input_args'"', 1, `using_pos'-1)
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
        di `"namelist: `namelist'"'
        di `"rest: `rest'"'
		
        syntax using/ [, clear in(string) if(string)]
    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
        syntax using/ [, clear in(string) if(string)]
        
        // namelist is empty since no "using" separator
        local namelist ""
    }
    
    di "namelist: `namelist'"
    di "using:    `using'"
    di `"if:       `if'"'
    di "in:        `in'"
	
	if ("`in'" != "") {
		
		local offset = substr("`in'", 1, strpos("`in'", "/") -1)
		local offset = max(`offset',0)
		local last_n = substr("`in'", strpos("`in'", "/") + 1, .)
	}
	else {
		local offset = 0
		local last_n = 0
	}
	
	
	//	Process the if statement, if passed
	if (`"`if'"' != "") {
		plugin call polars_parquet_plugin, if `"`if'"'
	}
	else {
		local sql_if
	}
	
	di `"sql_if: 	`sql_if'"'
	
	local b_quiet = 1
	local b_detailed = 1
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' `"`sql_if'"'

	
	local vars_in_file
	forvalues i = 1/`n_columns' {
		local vars_in_file `vars_in_file' `name_`i' '
	}
	
	
	// If namelist is empty or blank, return the full varlist
    if "`namelist'" == "" | "`namelist'" == "*" {
        local matched_vars `vars_in_file'
    }
    else {
        // Use function to match the variables from name list to the ones on the file
        pq_match_variables `namelist', against(`vars_in_file')
		local matched_vars = r(matched_vars)
    }

	//	Create the empty data
	if (`last_n' == 0)	local last_n = `n_rows'
	local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))
	
	
	
	clear
	quietly set obs `row_to_read'
	foreach vari in `matched_vars' {
		local type_info ``vari''
		
		tokenize `type_info', parse("|")
		local type `1'
		local string_length `3'
		
		
		if ("`type'" == "string") {
			quietly gen str`string_length' `vari' = ""
		}
		else if ("`type'" == "datetime") {
			quietly gen double `vari' = .
			format `vari' %tc
		}
		else if ("`type'" == "date") {
			quietly gen long `vari' = .
			format `vari' %td
		}
		else if ("`type'" == "time") {
			quietly gen double `vari' = .
			format `vari' %tchh:mm:ss
		}
		else {
			quietly gen double `type' `vari' = .
		}
	}

	local offset = max(0,`offset' - 1)
	local n_rows = `offset' + `row_to_read'
	di `"plugin call polars_parquet_plugin, function: read "`using'" "`matched_vars'" n_rows: `n_rows' offset: `offset' if: "`sql_if'" mapping: "`mapping'""'
	
	
	plugin call polars_parquet_plugin, read "`using'" "`matched_vars'" `n_rows' `offset' `"`sql_if'"' `"`mapping'"'
end


capture program drop pq_describe
program define pq_describe, rclass
    version 17.0
    
    // Parse syntax
    syntax  using/, 					///
			[quietly					///
			 detailed]

			 
	local b_quiet = ("`quietly'" != "")
	local b_detailed = ("`detailed'" != "")
	
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' ""

	
	local macros_to_return n_row n_columns //	mapping
	forvalues i = 1/`n_columns' {
		local macros_to_return `macros_to_return' type_`i' name_`i' 
		
		if (`b_detailed')	local macros_to_return `macros_to_return' string_length_`i'
		
	}
	
	foreach maci in `macros_to_return' {
		return local `maci' = `"``maci''"'
	}
end


capture program drop pq_match_variables
program define pq_match_variables, rclass
    syntax [anything(name=namelist)], against(string)

	di "namelist: `namelist'"
	di "against:  `against'"
    // Create local macros
    local vars `"`against'"'
    local matched
    local unmatched

    foreach name in `namelist' {
		di "name: `name'"
        local found = 0

        // Wildcard pattern
        if strpos("`name'", "*") | strpos("`name'", "?") {
            foreach v of local against {
                if match("`v'", "`name'") {
                    // Avoid duplicates
                    if strpos("`matched'", "`v'") == 0 {
                        local matched `matched' `v'
                    }
                    local found = 1
                }
            }
        }
        else {
            // Exact match
            foreach v of local against {
                if "`v'" == "`name'" {
                    if strpos("`matched'", "`v'") == 0 {
                        local matched `matched' `v'
                    }
                    local found = 1
                }
            }
        }

        // Track unmatched names
        if `found' == 0 {
            local unmatched `unmatched' `name'
        }
    }

	// Throw error if any names didn't match
    if "`unmatched'" != "" {
        di as error "The following variable(s) were not found: `unmatched'"
        error 111
    }

    // Return matched vars
    return local matched_vars = `"`matched'"'
end



capture log close
log using "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\polars_parquet.log", replace

/*
// Initialize plugin
if "`c(os)'" == "MacOSX" {
  local plugin_extension = "dylib"
} 
else if "`c(os)'" == "Windows" {
  local plugin_extension = "dll"
} 
else {
  local plugin_extension = "so"
}
program polars_parquet_plugin, plugin using("C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\target\release\stata_parquet_io.`plugin_extension'")
*/
if (0${pq_plugin_loaded} == 0) {

    // Plugin is not loaded, so initialize it
    if "`c(os)'" == "MacOSX" {
	  local plugin_extension = "dylib"
	} 
	else if "`c(os)'" == "Windows" {
	  local plugin_extension = "dll"
	} 
	else {
	  local plugin_extension = "so"
	}
	program polars_parquet_plugin, plugin using("C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\target\release\stata_parquet_io.`plugin_extension'")
	
	global pq_plugin_loaded = 1
}
else {
    // Plugin is already loaded, no need to reload
}

timer clear

//	local path C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample
//	local path C:\Users\jonro\Downloads\flights-1m
//	local path C:\Users\jonro\Downloads\fhv_tripdata_2025-01
local path C:\Users\jonro\Downloads\fhvhv_tripdata_2024-12
pq_describe using "`path'.parquet"
timer on 1
pq_use using "`path'.parquet"
timer off 1
sum
save "`path'.dta", replace

timer on 2
use "`path'", clear
timer off 2
sum
timer list
//	pq_describe using "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet"
//	return list
//	pq_use using "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet", // in(2/3) //	if(mynum > 0 | missing(mynum) | mytime > 1.1)
;

sum

local row_to_show = ceil(runiform()*_N)
di "row_to_show: `row_to_show'"
list in `row_to_show'/`row_to_show'
//	pq_use "C:/Users/jonro/Downloads/pyreadstat/test_data/basic/sample.parquet", n(100) offset(1) //	if(a > 2)

capture log close
//	cap program drop polars_parquet_plugin