*! polars_parquet - read/write parquet files with stata
*! Version 1.0.0
capture program drop _pq_use
program define _pq_use, rclass
    version 17.0
    
    local input_args = `"`0'"'

	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local namelist = substr(`"`input_args'"', 1, `using_pos'-1)
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
        
        syntax using/ [, clear in(string) if(string)]
    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
        syntax using/ [, clear in(string) if(string)]
        
        // namelist is empty since no "using" separator
        local namelist ""
    }
    
	`clear'
	
	if `=_N' > 0 {
		display as error "There is already data loaded, pass clear if you want to load a parquet file"
		exit 2000
	}
	
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
	
	plugin call polars_parquet_plugin, read "`using'" "`matched_vars'" `n_rows' `offset' `"`sql_if'"' `"`mapping'"'
end


capture program drop _pq_describe
program define _pq_describe, rclass
    version 17.0
    
    // Parse syntax
    syntax  using/, 					///
			[quietly					///
			 detailed]

			 
	local b_quiet = ("`quietly'" != "")
	local b_detailed = ("`detailed'" != "")
	pq_register_plugin
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

	
	// Create local macros
    local vars `"`against'"'
    local matched
    local unmatched

    foreach name in `namelist' {
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

capture program drop _pq_save
program define _pq_save
	version 17.0
	
	
    local input_args = `"`0'"'
    
	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local varlist = substr(`"`input_args'"', 1, `using_pos'-1)

		local rest = substr(`"`input_args'"', `using_pos'+6, .)

		local 0 =  `"`varlist' using `rest'"'

        syntax varlist using/ [, replace in(string) if(string)]

    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"* using `input_args'"'
		
        syntax varlist using/ [, replace in(string) if(string)]
        
        // namelist is empty since no "using" separator
        local varlist ""
    }
	
	pq_register_plugin
	
	local StataColumnInfo
	foreach vari in `varlist' {
		local typei: type `vari'
		local formati: format `vari'
		
		local str_length 0
		
		if ((substr("`typei'",1,3) == "str") & ("`typei'" != "strl")) {
			local str_length = substr("`typei'",4,.)
			local typei String
		}
		else {
			local typei = strproper("`typei'")
		}
		if ("`StataColumnInfo'" != "")	{
			local StataColumnInfo = `"`StataColumnInfo',"'
		}
		
		local StataColumnInfo = `"`StataColumnInfo'{"name":"`vari'","dtype":"`typei'","format":"`formati'","str_length":`str_length'}"'
	}
	
	local StataColumnInfo = `"[`StataColumnInfo']"'
	
	
	
	if ("`in'" != "") {
		
		local offset = substr("`in'", 1, strpos("`in'", "/") -1)
		local offset = max(`offset',0)
		local last_n = substr("`in'", strpos("`in'", "/") + 1, .)
		local n_rows = `last_n' - `offset' + 1
	}
	else {
		local offset = 0
		local last_n = 0
		local n_rows = 0
	}
	
	
	//	Process the if statement, if passed
	if (`"`if'"' != "") {
		plugin call polars_parquet_plugin, if `"`if'"'
	}
	else {
		local sql_if
	}
	
	
	
	local offset = max(0,`offset' - 1)
	
	
	
	plugin call polars_parquet_plugin, save "`using'" "`varlist'" `n_rows' `offset' `"`sql_if'"' `"`StataColumnInfo'"'
end




capture program drop pq_register_plugin
program define pq_register_plugin
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

		
		if ("${parquet_dll_override}" != "") {
			local parquet_path = "${parquet_dll_override}"
		}
		else {
			local parquet_path = "`c(sysdir_plus)'p"
		}
		program polars_parquet_plugin, plugin using("`parquet_path'\stata_parquet_io.`plugin_extension'")
		global pq_plugin_loaded = 1
	}

end