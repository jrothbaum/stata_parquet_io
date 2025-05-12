*! pq - read/write parquet files with stata
*! Version 1.0.0

capture program drop pq
program define pq
	gettoken todo 0: 0
    local todo `todo'

    if ("`todo'" == "use") {
		di `"pq_use `0'"'
		pq_use `0'
    }
    else if ("`todo'" == "save") {
		di `"pq_save `0'"'
        pq_save `0'
    }
    else if ("`todo'" == "describe") {
		di `"pq_describe `0'"'
        pq_describe `0'
    }
    else {
        disp as err `"Unknown sub-comand `todo'"'
        exit 198
    }
end

capture program drop pq_use
program pq_use, rclass
    version 16.0
    
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
    pq_register_plugin
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
	
	//	Initialize "mapping" to tell plugin to read from macro variables
	local mapping from_macros
	local b_quiet = 1
	local b_detailed = 1
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' `"`sql_if'"'
	
	local vars_in_file
	local n_renamed = 0
	forvalues i = 1/`n_columns' {
		local vars_in_file `vars_in_file' `name_`i''

		local renamei `rename_`i''
		if ("`renamei'" != "") {
			local n_renamed = `n_renamed' + 1 
			local rename_from_`n_renamed' `name_`i''
			local rename_to_`n_renamed' `renamei'
		}
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
	
	
	tempfile temp_strl
	//	local temp_strl C:\Users\jonro\Downloads\temp_strl
	local temp_strl_stub `temp_strl'
	
	
	quietly set obs `row_to_read'

	local match_vars_non_binary

	local var_number = 0
	local strl_var_indexes
	foreach vari in `matched_vars' {
		local var_number = `var_number' + 1
		local type_info ``vari''
		//	Set rename_to to nothing
		local rename_to
		
		tokenize `type_info', parse("|")
		local type `1'
		local string_length `3'

		//	Does it need to be renamed?
		local name_to_create `vari'
		forvalues i = 1/`n_renamed' {
			local rename_from `rename_from_`i''

			if ("`vari'" == "`rename_from'") {
				local rename_to `rename_to_`i''
				local name_to_create `rename_to'
				continue, break
			}
		}
		

		//	di "name: 			`name_to_create'"
		//	di "type: 			`type'"
		//	di "string_length: 	`string_length'"
	
		local keep = 1
		local strl_limit = 2045
		if ("`type'" == "string") {
			quietly gen str`string_length' `name_to_create' = ""
		}
		else if ("`type'" == "datetime") {
			quietly gen double `name_to_create' = .
			format `name_to_create' %tc
		}
		else if ("`type'" == "date") {
			quietly gen long `name_to_create' = .
			format `name_to_create' %td
		}
		else if ("`type'" == "time") {
			quietly gen double `name_to_create' = .
			format `name_to_create' %tchh:mm:ss
		}
		else if ("`type'" == "binary") {
			di "Dropping `name_to_create' as cannot process binary columns"
			local keep = 0
		}
		else if ("`type'" == "strl") {
			local strl_var_indexes `strl_var_indexes' `var_number'
			quietly gen strL `name_to_create' = ""
		}
		else {
			quietly gen double `type' `name_to_create' = .
		}

		if ("`rename_to'" != "") {
			label variable `name_to_create' "{parquet_name:`vari'}"
		}

		if (`keep') {
			//	di "keeping `vari'"
			local match_vars_non_binary `match_vars_non_binary' `vari'
		}
	}

	local matched_vars `match_vars_non_binary'

	local offset = max(0,`offset' - 1)
	local n_rows = `offset' + `row_to_read'

	plugin call polars_parquet_plugin, read "`using'" "from_macro" `n_rows' `offset' `"`sql_if'"' `"`mapping'"'

	
	if ("`strl_var_indexes'" != "") {
		di "Slowly processing strL variables"
		foreach var_indexi in `strl_var_indexes' {
			forvalues batchi = 1/`n_batches' {
				local pathi `strl_path_`var_indexi'_`batchi''
				local namei `strl_name_`var_indexi'_`batchi''
				local starti `strl_start_`var_indexi'_`batchi''
				local endi `strl_end_`var_indexi'_`batchi''

				if `batchi' == 1 {
					di "	`namei'"
				}
				pq_process_strl, path(`pathi') name(`namei') start(`starti') end(`endi')
			}
		}
	}

end


capture program drop pq_describe
program pq_describe, rclass
    version 16.0
    
    // Parse syntax
    syntax  using/, 					///
			[quietly					///
			 detailed]

	pq_register_plugin
	local b_quiet = ("`quietly'" != "")
	local b_detailed = ("`detailed'" != "")
	pq_register_plugin
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' ""

	
	local macros_to_return n_row n_columns //	mapping
	forvalues i = 1/`n_columns' {
		local macros_to_return `macros_to_return' type_`i' name_`i' rename_`i' 
		
		if (`b_detailed')	local macros_to_return `macros_to_return' string_length_`i'
		
	}
	
	foreach maci in `macros_to_return' {
		return local `maci' = `"``maci''"'
	}
end




capture program drop pq_match_variables
program pq_match_variables, rclass
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

capture program drop pq_save
program pq_save
	version 16.0
	
	
    local input_args = `"`0'"'
    di `"`input_args"'
	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local varlist = substr(`"`input_args'"', 1, `using_pos'-1)
		if (strtrim("`varlist'") == "")	local varlist *

		local rest = substr(`"`input_args'"', `using_pos'+6, .)

		local 0 = `"`varlist' using `rest'"'
		syntax varlist using/ [, replace in(string) if(string) NOAUTORENAME]

    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"* using `input_args'"'
		
        syntax varlist using/ [, replace in(string) if(string) NOAUTORENAME]
        
        // namelist is empty since no "using" separator
    }
	
	pq_register_plugin
	
	local StataColumnInfo from_macros
	local var_count = 0
	local n_rename = 0
	
	foreach vari in `varlist' {
		local var_count = `var_count' + 1
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
		
		local name_`var_count' `vari'
		local dtype_`var_count' `typei'
		local format_`var_count' `formati'
		local str_length_`var_count' `str_length'
		
		//	Rename?
		if ("`noautorename'" == "") {
			local labeli: variable label `vari'

			if regexm(`"`labeli'"', "^\{parquet_name:([^}]*)\}") {
				//	Extract the value between "parquet_name:" and "}"

				local n_rename = `n_rename' + 1
				local rename_from_`n_rename' `vari'
				local rename_to_`n_rename' = regexs(1)

				//	di "n_rename: `n_rename'"
				//	di "	from: `rename_from_`n_rename''"
				//	di "	to:   `rename_to_`n_rename''" 
			}
		}
	}
	
	
	
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
	
	
	//	di `"plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' "`sql_if'" "`StataColumnInfo'""'
	plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' `"`sql_if'"' `"`StataColumnInfo'"'
end




capture program drop pq_register_plugin
program pq_register_plugin
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
		program polars_parquet_plugin, plugin using("`parquet_path'\pq.`plugin_extension'")
		global pq_plugin_loaded = 1
	}

end


capture program drop pq_process_strl
program pq_process_strl
	version 16.0

	syntax , 	path(string)			///
				name(varname)			///
				start(integer)			///
				end(integer)
				
	local index = max(`start', 1)

	file open fstrl using "`path'", read text
	file read fstrl line
	while r(eof) == 0 {
		quietly replace `name' = `"`line'"'  if _n == `index'		

		local index = `index' + 1
		file read fstrl line
	}
	file close fstrl
	capture erase "`pathi'"
end
