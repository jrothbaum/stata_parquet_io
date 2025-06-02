*! pq - read/write parquet files with stata
*! Version 1.1.0

capture program drop pq
program define pq
	gettoken todo 0: 0
    local todo `todo'

    if ("`todo'" == "use") {
		//	di `"pq_use `0'"'
		pq_use `0'
    }
	else if ("`todo'" == "append") {
		//	di `"pq_append `0'"'
		pq_append `0'
    }
	else if ("`todo'" == "merge") {
		//	di `"pq_merge `0'"'
		pq_merge `0'
    }
    else if ("`todo'" == "save") {
		//	di `"pq_save `0'"'
        pq_save `0'
    }
    else if ("`todo'" == "describe") {
		//	di `"pq_describe `0'"'
        pq_describe `0'
    }
    else {
        disp as err `"Unknown sub-comand `todo'"'
        exit 198
    }
end

capture program drop pq_use
program pq_use
    version 16.0
    
    local input_args = `"`0'"'

	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local namelist = substr(`"`input_args'"', 1, `using_pos'-1)
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
	}
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
    
        // namelist is empty since no "using" separator
        local namelist ""
    }

	syntax using/ [, 	clear 					///
						in(string) 				///
						if(string asis) 		///
						relaxed 				///
						asterisk_to_variable(string)	///
						parallelize(string)		///
						sort(string)			///
						compress				///
						compress_string_to_numeric]

    pq_register_plugin
	`clear'
	
	if `=_N' > 0 {
		display as error "There is already data loaded, pass clear if you want to load a parquet file"
		exit 2000
	}

	if (!inlist("`parallelize'", "", "columns", "rows")) {
		display as error `"Acceptable options for parallelize are "columns", "rows", and "", passed "`parallelize'""'
		exit 198
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
		local greater_than = strpos(`"`if'"', ">") > 0
		if (`greater_than') {
			di as error "pq will interpret > as in SQL, which is different than Stata."
			di as error "	It will not include . as > any value."
		}
		//	di `"plugin call polars_parquet_plugin, if "`if'""'
		plugin call polars_parquet_plugin, if `"`if'"'
	}
	else {
		local sql_if
	}
	
	//	di `"if: `sql_if'"'
	//	Initialize "mapping" to tell plugin to read from macro variables
	local mapping from_macros
	local b_quiet = 1
	local b_detailed = 1
	local b_compress = "`compress'" != ""
	local b_compress_string_to_numeric = "`compress_string_to_numeric'" != ""
	//	di `"plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "`sql_if'" "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric'"'
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' `"`sql_if'"' "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric'
	
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
		local match_all = 1
    }
    else {
        // Use function to match the variables from name list to the ones on the file
        pq_match_variables `namelist', against(`vars_in_file')

		local matched_vars = r(matched_vars)
		local match_all = 0
    }
	
	//	Create the empty data
	if (`last_n' == 0)	local last_n = `n_rows'
	local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))
	
	
	tempfile temp_strl
	//	local temp_strl C:\Users\jonro\Downloads\temp_strl
	local temp_strl_stub `temp_strl'
	
	
	quietly set obs `row_to_read'

	local match_vars_non_binary

	local dropped_vars = 0
	local strl_var_indexes

	foreach vari in `matched_vars' {
		local var_number: list posof "`vari'" in vars_in_file
		local type `type_`var_number''
		local string_length `string_length_`var_number''
		//	di "var_number: `var_number'"
		//	di "vari: `vari'"
		//	di "string_length_`var_number': `string_length'"
		
		//	Set rename_to to nothing
		local rename_to
		
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
			local string_length = max(1,`string_length')
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
			local strl_position_`var_number' = `var_number' - `dropped_vars'
			local strl_var_indexes `strl_var_indexes' `var_number'
			quietly gen strL `name_to_create' = ""
		}
		else if ("`type'" == "float") {
			quietly gen float `type' `name_to_create' = .
		}
		else if ("`type'" == "long") {
			quietly gen long `type' `name_to_create' = .
		}
		else if ("`type'" == "int") {
			quietly gen int `type' `name_to_create' = .
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
		else {
			local dropped_vars = `dropped_vars' + 1
		}
	}

	local matched_vars `match_vars_non_binary'

	local offset = max(0,`offset' - 1)
	//	local n_rows = `offset' + `row_to_read'

	//	Tell polars to concatenate file list with "vertical_relaxed"
	//		so that it can combine different schema types
	//		to their supertype 
	//		i.e. if a is an int8 in file 1 and an int16 in file 2,
	//			it won't throw an error, but make it an int16 in the final file
	local vertical_relaxed = "`relaxed'" != ""

	//	asterisk_to_variable - for files /file/*.parquet, convert
	//		* to a variable, so /file/2019.parquet, file/2020.parquet
	//		will have the item in asterisk_to_variable as 2019 and 2020
	//		for the records on the file
	di `"plugin call polars_parquet_plugin, read "`using'" "from_macro" `row_to_read' `offset' "`sql_if'" "`mapping'" "`parallelize'" `vertical_relaxed' "`asterisk_to_variable'" "`sort'" 0"'
	plugin call polars_parquet_plugin, read "`using'" "from_macro" `row_to_read' `offset' `"`sql_if'"' `"`mapping'"' "`parallelize'" `vertical_relaxed' "`asterisk_to_variable'" "`sort'" 0

	if ("`strl_var_indexes'" != "") {
		di "Slowly processing strL variables"
	
		foreach var_indexi in `strl_var_indexes' {
			local lookup_number `strl_position_`var_indexi''
			forvalues batchi = 1/`n_batches' {
				local pathi `strl_path_`lookup_number'_`batchi''
				local namei `strl_name_`lookup_number'_`batchi''
				local starti `strl_start_`lookup_number'_`batchi''
				local endi `strl_end_`lookup_number'_`batchi''

				if `batchi' == 1 {
					di "	`namei'"
				}
				
				//	di "strl_position_`var_indexi' = `strl_position_`var_indexi''"
				//	di `"pq_process_strl, path(`pathi') name(`namei') var_index(`strl_position_`var_indexi'') first(`starti') last(`endi')"'
				pq_process_strl, path(`pathi') name(`namei') var_index(`strl_position_`var_indexi'') first(`starti') last(`endi')
			}
		}
	}
end



capture program drop pq_merge
program pq_merge
    version 16.0
    
    gettoken mtype 0 : 0, parse(" ,")
	local origmtype `"`mtype'"'
	//	di "mtype: `mtype'"
	local varlist_n
	/* ------------------------------------------------------------ */
				/* parsing				*/
				/* we have pulled off <mtype> from 0	*/
	gettoken token : 0, parse(" ,")
	if ("`token'"=="_n") {
		if ("`mtype'"!="1:1") {
			error_seq_not11 "`mtype'" "`origmtype'"
			/*NOTREACHED*/
		}
		gettoken token 0 : 0, parse(" ,")
		local varlist_n _n
	}

	syntax [varlist(default=none)] using/ [,	///
		  ASSERT(string)			///
		  DEBUG					///
		  GENerate(name)			///
		  FORCE					///
		  KEEP(string)				///
		  KEEPUSing(string)			///
		noLabels				///
		  NOGENerate			        ///
		noNOTEs					///
		  REPLACE				///
		noREPort				///	
		  SORTED				///
		  UPDATE       				///
		  in(string) 				///
		if(string asis) 		///
		relaxed 				///
		asterisk_to_variable(string)	///
		parallelize(string)		///
		sort(string)			///
		compress				///
		compress_string_to_numeric	///
		]

	if "`keepusing'" != "" {
		if ("`varlist_n'" == "_n")	local using_vars `keepusing'
		else 						local using_vars `varlist' `keepusing' 
	}
	else {
		local using_vars
	}

	tempfile t_save
	tempname f_pq
	frame create `f_pq'
	frame `f_pq' {
		di "Loading parquet file and saving to temporary dta"
		pq use `using_vars' using `"`using'"', 	clear in(`in') 					///
												if(`if') 						///
												`relaxed' 						///
												asterisk_to_variable(`asterisk_to_variable')	///
												parallelize(`parallelize')		///
												sort(`varlist')					///
												`compress'						///
												`compress_string_to_numeric'
		quietly save `t_save'
	}
	/*
	di `"merge `origmtype' `varlist_n'`varlist' using "`t_save'",	gen(`generate') 	///"'
	di `"												`nogenerate'			///"'
	di `"												`nolabel'				///"'
	di `"												`nonotes'				///"'
	di `"												`update'				///"'
	di `"												`replace'				///"'
	di `"												`noreport'				///"'
	di `"												`force'					///"'
	di `"												assert(`assert')		///"'
	di `"												keep(`keep')"'
	*/

	
	di "Merging to data"
	merge `origmtype' `varlist_n'`varlist' using "`t_save'",	gen(`generate') 	///
													`nogenerate'			///
													`nolabel'				///
													`nonotes'				///
													`update'				///
													`replace'				///
													`noreport'				///
													`force'					///
													assert(`assert')		///
													keep(`keep')



	frame drop `f_pq'

end


capture program drop pq_append
program pq_append
    version 16.0
    
    local input_args = `"`0'"'

	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local namelist = substr(`"`input_args'"', 1, `using_pos'-1)
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
	}
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
    
        // namelist is empty since no "using" separator
        local namelist ""
    }

	syntax using/ [, 	in(string) 				///
						if(string asis) 		///
						relaxed 				///
						asterisk_to_variable(string)	///
						parallelize(string)		///
						sort(string)			///
						compress				///
						compress_string_to_numeric	///
						clear					///
						append]

    pq_register_plugin
	

	if (!inlist("`parallelize'", "", "columns", "rows")) {
		display as error `"Acceptable options for parallelize are "columns", "rows", and "", passed "`parallelize'""'
		exit 198
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
		local greater_than = strpos(`"`if'"', ">") > 0
		if (`greater_than') {
			di as error "pq will interpret > as in SQL, which is different than Stata."
			di as error "	It will not include . as > any value."
		}
		//	di `"plugin call polars_parquet_plugin, if "`if'""'
		plugin call polars_parquet_plugin, if `"`if'"'
	}
	else {
		local sql_if
	}
	
	//	di `"if: `sql_if'"'
	//	Initialize "mapping" to tell plugin to read from macro variables
	local mapping from_macros
	local b_quiet = 1
	local b_detailed = 1
	local b_compress = "`compress'" != ""
	local b_compress_string_to_numeric = "`compress_string_to_numeric'" != ""
	//	di `"plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "`sql_if'" "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric'"'
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' `"`sql_if'"' "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric'
	
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
		local match_all = 1
    }
    else {
        // Use function to match the variables from name list to the ones on the file
        pq_match_variables `namelist', against(`vars_in_file')

		local matched_vars = r(matched_vars)
		local match_all = 0
    }
	
	//	Get the list of already existing variables
	unab all_vars: *
	local n_vars_already : word count `all_vars'

	//	Create the empty data, if needed, or add rows, if needed
	if (`last_n' == 0)	local last_n = `n_rows'
	local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))
	//	di "local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))"
	
	tempfile temp_strl
	local temp_strl_stub `temp_strl'
	
	local n_obs_already = _N
	//	di "local n_obs_after = `n_obs_already' + `row_to_read'"
	local n_obs_after = `n_obs_already' + `row_to_read'
	quietly set obs `n_obs_after'

	local match_vars_non_binary

	local dropped_vars = 0
	local strl_var_indexes

	foreach vari in `matched_vars' {
		local var_number: list posof "`vari'" in vars_in_file
		local type `type_`var_number''
		local string_length `string_length_`var_number''
		//	di "var_number: `var_number'"
		//	di "vari: `vari'"
		//	di "string_length_`var_number': `string_length'"
		
		//	Set rename_to to nothing
		local rename_to
		
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
		//	di "pq_gen_or_recast,	name(`name_to_create')			///"
		//	di "					type_new(`type')				///"
		//	di "					str_length(`string_length')"
		pq_gen_or_recast,	name(`name_to_create')			///
							type_new(`type')				///
							str_length(`string_length')
		local keep = 1

		if ("`type'" == "strl") {
			local strl_position_`var_number' = `var_number' - `dropped_vars'
			local strl_var_indexes `strl_var_indexes' `var_number'
		}
		else if ("`type'" == "datetime") {
			format `name_to_create' %tc
		}
		else if ("`type'" == "date") {
			format `name_to_create' %td
		}
		else if ("`type'" == "time") {
			format `name_to_create' %tchh:mm:ss
		}
		else if ("`type'" == "binary") {
			di "Dropping `name_to_create' as cannot process binary columns"
			local keep = 0
		}

		if ("`rename_to'" != "") {
			label variable `name_to_create' "{parquet_name:`vari'}"
		}

		if (`keep') {
			//	di "keeping `vari'"
			local match_vars_non_binary `match_vars_non_binary' `vari'
		}
		else {
			local dropped_vars = `dropped_vars' + 1
		}
	}

	local matched_vars `match_vars_non_binary'
	local n_matched_vars: word count `match_vars_non_binary'
	
	//	Some work to get the proper variable indices for the ones to append
	//		This is a little wasteful on use, but it's easier to have one way
	//		for the code to run

	tempname f_var_list
	frame create `f_var_list'
	frame `f_var_list' {
		quietly gen index = .
		quietly gen name = ""
		quietly gen byte to_edit = .
		quietly set obs `n_vars_already'

		forvalues i = 1/`n_vars_already' {
			local vari = word("`all_vars'", `i')
			quietly replace name = "`vari'" if _n == `i'
		}
		quietly replace to_edit = 0
		quietly set obs `=`n_vars' + `n_vars_already''
		forvalues i = 1/`n_vars' {
			local vari = word("`matched_vars'", `i')
			
			quietly replace name = "`vari'" if _n == (`i' + `n_vars_already')
			quietly replace to_edit = 1 if _n == (`i' + `n_vars_already')
		}
		quietly replace index = _n
		
		sort name index
		
		quietly by name: replace to_edit = to_edit[_N]
		quietly by name: gen new_index = index[_N]
		quietly by name: keep if _n == 1
		sort new_index
		forvalues i = 1/`=_N' {
			local index_`i' = index[`i']
		}
	}
	frame drop `f_var_list'

	//	Reassign the strL indices
	if ("`strl_var_indexes'" != "") {
		local strl_var_indexes_old `strl_var_indexes'
		local strl_var_indexes
		foreach indexi in `strl_var_indexes_old' {
			local strl_var_indexes `strl_var_indexes' `index_`indexi''
		}
	}
	
	local offset = max(0,`offset' - 1)
	//	local n_rows = `offset' + `row_to_read'

	//	Tell polars to concatenate file list with "vertical_relaxed"
	//		so that it can combine different schema types
	//		to their supertype 
	//		i.e. if a is an int8 in file 1 and an int16 in file 2,
	//			it won't throw an error, but make it an int16 in the final file
	local vertical_relaxed = "`relaxed'" != ""

	//	asterisk_to_variable - for files /file/*.parquet, convert
	//		* to a variable, so /file/2019.parquet, file/2020.parquet
	//		will have the item in asterisk_to_variable as 2019 and 2020
	//		for the records on the file
	//	di `"plugin call polars_parquet_plugin, read "`using'" "`matched_vars'" `row_to_read' `offset' "`sql_if'" "`mapping'" "`parallelize'" `vertical_relaxed' "`asterisk_to_variable'" "`sort'" `n_obs_already'"'


	plugin call polars_parquet_plugin, read "`using'" "from_macro" `row_to_read' `offset' `"`sql_if'"' `"`mapping'"' "`parallelize'" `vertical_relaxed' "`asterisk_to_variable'" "`sort'" `n_obs_already'
	
	if ("`strl_var_indexes'" != "") {
		di "Slowly processing strL variables"
		foreach var_indexi in `strl_var_indexes' {
			local lookup_number `var_indexi'
			forvalues batchi = 1/`n_batches' {
				local pathi `strl_path_`lookup_number'_`batchi''
				local namei `strl_name_`lookup_number'_`batchi''
				local starti `strl_start_`lookup_number'_`batchi''
				local endi `strl_end_`lookup_number'_`batchi''

				//	local starti = `starti' + `n_obs_already'

				if `batchi' == 1 {
					di "	`namei'"
				}
				
				//	di `"pq_process_strl, path(`pathi') name(`namei') var_index(`lookup_number') first(`starti') last(`endi')"'
				pq_process_strl, path(`pathi') name(`namei') var_index(`lookup_number') first(`starti') last(`endi')
			}
		}
	}
end

capture program drop pq_gen_or_recast
program pq_gen_or_recast
	version 16
	syntax  ,	name(string)				///
			 	type_new(string)			///
				str_length(integer)
	
	local string_length = max(1,`str_length')
	if ("`type_new'" == "datetime")			local type_new double
	else if ("`type_new'" == "time")		local type_new double
	else if ("`type_new'" == "date")		local type_new long
	else if ("`type_new'" == "string")		local type_str str`str_length'
	
	capture confirm variable `name'
	local b_gen = _rc > 0

	local vartype
	if (!`b_gen')	local vartype: type `name'

	//	di _newline(2)
	//	di "name: 		`name'"
	//	di "type_new: 		`type_new'"
	//	di "string_length:	`string_length'"
	//	di "vartype: 		`vartype'"
	

	if ("`type_new'" == "string") {
		if `b_gen' {
			quietly gen `type_str' `name' = ""
		}
		else {
			// Check if it's a fixed-length string type (str1, str2, etc.)
			if regexm("`vartype'", "^str([0-9]+)$") {
				local current_length = regexs(1)
				
				if `string_length' > `current_length' {
					recast str`string_length' `name'
				}
			}
			else if inlist("`vartype'", "byte", "int", "long", "float", "double") {
				tostring `name', replace force
			}
		}
	}
	else if ("`type'" == "strl") {
		local strl_position_`var_number' = `var_number' - `dropped_vars'
		local strl_var_indexes `strl_var_indexes' `var_number'

		if `b_gen' {
			quietly gen strL `name' = ""
		}
		else {
			// Check if it's a fixed-length string type (str1, str2, etc.)
			if regexm("`vartype'", "^str([0-9]+)$") {
				recast strL `name'
			}
			else if inlist("`vartype'", "byte", "int", "long", "float", "double") {
				tostring `name', replace force
				recast strL `name'
			}
		}
	}
	else if ("`type'" == "float") {
		if `b_gen' {
			quietly gen float `type' `name' = .
		}
		else {
			if inlist("`vartype'", "long","double") {
				recast double `name'
			}
			else if inlist("`vartype'", "byte", "int") {
				recast float `name'
			}
		}
	}
	else if ("`type'" == "long") {
		if `b_gen' {
			quietly gen long `name' = .
		}
		else {
			if inlist("`vartype'", "byte", "int") {
				recast long `name'
			}
			else if inlist("`vartype'", "float") {
				recast double `name'
			}
		}
	}
	else if ("`type'" == "int") {
		if `b_gen' {
			quietly gen int `name' = .
		}
		else {
			if inlist("`vartype'", "byte") {
				recast int `name'
			}
		}
	}
	else if ("`type'" == "byte") {
		if `b_gen' {
			quietly gen byte `name' = .
		}
		else {
			if inlist("`vartype'", "int","long","float","double") {
				recast `vartype' `name'
			}
		}
	}
	else {
		if `b_gen' {
			quietly gen double `name' = .
		}
		else {
			if inlist("`vartype'", "byte", "int", "long", "float") {
				recast double `name'
			}
		}
	}
end

capture program drop pq_describe
program pq_describe, rclass
    version 16.0
    
	local input_args = `"`0'"'

	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local pre_using = substr(`"`input_args'"', 1, `using_pos'-1)

		if `"`pre_using'"' != "" {
			di as error "varlist not allowed"
			error 101
		}
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
        
        // As intended, pre_using needs to be blank

    }

    // Parse syntax
    syntax  using/, 					///
			[quietly					///
			 detailed					///
			 asterisk_to_variable(string)]

	pq_register_plugin
	local b_quiet = ("`quietly'" != "")
	local b_detailed = ("`detailed'" != "")
	
	//	Trailing zeros are compress indicators
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "" "`asterisk_to_variable'" 0 0

	
	local macros_to_return n_rows n_columns //	mapping
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
    //	di `"`input_args"'
	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local varlist = substr(`"`input_args'"', 1, `using_pos'-1)
		if (strtrim("`varlist'") == "")	local varlist *

		local rest = substr(`"`input_args'"', `using_pos'+6, .)

		local 0 = `"`varlist' using `rest'"'
    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"* using `input_args'"'
        
        // namelist is empty since no "using" separator
    }

	syntax varlist using/ [, replace 						///
						   if(string asis) 					///
						   NOAUTORENAME						///
						   partition_by(varlist)			///
						   compression(string)				///
						   compression_level(integer -1)	///
						   NOPARTITIONOVERWRITE				///
						   compress							///
						   compress_string_to_numeric		///	
						   ]	//	in(string) 
        
	//	if "`partition_by'" != "" {
	//		di as error "Hive partitioning not implemented yet"
	//		exit 198
	//	}
	if (!inlist("`compression'", "", "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd")) {
		display as error `"Acceptable options for compression are "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd", and "" ("" will be zstd), passed "`compression'""'
		exit 198
	}
	
	if `compression_level' != -1 {
		local check_compression_level = 0
		if inlist("`compression'", "", "zstd") {
			local check_compression_level = 1
			local compression_level_min = 1
			local compression_level_max = 22
		}
		else if "`compression'"== "brotli" {
			local check_compression_level = 1
			local compression_level_min = 0
			local compression_level_max = 11
		}
		else if "`compression'"== "gzip" {
			local check_compression_level = 1
			local compression_level_min = 0
			local compression_level_max = 9
		}
		
		if `check_compression_level' {
			if !inrange(`compression_level', `compression_level_min', `compression_level_max') {
				display as error `"Acceptable compression_level range for compression = "`compression'" (zstd if blank) [`compression_level_min', `compression_level_max'], passed "`compression_level'""'
				exit 198
				
			}
		}
	}
		
	//	Currently not available to have an in statement on write
	local in
	pq_register_plugin
	
	if "`replace'" == "" {
		//	Check if file exists as file or path
		quietly local is_file = fileexists("`using'")
		mata: st_local("is_directory",  strofreal(direxists("`using'")))

		if `is_file' | `is_directory' {
			di as error "File exists: `using'"
			di as error `" 	Add ", replace" if you want to overwrite the file"'
			error 602
		}
	}


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
		local greater_than = strpos(`"`if'"', ">") > 0
		if (`greater_than') {
			di as error "pq will interpret > as in SQL, which is different than Stata."
			di as error "	It will not include . as > any value."
		}

		plugin call polars_parquet_plugin, if `"`if'"'
	}
	else {
		local sql_if
	}
	
	
	
	local offset = max(0,`offset' - 1)
	
	local overwrite_partition = "`nopartitionoverwrite'" == ""
	local b_compress = "`compress'" != ""
	local b_compress_string_to_numeric = "`compress_string_to_numeric'" != ""

	di `"plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' "`sql_if'" "`StataColumnInfo'" "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric'"'
	plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' `"`sql_if'"' `"`StataColumnInfo'"' "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric'
end




capture program drop pq_register_plugin
program pq_register_plugin

	//	di "PLUGIN CHECK"
	capture plugin call polars_parquet_plugin, setup_check ""
	
	if (_rc > 0) {
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
		program polars_parquet_plugin, plugin using("`parquet_path'/pq.`plugin_extension'")
	}
end


capture program drop pq_process_strl
program pq_process_strl
	version 16.0

	syntax , 	path(string)			///
				name(varname)			///
				var_index(integer)		///
				first(integer)			///
				last(integer)

	local first = max(`first',1)
	//	di `"mata: read_strl_block("`path'", `var_index', `first', `last')"'

	mata: read_strl_block("`path'", `var_index', `first', `last')
	capture erase "`pathi'"
end

mata:
	void read_strl_block(string scalar path,
						 real scalar var_index,
						 real scalar first,
						 real scalar last) {
		strl_values = cat(path)
		st_sstore(first::last,var_index,strl_values)		
	}

end
