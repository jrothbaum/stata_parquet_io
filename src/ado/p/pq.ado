*! pq - read/write parquet files with stata
*! Version 4.0.2 - Allow limit core usage with pq set_threads
*!         4.0.1 - Add Stata metadata round-tripping (variable/value labels, notes, formats,
*!                 characteristics) through `pq save`/`pq use`. Faster `pq use`: batched variable
*!                 allocation cuts load time up to ~4x on large files and ~7x on wide files
*!                 (see benchmarks.md).  Add metadata roundtrip of stata types.
*!         3.0.9 - Add safe_int64 option: error (naming columns) when Int64/UInt64 values exceed
*!                 +/-2^53 and would silently lose precision as a Stata double; safe_int64 auto-loads
*!                 the affected columns as strings instead of erroring.
*!                 Also fix regression to properly read columns with names longer than 32 characters.
*!         3.0.8 - Fix overflow/null regression (cast to larger type to avoid losing values to reserved nulls)
*!         3.0.7 - Fix for write subset of variables, support for arrow extension types
*!				   Fix for relaxed on directory read
*!				   Auto infer file format on pq read-like
*!         3.0.6 - Update underlying rust library for better SPSS write compatibility
*!         3.0.5 - Fix random_n() off-by-one; fix pq use with wide varlists hitting Stata's plugin-call
*!                 string limit; aggressive memory return on Linux for HPC/cgroup environments. Fix for weird label char edge case
*!         3.0.0 - Add robust SPSS/CSV round-trip support, faster CSV read/write than native stata CSV
*! 				   faster SAS reads.
*!				   Better handling reads of strl columns, handle datasets that exceed the limit
*!				   of Stata's C plugin API.  Better options for low-memory parquet writes
*!         2.0.0 - Fix float32 compress, improve strL (string) load, allow large file load/save
*! 		   1.9.1 - Fix parquet->stata integer cast overflow bug
*!         1.9.0 - Vastly simplified use/append code to make it easier to manage and debug.  No change to API/function signature or functionality
*! 		   1.8.0 - Fix pq append for subsets of variables, add settable batch_size *
*! 		   1.7.4 - fix str length bug for special characters (str lengths is number of bytes not characters) *
*! 		   1.7.3 - Minor change to saves with partition and compress - don't downcast to boolean to avoid a=true/a=false columns (so it's a=1/a=0)*
*! 	       1.7.2 - Fix overzealous compress on parquet use (to respect stata's odd integer limits) *
*! 	       1.7.1 - fix bug where variables that contain another variable in them not loading with *
*!  	   1.7.0 - upgrade to rust polars 0.49, add option to save labels rather than numeric value

capture program drop pq
program define pq
	gettoken todo 0: 0
    local todo `todo'

    if ("`todo'" == "use") {
		//	di `"pq_use_append `0'"'
		//	pq_use_append `0'
		pq_use_append `0'
    }
	else if ("`todo'" == "use_sas") {
		pq_use_sas `0'
	}
	else if ("`todo'" == "use_spss") {
		pq_use_spss `0'
	}
	else if ("`todo'" == "use_csv") {
		pq_use_csv `0'
	}
	else if ("`todo'" == "append") {
		//	di `"pq_use_append `0' append"'
		if strpos(`"`0'"', ",") > 0 {
			// Already has options, just add append
			pq_use_append `0' append
		}
		else {
			// No options yet, need to add comma before append
			pq_use_append `0', append
		}
    }
	else if ("`todo'" == "merge") {
		//	di `"pq_merge `0'"'
		pq_merge `0'
    }
	else if ("`todo'" == "merge_sas") {
		pq_merge_sas `0'
	}
	else if ("`todo'" == "merge_spss") {
		pq_merge_spss `0'
	}
	else if ("`todo'" == "merge_csv") {
		pq_merge_csv `0'
	}
    else if ("`todo'" == "save") {
		//	di `"pq_save `0'"'
        pq_save `0'
    }
    else if ("`todo'" == "save_spss") {
        pq_save_spss `0'
    }
    else if ("`todo'" == "save_csv") {
        pq_save_csv `0'
    }
    else if ("`todo'" == "describe") {
		//	di `"pq_describe `0'"'
        pq_describe `0'
    }
	else if ("`todo'" == "describe_sas") {
		pq_describe_sas `0'
	}
	else if ("`todo'" == "describe_spss") {
		pq_describe_spss `0'
	}
	else if ("`todo'" == "describe_csv") {
		pq_describe_csv `0'
	}
	else if ("`todo'" == "path") {
		//	di `"pq_convert_path `0'"'
		pq_convert_path `0'
	}
	else if ("`todo'" == "metadata") {
		pq_metadata `0'
	}
	else if ("`todo'" == "set_threads") {
		pq_set_threads `0'
	}
    else {
        disp as err `"Unknown sub-comand `todo'"'
        exit 198
    }
end

capture program drop pq_use_sas
program define pq_use_sas
	if strpos(`"`0'"', ",") > 0 {
		pq_use_append `0' format(sas)
	}
	else {
		pq_use_append `0', format(sas)
	}
end

capture program drop pq_use_spss
program define pq_use_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_use_append `0' format(spss)
	}
	else {
		pq_use_append `0', format(spss)
	}
end

capture program drop pq_use_csv
program define pq_use_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_use_append `0' format(csv)
	}
	else {
		pq_use_append `0', format(csv)
	}
end

capture program drop pq_save_spss
program define pq_save_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_save `0' format(spss)
	}
	else {
		pq_save `0', format(spss)
	}
end

capture program drop pq_save_csv
program define pq_save_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_save `0' format(csv)
	}
	else {
		pq_save `0', format(csv)
	}
end

capture program drop pq_describe_sas
program define pq_describe_sas
	if strpos(`"`0'"', ",") > 0 {
		pq_describe `0' format(sas)
	}
	else {
		pq_describe `0', format(sas)
	}
end

capture program drop pq_describe_spss
program define pq_describe_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_describe `0' format(spss)
	}
	else {
		pq_describe `0', format(spss)
	}
end

capture program drop pq_describe_csv
program define pq_describe_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_describe `0' format(csv)
	}
	else {
		pq_describe `0', format(csv)
	}
end

capture program drop pq_merge_sas
program define pq_merge_sas
	if strpos(`"`0'"', ",") > 0 {
		pq_merge `0' format(sas)
	}
	else {
		pq_merge `0', format(sas)
	}
end

capture program drop pq_merge_spss
program define pq_merge_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_merge `0' format(spss)
	}
	else {
		pq_merge `0', format(spss)
	}
end

capture program drop pq_merge_csv
program define pq_merge_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_merge `0' format(csv)
	}
	else {
		pq_merge `0', format(csv)
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
		sort(string)			///
		compress				///
		compress_string_to_numeric	///
			random_n(integer 0)		///
			random_share(real 0.0)	///
			random_seed(integer 0)	///
			batch_size(string)	///
			infer_schema_length(integer 10000)	///
			parse_dates				///
			preserve_order			///
			drop(string)			///
			drop_strl					///
			format(string)			///
		]



	pq_convert_path `"`using'"'
	local using = r(fullpath)
	if "`keepusing'" != "" {
		if ("`varlist_n'" == "_n")	local using_vars `keepusing'
		else 						local using_vars `varlist' `keepusing' 
	}
	else {
		local using_vars
	}
	local format_opt
	if ("`format'" != "") local format_opt format(`format')
	local batch_size_opt
	if ("`batch_size'" != "") local batch_size_opt batch_size(`batch_size')

	tempfile t_save
	tempname f_pq
	frame create `f_pq'
	frame `f_pq' {
		pq use `using_vars' using `"`using'"', 	clear in(`in') 					///
												if(`if') 						///
												`relaxed' 						///
												asterisk_to_variable(`asterisk_to_variable')	///
																			sort(`varlist')					///
												`compress'						///
												`compress_string_to_numeric'	///
												random_n(`random_n')			///
												random_share(`random_share')	///
												random_seed(`random_seed')		///
												`batch_size_opt'				///
												infer_schema_length(`infer_schema_length')	///
												`parse_dates'				///
												`preserve_order'				///
												`format_opt'					///
												drop(`drop')					///
												`drop_strl'
		quietly save `t_save'
		//	sum
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


capture program drop pq_use_append
program pq_use_append
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
										sort(string)			///
						compress				///
						compress_string_to_numeric	///
						clear					///
						random_n(integer 0)		///
						random_share(real 0.0)	///
						random_seed(integer 0)	///
						infer_schema_length(integer 10000)	///
						parse_dates				///
						batch_size(string)	///
						max_obs_per_batch(integer 0)	///
						preserve_order			///
						drop(string)			///
						drop_strl				///
						format(string)			///
						fast					///
						append				///
						cast(string asis)		///
						lax				///
						safe_int64			///
						binary_to_string	///
						NOSTATAMETADATA	///
						metadata_only]

	local pq_namelist_buf `"`namelist'"'
		
	pq_register_plugin
	
	pq_convert_path `"`using'"'
	local using = r(fullpath)
	pq_infer_format, path("`using'") format("`format'")
	local source_format = r(format)
	if !inlist("`source_format'", "parquet", "sas", "spss", "csv") {
		display as error `"Unsupported format(`format'): expected parquet, sas, spss, or csv"'
		exit 198
	}
	
	local b_append = "`append'" != ""

	//	Snapshot of variables that existed before this call, so statametadata
	//	restoration on append only touches newly created variables and never
	//	overwrites labels/value-labels/notes a user already set on existing
	//	ones. Empty for a plain (non-append) use, so every loaded variable
	//	counts as "new" there.
	local pq_meta_preexisting_vars
	if (`b_append') {
		unab pq_meta_preexisting_vars : _all
	}

	//	metadata_only skips the whole data-read pipeline below and just
	//	applies the footer's Stata metadata to whatever is already loaded,
	//	matched by variable name. `local' scope in Stata is per-program, not
	//	per-brace-block, so the pq_meta_* macros this plugin call stages,
	//	and the locals the skipped branch would otherwise have set
	//	(rename_list, pq_meta_preexisting_vars - both left empty/undefined
	//	here), are still exactly what the shared apply block at the end of
	//	this program expects.
	if ("`metadata_only'" != "") {
		if (`b_append') {
			display as error "metadata_only may not be combined with append"
			exit 198
		}
		if ("`nostatametadata'" != "") {
			display as error "metadata_only may not be combined with nostatametadata"
			exit 198
		}
		if ("`source_format'" != "parquet") {
			display as error "metadata_only is only supported for Parquet input"
			exit 198
		}
		if (`=_N' == 0) {
			display as error "No data in memory for metadata_only to apply to"
			exit 2000
		}
		plugin call polars_parquet_plugin, describe_stata_metadata "`using'" 1
	}
	else {

	//	Pre-flight: validate embedded Stata metadata (conflicting/mixed/
	//	malformed footers across a directory or glob) BEFORE clearing the
	//	currently loaded dataset. Without this, a bad target directory
	//	would wipe the caller's existing data via `clear' below and only
	//	then fail deep inside the "read" plugin call, losing data neither
	//	the old nor the new dataset. Not run for metadata_only (handled in
	//	its own branch above, before any clear) or when the caller opted
	//	out entirely with nostatametadata.
	if ("`nostatametadata'" == "" & "`source_format'" == "parquet") {
		plugin call polars_parquet_plugin, describe_stata_metadata "`using'" 1
	}

	if (!`b_append' & "`clear'" != "")	clear
	if (`=_N' > 0 & !`b_append') {
		display as error "There is already data loaded, pass clear if you want to load a file"
		exit 2000
	}

	if (`random_share' > 1) {
		display as error `"Cannot set random_share > 1 (`random_share')"'
		exit 198
	}

	if (`infer_schema_length' < 0) {
		display as error `"infer_schema_length() must be >= 0, passed `infer_schema_length'"'
		exit 198
	}
	if ("`batch_size'" != "") {
		capture confirm integer number `batch_size'
		if (_rc) {
			display as error `"batch_size() must be a positive integer, passed `batch_size'"'
			exit 198
		}
		local batch_size_num = real("`batch_size'")
		if (`batch_size_num' <= 0) {
			display as error `"batch_size() must be > 0, passed `batch_size'"'
			exit 198
		}
	}

	if ("`source_format'" != "parquet") {
		if ("`relaxed'" != "") {
			display as error "relaxed is only supported for parquet input"
			exit 198
		}
		if ("`asterisk_to_variable'" != "") {
			display as error "asterisk_to_variable() is only supported for parquet input"
			exit 198
		}
	}

	local b_preserve_order = "`preserve_order'" != ""
	if (`b_preserve_order' & !inlist("`source_format'", "sas", "spss")) {
		di as text "note: preserve_order ignored for format(`source_format'); only used for sas/spss reads."
		local b_preserve_order = 0
	}
	local b_parse_dates = "`parse_dates'" != ""
	pq_normalize_csv_opts, source_format(`source_format') infer_schema_length(`infer_schema_length') b_parse_dates(`b_parse_dates')
	local infer_schema_length_for_plugin = r(infer_schema_length_for_plugin)
	local parse_dates_for_plugin = r(parse_dates_for_plugin)
	local b_fast = "`fast'" != ""
	local batch_size_for_plugin -1
	if ("`batch_size'" != "") local batch_size_for_plugin = real("`batch_size'")

	// Set default for max_obs_per_batch if not specified
	if (`max_obs_per_batch' == 0) {
		local max_obs_per_batch = 2147483647  // i32::MAX
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
		//	Detect Stata date functions for parquet only.
		if ("`source_format'" == "parquet") {
			if (regexm(`"`if'"', "t[cdwmqhC]\(")) {
				di as error "if() expression contains a Stata date function (td, tc, tC, tw, tm, tq, or th)."
				di as error "Parquet dates use Unix epoch (01jan1970); Stata date functions use 01jan1960."
				di as error "Use Polars date/datetime literals instead, e.g.:"
				di as error `"  %td (daily date): if(date_col >= date('01jan2020','%d%b%Y'))"'
				di as error `"  %tc (datetime):   if(dt_col >= TIMESTAMP '2020-01-01 00:00:00')"'
				exit 198
			}
		}
		local greater_than = strpos(`"`if'"', ">") > 0
		if (`greater_than') {
			di as error "pq will interpret > as in SQL, which is different than Stata."
			di as error "	It will not include . as > any value."
		}
		//	di `"plugin call polars_parquet_plugin, if "`if'""'
		plugin call polars_parquet_plugin, if `"`if'"'
		if ("`sql_if'" != "" & inlist("`source_format'", "sas", "spss", "csv")) {
			di as text "note: sql_if on `source_format' currently scans source data twice (describe + read); this can be slow on large files."
		}
	}
	else {
		local sql_if
	}
	
	//	Initialize "mapping" to tell plugin to read from macro variables
	local mapping from_macros
	local b_quiet = 1
	local b_detailed = 1
	local b_compress = "`compress'" != ""
	local b_compress_string_to_numeric = "`compress_string_to_numeric'" != ""
	
	//	di `"plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "`sql_if'" "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric'"'
	// Rust resolves wildcards and applies drop() inside file_summary(), then sets
	// matched_vars. drop_strl columns (binary parquet type) are filtered below.
	local b_binary_to_string = ("`binary_to_string'" != "")
	local b_cast_strict = ("`lax'" == "")
	local b_safe_int64 = ("`safe_int64'" != "")
	local pq_cast_buf `cast'
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' `"`sql_if'"' "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric' "`source_format'" `infer_schema_length_for_plugin' `parse_dates_for_plugin' `b_fast' 100 "pq_namelist_buf" "`drop'" "pq_cast_buf" `b_binary_to_string' `b_cast_strict' `b_safe_int64'
	if (_rc) {
		if (`"`pq_cast_error'"' != "") di as error "`pq_cast_error'"
		exit _rc
	}

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

	// matched_vars is set by the Rust describe plugin (wildcards expanded, drop applied).
	// Handle drop_strl separately: remove binary ("strl") columns identified from schema.
	if "`drop_strl'" == "drop_strl" {
		local strl_drop_list
		forvalues i = 1/`n_columns' {
			if "`type_`i''" == "strl" {
				local strl_drop_list `strl_drop_list' `name_`i''
			}
		}
		if "`strl_drop_list'" != "" {
			local new_matched
			foreach vari in `matched_vars' {
				if !`:list vari in strl_drop_list' {
					local new_matched `new_matched' `vari'
				}
			}
			local matched_vars `new_matched'
		}
	}

	local match_all = ("`namelist'" == "" | "`namelist'" == "*") & "`drop'" == ""
	
	//	Get the list of already existing variables
	capture unab all_vars: *
	local n_vars_already : word count `all_vars'
	
	//	Create the empty data, if needed, or add rows, if needed
	if (`last_n' == 0)	local last_n = `n_rows'
	local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))

	if (`random_n' > `row_to_read') {
		di "random_n (`random_n') > number of rows to read (`row_to_read')"
	}

	if (`random_n' > 0 & `random_n' < `row_to_read') {
		local random_share = `random_n'/`row_to_read'
		local row_to_read = `random_n'
	}
	else if (`random_share' > 0)						local row_to_read = floor(`random_share'*`row_to_read')

	//	di "local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))"

	local n_obs_already = _N

	//	Build list of strL column names from describe output.
	//	A column is treated as strL if ANY of:
	//	  (a) parquet describe says type is "strl"
	//	  (b) parquet max string length > 2045 bytes (DTA_MAX_STR)
	//	  (c) the existing Stata variable is already strL (for append)
	local strl_col_names
	local non_strl_matched_vars
	foreach vari in `matched_vars' {
		local var_number: list posof "`vari'" in vars_in_file
		local typei `type_`var_number''
		local str_len_i `string_length_`var_number''

		//	Check if existing Stata variable is strL (Stata returns "strL" with capital L)
		local ti
		capture confirm variable `vari', exact
		if _rc == 0 {
			local ti : type `vari'
		}

		if (("`typei'" == "strl") | (`str_len_i' > 2045) | (lower("`ti'") == "strl")) {
			local strl_col_names `strl_col_names' `vari'
		}
		else {
			local non_strl_matched_vars `non_strl_matched_vars' `vari'
		}
	}

	//	Tell polars to concatenate file list with "vertical_relaxed"
	local vertical_relaxed = "`relaxed'" != ""
	local offset_for_plugin = max(0,`offset' - 1)

	//	Check if batching is needed due to observation limit
	local needs_batching = (`row_to_read' > `max_obs_per_batch')

	if (`needs_batching' & `random_share' > 0) {
		di as error "random_n/random_share is not supported when the dataset exceeds `max_obs_per_batch' rows (overflow batching)."
		exit 198
	}

	if (`needs_batching') {
		//	Large dataset detected - split into two batches
		display as text "Large dataset detected: `row_to_read' rows > `max_obs_per_batch' limit"
		display as text "Processing in 2 batches..."

		//	BATCH 1: First max_obs_per_batch rows via normal flow
		local row_to_read_first = `max_obs_per_batch'
		local original_row_to_read = `row_to_read'
		local row_to_read = `row_to_read_first'
	}

	//	Handle strL columns via .dta if any exist
	//	The strl .dta is written as a side effect of the read plugin call below,
	//	so sampling is guaranteed consistent between strl and non-strl columns.
	local has_strl = "`strl_col_names'" != ""
	local temp_strl_dta
	if (`has_strl') {
		tempfile temp_strl_tmp
		local temp_strl_dta : subinstr local temp_strl_tmp ".tmp" ".dta", all
		if ("`temp_strl_dta'" == "`temp_strl_tmp'") {
			local temp_strl_dta "`temp_strl_tmp'.dta"
		}
	}

	//	Detect all-strL append: when every matched variable is strL, the plugin
	//	writes only temp_strl_dta (no Stata matrix writes) so we skip `set obs'.
	//	Blob collision is prevented by the Rust writer using n_obs_already as an
	//	offset for the `o` identifier, so the new blobs start at n_obs_already+1.
	local n_strl_matched: word count `strl_col_names'
	local n_matched_total: word count `matched_vars'
	local all_strl_append = (`b_append' & `n_matched_total' > 0 & `n_matched_total' == `n_strl_matched')

	local n_obs_after = `n_obs_already' + `row_to_read'

	//	Declare every NEW variable via Mata (st_addvar) before `set obs'
	//	extends the dataset, instead of one `gen' per variable afterward -
	//	each `gen' would redundantly refill all N rows again. Pre-existing
	//	variables (append/recast) still go through the loop below unchanged.
	if (!`all_strl_append') {
		local pq_batch_new_names
		local pq_batch_new_types
		foreach vari in `matched_vars' {
			local var_number: list posof "`vari'" in vars_in_file
			local type `type_`var_number''
			local string_length `string_length_`var_number''

			local name_to_create `vari'
			forvalues i = 1/`n_renamed' {
				local rename_from `rename_from_`i''
				if ("`vari'" == "`rename_from'") {
					local name_to_create `rename_to_`i''
					continue, break
				}
			}

			local is_strl_col : list posof "`vari'" in strl_col_names
			if ("`type'" == "strl" | `is_strl_col' > 0 | "`type'" == "binary") {
				continue
			}

			capture confirm variable `name_to_create', exact
			if (_rc == 0) continue

			local mata_type `type'
			if ("`mata_type'" == "datetime" | "`mata_type'" == "time") {
				local mata_type double
			}
			else if ("`mata_type'" == "date") {
				local mata_type long
			}
			else if ("`mata_type'" == "string") {
				local mata_str_len = max(1,`string_length')
				local mata_type str`mata_str_len'
			}

			local pq_batch_new_names `pq_batch_new_names' `name_to_create'
			local pq_batch_new_types `pq_batch_new_types' `mata_type'
		}
		if ("`pq_batch_new_names'" != "") {
			mata: _pq_batch_addvar("`pq_batch_new_names'", "`pq_batch_new_types'")
		}

		quietly set obs `n_obs_after'
	}

	local match_vars_non_binary

	local dropped_vars = 0

	local var_position = 0
	local rename_count = 0
	local rename_list
	foreach vari in `matched_vars' {
		local var_position = `var_position' + 1
		local var_number: list posof "`vari'" in vars_in_file
		local type `type_`var_number''
		local string_length `string_length_`var_number''

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

		//	Skip columns designated as strL - they are loaded via .dta.
		//	This includes parquet type "strl" and promoted long strings.
		local is_strl_col : list posof "`vari'" in strl_col_names
		if ("`type'" == "strl" | `is_strl_col' > 0) {
			//	Handle renames for strL columns loaded from .dta
			if ("`rename_to'" != "") {
				capture confirm variable `vari', exact
				if (_rc == 0) {
					rename `vari' `name_to_create'
				}
				local rename_list `rename_list' `name_to_create'
				local rename_count = `rename_count' + 1
				local rename_from_`rename_count' `vari'
				//	Rename bookkeeping lives in a characteristic, not the
				//	variable label, so statametadata's real label can
				//	coexist with a later `pq save' round-tripping this
				//	(possibly >32 char / invalid) original Parquet name.
				char `name_to_create'[_pq_parquet_name] `"`vari'"'
			}
			//	Don't add strL to match_vars_non_binary - they're not sent to read plugin
			continue
		}

		pq_gen_or_recast,	name(`name_to_create')			///
							type_new(`type')				///
							str_length(`string_length')

		local keep = 1

		if ("`type'" == "datetime") {
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
			local rename_list `rename_list' `name_to_create'
			local rename_count = `rename_count' + 1
			local rename_from_`rename_count' `vari'

			//	Rename bookkeeping lives in a characteristic, not the
			//	variable label, so statametadata's real label can
			//	coexist with a later `pq save' round-tripping this
			//	(possibly >32 char / invalid) original Parquet name.
			char `name_to_create'[_pq_parquet_name] `"`vari'"'
		}

		if (`keep') {
			local match_vars_non_binary `match_vars_non_binary' `vari'
		}
	}


	//	Make a list of the loaded variables (excluding strL)
	local n_matched_vars: word count `match_vars_non_binary'

	local i = 0
	if `n_matched_vars' > 0 foreach vari of varlist * {
		//	Actual variable index
		local i = `i' + 1

		//	vari is the final name, but if it was renamed, we
		//		need to get the original value to get the index
		//		of the variable in the original list
		local i_rename : list posof "`vari'" in rename_list
		if (`i_rename' > 0)		local vari_original `rename_from_`i_rename''
		else					local vari_original `vari'


		//	Index of the actual new variables (possible != i for append)
		local i_matched : list posof "`vari_original'" in match_vars_non_binary

		if (`i_matched' > 0) {
			local v_to_read_index_`i_matched' `i'
			//	Name used to look up the column in the Polars batch, which always
			//	keeps the original (possibly >32 char) parquet column name - not
			//	the (possibly truncated/renamed) Stata variable name `vari'.
			local v_to_read_name_`i_matched' `vari_original'
			local v_to_read_type_`i_matched': type `vari'
			local v_to_read_type_`i_matched' = lower("`v_to_read_type_`i_matched''")
			//	For getting the polars and polars assigned stata type and passing back to read
			local i_original : list posof "`vari_original'" in vars_in_file


			//	Get the originally set stata type
			local v_to_read_type_`i_matched' `type_`i_original''
			//	Get the polars type from the earlier list
			local v_to_read_p_type_`i_matched' `polars_type_`i_original''

			//	display "`v_to_read_index_`i_matched'': `v_to_read_name_`i_matched'', `v_to_read_type_`i_matched'', `v_to_read_p_type_`i_matched''"
		}
	}

	local offset = `offset_for_plugin'

	//	asterisk_to_variable - for files /file/*.parquet, convert
	//		* to a variable, so /file/2019.parquet, file/2020.parquet
	//		will have the item in asterisk_to_variable as 2019 and 2020
	//		for the records on the file

	//	strl col names and dta path are passed so the plugin writes the strl .dta
	//	in the same scan as the non-strl columns (consistent sampling)
	local b_skip_metadata = ("`nostatametadata'" != "")
	capture noisily plugin call polars_parquet_plugin, read "`using'" "from_macro" `row_to_read' `offset' `"`sql_if'"' `"`mapping'"' `vertical_relaxed' "`asterisk_to_variable'" "`sort'" `n_obs_already' `random_share' `random_seed' `batch_size_for_plugin' "`strl_col_names'" "`temp_strl_dta'" "`source_format'" `b_preserve_order' `infer_schema_length_for_plugin' `parse_dates_for_plugin' "" `b_skip_metadata'
	local _read_rc = _rc
	if (`_read_rc') {
		if (`b_append' & !`all_strl_append' & `n_obs_already' < _N) {
			quietly keep in 1/`n_obs_already'
		}
		if (`"`pq_cast_error'"' != "") di as error "`pq_cast_error'"
		exit `_read_rc'
	}

	//	Merge strL columns from .dta written by plugin into the current dataset
	//	The .dta always contains _pq_strl_key (1-based row index) added by Rust.
	if (`has_strl') {
		if (`n_matched_vars' == 0) {
			//	All variables are strL; `set obs' was skipped for the append path.
			if (!`b_append') {
				//	Non-append: dataset is empty, load the strL .dta directly.
				quietly use "`temp_strl_dta'", clear
			}
			else {
				//	All-strL append: blob `o` values in temp_strl_dta are offset by
				//	n_obs_already (done in Rust), so they can't collide with the master.
				quietly append using "`temp_strl_dta'"
			}
			capture erase "`temp_strl_dta'"
		}
		else if (!`b_append') {
			//	Mixed strL + non-strL, non-append: gen key=_n, merge, drop key.
			quietly capture drop _pq_strl_key
			quietly gen long _pq_strl_key = _n
			quietly merge 1:1 _pq_strl_key using "`temp_strl_dta'", nogen
		}
		else {
			//	Mixed strL + non-strL, append: gen key=_n, merge update, drop key.
			quietly capture drop _pq_strl_key
			quietly gen long _pq_strl_key = _n
			quietly merge 1:1 _pq_strl_key using "`temp_strl_dta'", update nogen
		}
		//	Drop the key column that Rust added (present in all paths after use/merge/append)
		quietly capture drop _pq_strl_key
		capture erase "`temp_strl_dta'"

		//	Restore original column order
		local ordered_vars
		foreach vari in `matched_vars' {
			local rname
			forvalues ri = 1/`rename_count' {
				if ("`vari'" == "`rename_from_`ri''") {
					local pos : list posof "`rename_from_`ri''" in rename_list
					if (`pos' > 0) {
						local rname : word `pos' of `rename_list'
					}
				}
			}
			if ("`rname'" != "") {
				local ordered_vars `ordered_vars' `rname'
			}
			else {
				local ordered_vars `ordered_vars' `vari'
			}
		}
		capture order `ordered_vars'
	}

	//	BATCH 2: Overflow rows via .dta append (if batching was needed)
	if (`needs_batching') {
		display as text "Batch 1 complete. Processing overflow batch..."

		//	Calculate overflow parameters
		local overflow_offset = `offset_for_plugin' + `max_obs_per_batch'
		local overflow_count = `original_row_to_read' - `max_obs_per_batch'

		//	Create temp file for overflow .dta
		tempfile temp_overflow_tmp
		local temp_overflow_dta : subinstr local temp_overflow_tmp ".tmp" ".dta", all
		if ("`temp_overflow_dta'" == "`temp_overflow_tmp'") {
			local temp_overflow_dta "`temp_overflow_tmp'.dta"
		}

		//	Build column list for overflow (all matched vars, including strL)
		local overflow_columns `matched_vars'

		//	Set up relax option for overflow call
		if ("`relaxed'" != "") {
			local relax_opt "relax"
		}
		else {
			local relax_opt ""
		}

		//	Call helper to write overflow batch to .dta
		pq_write_overflow_dta, using("`using'") output("`temp_overflow_dta'") ///
			offset(`overflow_offset') n_rows(`overflow_count') ///
			columns("`overflow_columns'") if_clause(`"`sql_if'"') ///
			`relax_opt' asterisk_to_variable("`asterisk_to_variable'") ///
			random_share(`random_share') random_seed(`random_seed') format(`source_format') ///
			infer_schema_length(`infer_schema_length_for_plugin') ///
			parse_dates(`parse_dates_for_plugin')

		//	Append the overflow .dta
		quietly append using "`temp_overflow_dta'"
		capture erase "`temp_overflow_dta'"

		display as text "Overflow batch complete. Total rows loaded: `=_N'"
	}

	}	//	end of the "not metadata_only" branch opened above

	//	Apply Stata label/format metadata that the plugin staged as indexed
	//	pq_meta_* macros while reading the footer (see stata_metadata.rs).
	//	macval() is used at every point free-text metadata lands in a
	//	command, since a plain `macroname' dereference re-scans the
	//	substituted text for backtick/$ sequences and would corrupt labels
	//	or notes that happen to contain them.
	if ("`nostatametadata'" == "" & "`pq_meta_present'" == "1") {
		local pq_meta_names_all
		forvalues j = 1/`pq_meta_count' {
			local pq_meta_names_all `pq_meta_names_all' `pq_meta_name_`j''
		}

		//	Value-label definitions are staged indexed by a small integer
		//	`m' (see _pq_stage_vallabel_defn), not by name - build the
		//	name -> m lookup once.
		local pq_meta_vallabel_names_all
		forvalues m = 1/`pq_meta_vallabel_count' {
			local pq_meta_vallabel_names_all `pq_meta_vallabel_names_all' `pq_meta_vallabel_name_`m''
		}

		local pq_meta_vallabels_applied
		foreach vari of varlist * {
			//	On append, only newly created variables get metadata applied -
			//	a variable that already existed keeps whatever label/value
			//	label/notes the user already had on it.
			local i_preexisting : list posof "`vari'" in pq_meta_preexisting_vars
			if (`i_preexisting' > 0) continue

			local i_rename : list posof "`vari'" in rename_list
			if (`i_rename' > 0)		local vari_original `rename_from_`i_rename''
			else					local vari_original `vari'

			local j : list posof "`vari_original'" in pq_meta_names_all
			if (`j' == 0) continue

			//	Rename bookkeeping now lives in the _pq_parquet_name
			//	characteristic, not the label (see pq_use_append and
			//	pq_save), so the real label can always be restored here,
			//	including for a variable that was auto-renamed for a long
			//	or invalid Parquet name.
			if (`"`macval(pq_meta_label_`j')'"' != "") {
				label variable `vari' `"`macval(pq_meta_label_`j')'"'
			}

			local varformat `pq_meta_format_`j''
			if ("`varformat'" != "") {
				capture noisily format `vari' `varformat'
			}

			local vlname `pq_meta_vallabel_`j''
			if ("`vlname'" != "") {
				local already : list posof "`vlname'" in pq_meta_vallabels_applied
				if (`already' == 0) {
					local pq_meta_vallabels_applied `pq_meta_vallabels_applied' `vlname'
					local m : list posof "`vlname'" in pq_meta_vallabel_names_all
					if (`m' > 0) {
						capture label drop `vlname'
						local defn_count `pq_meta_vallabel_defn_count_`m''
						forvalues k = 1/`defn_count' {
							label define `vlname' `pq_meta_vallabel_defn_code_`m'_`k'' ///
								`"`macval(pq_meta_vallabel_defn_text_`m'_`k')'"', add
						}
					}
				}
				label values `vari' `vlname'
			}

			local note_count `pq_meta_note_`j'_count'
			if ("`note_count'" != "0" & "`note_count'" != "") {
				quietly notes drop `vari'
				forvalues k = 1/`note_count' {
					notes `vari': `"`macval(pq_meta_note_`j'_`k')'"'
				}
			}
		}

		//	Dataset-level label/notes: left alone on append, matching the
		//	existing dataset rather than overwriting it from one file.
		if (!`b_append') {
			label data `"`macval(pq_meta_dataset_label)'"'

			local dnote_count `pq_meta_dataset_note_count'
			if ("`dnote_count'" != "0" & "`dnote_count'" != "") {
				quietly notes drop _dta
				forvalues k = 1/`dnote_count' {
					notes: `"`macval(pq_meta_dataset_note_`k')'"'
				}
			}
		}
	}
end

//	Declares every variable in `names_sp' with the parallel type in
//	`types_sp' via st_addvar - one Mata call instead of one `gen' per
//	variable. The caller still owns the single `set obs' that follows.
capture mata: mata drop _pq_batch_addvar()
mata:
void _pq_batch_addvar(string scalar names_sp, string scalar types_sp)
{
	string rowvector names, types
	real scalar i

	names = tokens(names_sp)
	types = tokens(types_sp)
	for (i = 1; i <= cols(names); i++) {
		(void) st_addvar(types[i], names[i])
	}
}
end

//	Widens a numeric variable's storage type via a Mata view-copy instead of
//	`recast' - ~3x faster at scale, same reasoning as the `gen' fix above.
capture mata: mata drop _pq_widen_numeric()
mata:
void _pq_widen_numeric(string scalar oldname, string scalar newtype, string scalar newname)
{
	real colvector vold
	real colvector vnew

	st_view(vold, ., oldname)
	(void) st_addvar(newtype, newname)
	st_view(vnew, ., newname)
	vnew[.,.] = vold
}
end

//	Unlike `recast' (in-place), this creates a new variable and swaps it
//	into `name', so nothing about the old one carries over automatically.
//	Format, variable label, value label, column position, and every
//	characteristic (notes included - they're just characteristics under
//	the hood) are captured before the swap and reapplied after.
capture program drop pq_fast_recast
program pq_fast_recast
	version 16
	syntax, name(string) newtype(string)

	local orig_fmt: format `name'
	local orig_varlabel: variable label `name'
	local orig_vallabel: value label `name'

	//	Values must be read from the OLD variable before it is dropped -
	//	`: char name[]' only gives the key names.
	local orig_char_names : char `name'[]
	local n_chars : word count `orig_char_names'
	forvalues c = 1/`n_chars' {
		local cnm_`c' : word `c' of `orig_char_names'
		local cval_`c' : char `name'[`cnm_`c'']
	}

	//	`order' only touches the variable table, not row data, so restoring
	//	it here isn't the O(N) cost this routine exists to avoid.
	unab pq_fr_all_vars : _all

	tempvar newv
	mata: _pq_widen_numeric("`name'", "`newtype'", "`newv'")

	quietly drop `name'
	quietly rename `newv' `name'
	quietly order `pq_fr_all_vars'

	if ("`orig_fmt'" != "") {
		capture format `name' `orig_fmt'
	}
	if (`"`orig_varlabel'"' != "") {
		label variable `name' `"`macval(orig_varlabel)'"'
	}
	if ("`orig_vallabel'" != "") {
		label values `name' `orig_vallabel'
	}
	forvalues c = 1/`n_chars' {
		char `name'[`cnm_`c''] `"`macval(cval_`c')'"'
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
	else if ("`type_new'" == "string")		local type_str str`string_length'
	
	capture confirm variable `name', exact
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
	else if ("`type_new'" == "strl") {
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
	else if ("`type_new'" == "float") {
		if `b_gen' {
			quietly gen float `type' `name' = .
		}
		else {
			if inlist("`vartype'", "long","double") {
				pq_fast_recast, name(`name') newtype(double)
			}
			else if inlist("`vartype'", "byte", "int") {
				pq_fast_recast, name(`name') newtype(float)
			}
		}
	}
	else if ("`type_new'" == "long") {
		if `b_gen' {
			quietly gen long `name' = .
		}
		else {
			if inlist("`vartype'", "byte", "int") {
				pq_fast_recast, name(`name') newtype(long)
			}
			else if inlist("`vartype'", "float") {
				pq_fast_recast, name(`name') newtype(double)
			}
		}
	}
	else if ("`type_new'" == "int") {
		if `b_gen' {
			quietly gen int `name' = .
		}
		else {
			if inlist("`vartype'", "byte") {
				pq_fast_recast, name(`name') newtype(int)
			}
		}
	}
	else if ("`type_new'" == "byte") {
		if `b_gen' {
			quietly gen byte `name' = .
		}
		else {
			//	Same source and target type by construction - a genuine
			//	no-op left on the native path (nothing for Mata to win).
			if inlist("`vartype'", "int","long","float","double") {
				recast `vartype' `name'
			}
		}
	}
	else if ("`type_new'" == "binary") {
		di "Dropping `name' as cannot process binary columns"
	}
	else {
		if `b_gen' {
			quietly gen double `name' = .
		}
		else {
			if inlist("`vartype'", "byte", "int", "long", "float") {
				pq_fast_recast, name(`name') newtype(double)
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
			 asterisk_to_variable(string) ///
			 format(string)				///
			 infer_schema_length(integer 10000) ///
			 parse_dates]

	pq_register_plugin
	local b_quiet = ("`quietly'" != "")
	local b_detailed = ("`detailed'" != "")
	
	pq_convert_path `"`using'"'
	local using = r(fullpath)
	pq_infer_format, path("`using'") format("`format'")
	local source_format = r(format)
	if !inlist("`source_format'", "parquet", "sas", "spss", "csv") {
		display as error `"Unsupported format(`format'): expected parquet, sas, spss, or csv"'
		exit 198
	}
	if ("`source_format'" != "parquet" & "`asterisk_to_variable'" != "") {
		display as error "asterisk_to_variable() is only supported for parquet input"
		exit 198
	}
	if (`infer_schema_length' < 0) {
		display as error `"infer_schema_length() must be >= 0, passed `infer_schema_length'"'
		exit 198
	}
	local b_parse_dates = "`parse_dates'" != ""
	pq_normalize_csv_opts, source_format(`source_format') infer_schema_length(`infer_schema_length') b_parse_dates(`b_parse_dates')
	local infer_schema_length_for_plugin = r(infer_schema_length_for_plugin)
	local parse_dates_for_plugin = r(parse_dates_for_plugin)

	//	Trailing zeros are compress indicators
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "" "`asterisk_to_variable'" 0 0 "`source_format'" `infer_schema_length_for_plugin' `parse_dates_for_plugin'

	
	local macros_to_return n_rows n_columns //	mapping
	forvalues i = 1/`n_columns' {
		local macros_to_return `macros_to_return' type_`i' name_`i' rename_`i' 
		
		if (`b_detailed')	local macros_to_return `macros_to_return' string_length_`i'
		
	}
	
	foreach maci in `macros_to_return' {
		return local `maci' = `"``maci''"'
	}
end


capture program drop pq_metadata
program pq_metadata, rclass
	version 16.0

	local input_args = `"`0'"'

	local using_pos = strpos(`" `input_args' "', " using ")
	if `using_pos' > 0 {
		local pre_using = substr(`"`input_args'"', 1, `using_pos'-1)
		if `"`pre_using'"' != "" {
			di as error "varlist not allowed"
			error 101
		}
		local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
	}
	else {
		local 0 = `"using `input_args'"'
	}

	syntax using/ [, quietly]

	pq_register_plugin
	local b_quiet = ("`quietly'" != "")

	pq_convert_path `"`using'"'
	local using = r(fullpath)
	pq_infer_format, path("`using'")
	local source_format = r(format)
	if ("`source_format'" != "parquet") {
		display as error "pq metadata is only supported for Parquet input"
		exit 198
	}

	plugin call polars_parquet_plugin, describe_stata_metadata "`using'" `b_quiet'

	return local pq_meta_present `"`pq_meta_present'"'
	if ("`pq_meta_present'" == "1") {
		return local pq_meta_count `"`pq_meta_count'"'
		return local pq_meta_dataset_label `"`macval(pq_meta_dataset_label)'"'
	}
end


//	st_vlload returns exactly the defined (code, text) pairs for a value
//	label, unlike scanning min()/max() in ado, which breaks on sparse or
//	negative codes. st_local() writes straight into the calling program's
//	locals, so no macro re-expansion risk for label text containing
//	backticks or $.
//
//	Staged macros are indexed by the integer `m', not by the value-label
//	name itself: Stata macro *names* are capped at 32 characters (same as
//	variable names), and a name-keyed macro name like
//	pq_meta_vallabel_defn_count_<name> can exceed that for an ordinary
//	label name. pq_meta_vallabel_name_`m' records the name separately.
capture mata: mata drop _pq_stage_vallabel_defn()
mata:
void _pq_stage_vallabel_defn(string scalar vlname, real scalar m)
{
	real colvector values
	string colvector texts
	real scalar i
	string scalar prefix

	prefix = strofreal(m)
	st_local("pq_meta_vallabel_name_" + prefix, vlname)

	st_vlload(vlname, values, texts)
	st_local("pq_meta_vallabel_defn_count_" + prefix, strofreal(rows(values)))
	for (i = 1; i <= rows(values); i++) {
		st_local("pq_meta_vallabel_defn_code_" + prefix + "_" + strofreal(i), strofreal(values[i]))
		st_local("pq_meta_vallabel_defn_text_" + prefix + "_" + strofreal(i), texts[i])
	}
}
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
						   chunk(integer 2147483647)		///
						   stream							///
						   CONSolidate						///
						   DO_not_reload					///
						   label 							///
						   format(string)					///
						   statametadata					///
						   ]	//	in(string)

	if ("`label'" != "" & "`statametadata'" != "") {
		display as error "label and statametadata may not be combined"
		exit 198
	}
        
	//	if "`partition_by'" != "" {
	//		di as error "Hive partitioning not implemented yet"
	//		exit 198
	//	}
	if (!inlist("`compression'", "", "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd")) {
		display as error `"Acceptable options for compression are "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd", and "" ("" will be zstd), passed "`compression'""'
		exit 198
	}
	
	if "`do_not_reload'" != "" & "`stream'" == "" {
		di as text "note: do_not_reload ignored without stream"
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
	
	pq_convert_path `"`using'"'
	local using = r(fullpath)
	pq_infer_format, path("`using'") format("`format'")
	local source_format = r(format)
	if !inlist("`source_format'", "parquet", "spss", "csv") {
		display as error `"Unsupported save format(`format'): expected parquet, spss, or csv"'
		exit 198
	}

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
	unab _all_variables_ordered : _all
	


	local vars_labeled	
	local original_order
	if ("`label'" == "label") {
		quietly ds
		local original_order `r(varlist)'
		
		//	Do any variables have labels?
		foreach vari in `varlist' {
			local labeli : value label `vari'
			if "`labeli'" != "" {
				local vars_labeled `vars_labeled' `vari'
				tempvar `vari'

				//	Move the "true" value to a tempvar
				quietly rename `vari' ``vari''

				//	Create a decoded value in the original variable name
				decode ``vari'', gen(`vari')
				//	tab ``vari'' `vari'
			}
		}

		quietly order `original_order'
	}

	foreach vari in `varlist' {
		local var_count = `var_count' + 1
		local typei: type `vari'
		local formati: format `vari'
		local str_length 0
		
		
		if ((substr("`typei'",1,3) == "str") & (lower("`typei'") != "strl")) {
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
		local col_`var_count' : list posof "`vari'" in _all_variables_ordered
		
		//	Rename?
		if ("`noautorename'" == "") {
			//	Two independent ways to request a rename on save, checked
			//	in this order:
			//	1. The _pq_parquet_name characteristic, set automatically
			//	   by pq_use_append when it had to shorten/sanitize a long
			//	   or invalid Parquet name - kept off the label so
			//	   statametadata's real label can coexist with it.
			//	2. The legacy "{parquet_name:original}" variable-label
			//	   convention - a documented escape hatch a user can set
			//	   by hand (see rename.do) to force ANY variable to save
			//	   back out under a different Parquet column name. Still
			//	   honored for backward compatibility; a variable using
			//	   this form has no real label to preserve regardless.
			local parquet_name_char : char `vari'[_pq_parquet_name]

			if (`"`parquet_name_char'"' != "") {
				local n_rename = `n_rename' + 1
				local rename_from_`n_rename' `vari'
				local rename_to_`n_rename' `"`parquet_name_char'"'
			}
			else {
				local labeli: variable label `vari'
				local labeli: subinstr local labeli "\`" "'", all

				if regexm(`"`labeli'"', "^\{parquet_name:([^}]*)\}") {
					local n_rename = `n_rename' + 1
					local rename_from_`n_rename' `vari'
					local rename_to_`n_rename' = regexs(1)
				}

				//	di "n_rename: `n_rename'"
				//	di "	from: `rename_from_`n_rename''"
				//	di "	to:   `rename_to_`n_rename''"
			}
		}
	}

	//	Stage Stata label/format metadata as indexed macros for the plugin
	//	to read directly (mirrors name_N/dtype_N above) - every saved
	//	variable is staged unconditionally so display-format-only or
	//	unlabelled columns still carry their (empty) entry; the Rust side
	//	skips a variable whose label/value-label/notes are all empty.
	local pq_meta_count = 0
	if ("`statametadata'" != "") {
		local pq_meta_count `var_count'
		local pq_meta_vallabels_built
		local pq_meta_vallabel_count = 0
		local j = 0
		foreach vari in `varlist' {
			local j = `j' + 1

			local pq_meta_name_`j' `vari'
			local pq_meta_label_`j' : variable label `vari'
			local pq_meta_vallabel_`j' : value label `vari'
			local pq_meta_format_`j' : format `vari'

			local note_count : char `vari'[note0]
			if ("`note_count'" == "") local note_count 0
			local pq_meta_note_`j'_count `note_count'
			forvalues k = 1/`note_count' {
				local pq_meta_note_`j'_`k' : char `vari'[note`k']
			}

			local vlname `pq_meta_vallabel_`j''
			if ("`vlname'" != "") {
				local already_built : list posof "`vlname'" in pq_meta_vallabels_built
				if (`already_built' == 0) {
					local pq_meta_vallabels_built `pq_meta_vallabels_built' `vlname'
					local pq_meta_vallabel_count = `pq_meta_vallabel_count' + 1
					mata: _pq_stage_vallabel_defn("`vlname'", `pq_meta_vallabel_count')
				}
			}
		}

		local pq_meta_dataset_label : data label
		local pq_meta_dataset_note_count : char _dta[note0]
		if ("`pq_meta_dataset_note_count'" == "") local pq_meta_dataset_note_count 0
		forvalues k = 1/`pq_meta_dataset_note_count' {
			local pq_meta_dataset_note_`k' : char _dta[note`k']
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
	if ("`source_format'" != "parquet") {
		if ("`partition_by'" != "") {
			di as error "partition_by() is only supported for parquet output"
			exit 198
		}
		if ("`nopartitionoverwrite'" != "") {
			di as error "nopartitionoverwrite is only supported for parquet output"
			exit 198
		}
		if ("`compression'" != "" | `compression_level' != -1) {
			di as error "compression() and compression_level() are only supported for parquet output"
			exit 198
		}
		if ("`stream'" != "" | "`consolidate'" != "" | `chunk' != 2147483647) {
			di as error "stream/chunk/consolidate are only supported for parquet output"
			exit 198
		}
	}
	


	local n_rows = _N

	if (`n_rows' > `chunk') {
		if ("`partition_by'" != "") & (`b_compress' | `b_compress_string_to_numeric') {
			di "Compression disabled for chunked writing as the schema could vary for each chunk"
			di "	which can cause errors for parquet reads"

			local b_compress = 0
			local b_compress_string_to_numeric = 0
		}


		* check and delete file
		local needs_dir = "`partition_by'" == ""
		plugin call polars_parquet_plugin, clean_path "`using'" `needs_dir'
		
		local n_chunks = ceil(`n_rows'/`chunk')
		di "Writing file in `n_chunks' chunks of up to `=strtrim(string(`chunk',"%20.0gc"))' rows"

		if ("`stream'" != "") {
			di "	streaming, save temporary file"
			tempfile save_for_chunks
			quietly save "`save_for_chunks'"
		}
		else {
			tempname save_for_chunks
			capture frame drop `save_for_chunks'
			quietly frame
			local original_frame = r(currentframe)
		}

		local chunk_suffix
		
		local end_row = `offset'
		forvalues i = 1/`n_chunks' {
			if ("`partition_by'" == "")	local chunk_suffix /data_`i'.parquet

			local overwrite_partition = `overwrite_partition' & (`i' == 1)

			local start_row = `end_row' + 1
			local end_row = `start_row' + `chunk' - 1
			local end_row = min(`end_row', `n_rows')
			local rows_to_read = `end_row' - `start_row' + 1

			if ("`stream'" != "") {
				di "	chunk `i': loading rows `start_row'-`end_row'
				quietly use `varlist' in `start_row'/`end_row' using "`save_for_chunks'" 
			}
			else {
				di "	chunk `i': creating frame with rows `start_row'-`end_row'
				frame put `varlist' in `start_row'/`end_row', into(`save_for_chunks')

				frame change `save_for_chunks'
			}

			//	di "`using'`chunk_suffix'"
			plugin call polars_parquet_plugin, save "`using'`chunk_suffix'" "from_macro" `rows_to_read' 0 `"`sql_if'"' `"`StataColumnInfo'"' "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric' 1 `overwrite_partition' "`source_format'"


			if ("`stream'" == "") {
				frame change `original_frame'
				capture frame drop `save_for_chunks'
			}
			
		}

		if ("`partition_by'" == "") & ("`consolidate'" != "") {
			di "	consolidating chunked file into a single file"
			plugin call polars_parquet_plugin, consolidate "`using'"
		}
		if ("`stream'" != "") {
			if ("`do_not_reload'" == "") {
				di "	streaming finished, reload data"
				quietly use "`save_for_chunks'", clear
			}
			else {
				clear
				di "	streaming finished, do_not_reload set, so data not reloaded"
			}
			capture erase `save_for_chunks'.dta
		}

	}
	else {
		//	di `"plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' "`sql_if'" "`StataColumnInfo'" "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric' 0"'
		plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' `"`sql_if'"' `"`StataColumnInfo'"' "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric' 0 0 "`source_format'"
	}


	//	Reset the labeled variables to their original value
	if ("`vars_labeled'" != "") {
		foreach vari in `vars_labeled' {
			quietly drop `vari'
			quietly rename ``vari'' `vari'
		}

		quietly order `original_order'
	}
end


capture program drop pq_write_overflow_dta
program pq_write_overflow_dta
	syntax, using(string) output(string) offset(integer) n_rows(integer) ///
	        columns(string) [if_clause(string) relax asterisk_to_variable(string) ///
	        random_share(real 0) random_seed(integer 0) format(string) ///
	        infer_schema_length(integer 10000) parse_dates(integer 0)]

	if (`infer_schema_length' < 0) {
		display as error `"infer_schema_length() must be >= 0, passed `infer_schema_length'"'
		exit 198
	}

	pq_infer_format, path("`using'") format("`format'")
	local source_format = r(format)
	if !inlist("`source_format'", "parquet", "sas", "spss", "csv") {
		display as error `"Unsupported format(`format'): expected parquet, sas, spss, or csv"'
		exit 198
	}
	local parse_dates_for_plugin = `parse_dates'
	if ("`source_format'" != "csv") {
		local parse_dates_for_plugin = 0
	}

	// Set up relax flag
	if ("`relax'" != "") {
		local b_relax 1
	}
	else {
		local b_relax 0
	}

	// Call plugin to write overflow rows to .dta
	// This writes ALL columns (both strL and non-strL) for the overflow slice
	// Args: parquet_path, dta_output, columns, n_rows, offset, sql_if, relax, asterisk_to_variable, random_share, random_seed
	plugin call polars_parquet_plugin, write_overflow_dta "`using'" "`output'" "`columns'" `n_rows' `offset' `"`if_clause'"' `b_relax' "`asterisk_to_variable'" `random_share' `random_seed' "`source_format'" `infer_schema_length' `parse_dates_for_plugin'
end


capture program drop pq_normalize_csv_opts
program pq_normalize_csv_opts, rclass
	//	Normalize infer_schema_length and parse_dates for non-CSV formats.
	//	CSV-only options are silently reset to defaults for other formats.
	syntax, source_format(string) infer_schema_length(integer) b_parse_dates(integer)
	local infer_schema_length_for_plugin = `infer_schema_length'
	if ("`source_format'" != "csv") {
		if (`infer_schema_length' != 10000) {
			di as text "note: infer_schema_length() ignored for format(`source_format'); only used for csv reads."
		}
		local infer_schema_length_for_plugin = 10000
	}
	local parse_dates_for_plugin = `b_parse_dates'
	if ("`source_format'" != "csv") {
		if (`b_parse_dates') {
			di as text "note: parse_dates ignored for format(`source_format'); only used for csv reads."
		}
		local parse_dates_for_plugin = 0
	}
	return local infer_schema_length_for_plugin = `infer_schema_length_for_plugin'
	return local parse_dates_for_plugin = `parse_dates_for_plugin'
end


capture program drop pq_register_plugin
program pq_register_plugin

	//	di "PLUGIN CHECK"
	capture plugin call polars_parquet_plugin, setup_check ""
	
	if (_rc > 0) {
		// Plugin is not loaded, so initialize it
		capture program polars_parquet_plugin, plugin using("pq.plugin")


		capture plugin call polars_parquet_plugin, setup_check ""
		if (_rc > 0) {
            // OS specific check here
            local os = "`c(os)'"
            
            if ("`os'" == "Windows")		local plugin_file = "pq.dll"
            else if ("`os'" == "MacOSX")	local plugin_file = "pq.dylib"
            else if ("`os'" == "Unix")		local plugin_file = "pq.so"
            else {
                display as error "Unsupported operating system: `os'"
                exit 198
            }
            
            // Try loading the OS-specific plugin
            capture program polars_parquet_plugin, plugin using("`plugin_file'")
            
            if (_rc > 0) {
                display as error "Failed to load plugin `plugin_file' for `os'"
                display as error "Make sure the plugin file exists in the current directory or ado path"
                exit _rc
            }
		}
	}
end


//	Sets the number of threads polars (and pq's own parallel read path) uses,
//	via the POLARS_MAX_THREADS environment variable. Polars builds its global
//	thread pool lazily the first time it is actually used, and only reads that
//	variable once - so this only works before any other pq command (use,
//	describe, save, merge, metadata, ...) has run in the current Stata session.
//	Once that has happened, the plugin refuses and explains why.
capture program drop pq_set_threads
program pq_set_threads
	version 16.0
	syntax anything

	local n_threads `anything'
	capture confirm integer number `n_threads'
	if (_rc) {
		display as error `"pq set_threads requires a positive integer, passed "`n_threads'""'
		exit 198
	}
	if (`n_threads' < 1) {
		display as error `"pq set_threads requires a positive integer, passed "`n_threads'""'
		exit 198
	}

	pq_register_plugin
	plugin call polars_parquet_plugin, set_threads "`n_threads'"
end




program define pq_infer_format, rclass
	version 16
	syntax, path(string) [format(string)]
	local fmt = lower("`format'")
	if ("`fmt'" == "") {
		local p = lower("`path'")
		if regexm("`p'", "\.sas7bdat$")       local fmt sas
		else if regexm("`p'", "\.(sav|zsav)$") local fmt spss
		else if regexm("`p'", "\.csv$")        local fmt csv
		else                                    local fmt parquet
	}
	return local format "`fmt'"
end


program define pq_convert_path, rclass
	version 16
    syntax anything
    
	local filepath `anything'
    
    // Handle the case where filepath might be in quotes
    if `"`filepath'"' == "" {
        local filepath `"`0'"'
    }
    
    // Get current working directory
    local cwd = c(pwd)
    
    // Check operating system
    local os = c(os)
    local is_windows = ("`os'" == "Windows")
    
    // Clean up the input path
    local filepath = trim("`filepath'")
    
    // Debug: show what we're working with
    //	di "Input filepath: [`filepath']"
    //	di "Current directory: [`cwd']"
    //	di "OS: [`os']"
    
    // Check if path is already absolute
    local is_absolute = 0
    
    if `is_windows' {
        // Windows: Check for drive letter (C:) or UNC path (\\)
        if regexm("`filepath'", "^[A-Za-z]:") | regexm("`filepath'", "^\\\\") {
            local is_absolute = 1
        }
    }
    else {
        // Unix: Check if starts with /
        if regexm("`filepath'", "^/") {
            local is_absolute = 1
        }
    }
    
    //	di "Is absolute: `is_absolute'"
    
    // If already absolute, return as-is
    if `is_absolute' {
        local fullpath "`filepath'"
    }
    else {
        // Handle relative paths
        if substr("`filepath'", 1, 2) == "./" {
            // Remove leading "./"
            local filepath = substr("`filepath'", 3, .)
            //	di "After removing ./: [`filepath']"
        }
        else if substr("`filepath'", 1, 2) == ".\" {
            // Remove leading ".\"
            local filepath = substr("`filepath'", 3, .)
            //	di "After removing .\: [`filepath']"
        }
        else if substr("`filepath'", 1, 3) == "../" {
            // Handle parent directory with forward slash
            local filepath = substr("`filepath'", 4, .)
            // Get parent directory manually
            if `is_windows' {
                local lastslash = strpos(reverse("`cwd'"), "\")
                if `lastslash' > 0 {
                    local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                }
            }
            else {
                local lastslash = strpos(reverse("`cwd'"), "/")
                if `lastslash' > 0 {
                    local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                }
            }
            //	di "After removing ../: [`filepath'], parent dir: [`cwd']"
        }
        else if substr("`filepath'", 1, 3) == "..\" {
            // Handle parent directory with backslash
            local filepath = substr("`filepath'", 4, .)
            // Get parent directory manually
            local lastslash = strpos(reverse("`cwd'"), "\")
            if `lastslash' > 0 {
                local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
            }
            //	di "After removing ..\: [`filepath'], parent dir: [`cwd']"
        }
        
        // Handle multiple parent directory references
        while substr("`filepath'", 1, 3) == "../" | substr("`filepath'", 1, 3) == "..\" {
            if substr("`filepath'", 1, 3) == "../" {
                local filepath = substr("`filepath'", 4, .)
                // Get parent directory manually
                if `is_windows' {
                    local lastslash = strpos(reverse("`cwd'"), "\")
                    if `lastslash' > 0 {
                        local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                    }
                }
                else {
                    local lastslash = strpos(reverse("`cwd'"), "/")
                    if `lastslash' > 0 {
                        local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                    }
                }
                //	di "Additional ../: [`filepath'], new parent: [`cwd']"
            }
            else if substr("`filepath'", 1, 3) == "..\" {
                local filepath = substr("`filepath'", 4, .)
                // Get parent directory manually
                local lastslash = strpos(reverse("`cwd'"), "\")
                if `lastslash' > 0 {
                    local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                }
                //	di "Additional ..\: [`filepath'], new parent: [`cwd']"
            }
        }
        
        // Combine current directory with relative path
        if `is_windows' {
            local fullpath "`cwd'\\`filepath'"
        }
        else {
            local fullpath "`cwd'/`filepath'"
        }
        
        //	di "After combination: [`fullpath']"
    }
    
    // Clean up any double separators
    if `is_windows' {
        while regexm("`fullpath'", "\\\\\\\\") {
            local fullpath = regexr("`fullpath'", "\\\\\\\\", "\\\\")
        }
    }
    else {
        while regexm("`fullpath'", "//") {
            local fullpath = regexr("`fullpath'", "//", "/")
        }
    }
    
    // Return the full path
    return local fullpath "`fullpath'"
end
