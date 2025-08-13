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
		quietly gen c_`cols_created' = floor(runiform()*4)
	}
	
	if `n_cols' > `cols_created' {
		local cols_created = `cols_created' + 1
		forvalues ci = `cols_created'/`n_cols' {
			quietly gen c_`ci' = rnormal()
		}
	}
end


di "Create data"
create_data, n_rows(1000) n_cols(10) 


tempfile pq_label
di "Save without the label (as numeric)"


pq save "`pq_label'.parquet", replace label


di "Load numeric (no label)"
pq use "`pq_label'.parquet", clear
sum
tab c_4

di "Add the label"
label define label_test 0 "This" 1 "That" 2 "The Other" 3 "Anything"
label values c_4 label_test


di "Save with the label"
pq save "`pq_label'.parquet", replace label


di "Summarize data (should be original)"
sum


di "Reload the labeled data (string label)"
pq use "`pq_label'.parquet", clear
sum
tab c_4



di "Create data"
create_data, n_rows(1000) n_cols(10) 


di "Add the label"
label define label_test 0 100 1 200 2 300 3 400
label values c_4 label_test


di "Save with the label"
pq save "`pq_label'.parquet", replace label

di "Reload the labeled data (numeric label, saved as string)"
pq use "`pq_label'.parquet", clear
sum
tab c_4


capture erase `pq_label'.parquet



