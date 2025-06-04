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


di "Parallelization"
create_data, n_rows(1001) n_cols(10) 

tempfile pq_random


pq save "`pq_random'.parquet", replace

di "Load the full file"
pq use "`pq_random'.parquet", clear
count
sum


local random_n = 100
di "Load `random_n' at random, no seed"
pq use "`pq_random'.parquet", clear random_n(`random_n')
count
sum


di "Load `random_n' at random, no seed, again"
pq use "`pq_random'.parquet", clear random_n(`random_n')
count
sum



di "Load `random_n' at random, seed"
pq use "`pq_random'.parquet", clear random_n(`random_n') random_seed(12345)
count
sum


di "Load `random_n' at random, seed, again"
pq use "`pq_random'.parquet", clear random_n(`random_n') random_seed(12345)
count
sum


local random_share = 0.5
di "Load `random_share' at random, no seed"
pq use "`pq_random'.parquet", clear random_share(`random_share')
count
sum


di "Load `random_share' at random, no seed, again"
pq use "`pq_random'.parquet", clear random_share(`random_share')
count
sum


di "Load `random_share' at random, seed"
pq use "`pq_random'.parquet", clear random_share(`random_share') random_seed(12345)
count
sum


di "Load `random_share' at random, seed, again"
pq use "`pq_random'.parquet", clear random_share(`random_share') random_seed(12345)
count
sum
capture erase `pq_random'.parquet



