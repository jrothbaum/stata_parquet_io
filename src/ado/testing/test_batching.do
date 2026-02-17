clear all

// Create a test dataset with 2500 rows
set obs 2500
gen id = _n
gen value = "row_" + string(id)
gen number = id * 1.5

// Show first few rows
list in 1/5

// Save to parquet
pq save "test_batching.parquet", replace

// Test loading with batching (max_obs_per_batch = 1000)
clear
di ""
di "Testing batch loading with max_obs_per_batch(1000)..."
pq use "test_batching.parquet", clear max_obs_per_batch(1000)

// Verify all rows loaded
di ""
di "Total rows loaded: " _N
assert _N == 2500

// Verify data integrity
di "Checking data integrity..."
assert id[1] == 1
assert id[1000] == 1000
assert id[2500] == 2500
assert value[1] == "row_1"
assert value[2500] == "row_2500"
assert abs(number[1] - 1.5) < 0.001
assert abs(number[2500] - 3750) < 0.001

di ""
di as result "SUCCESS: All 2500 rows loaded correctly with batching!"
