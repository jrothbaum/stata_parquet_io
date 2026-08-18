*   Companion to set_threads_profile_1thread.do - same data/read, but with
*   16 threads instead of 1. Compare the "[pq profile read ...] total="
*   timings across the two logs to confirm set_threads actually changes
*   polars' thread count.

version 16
clear all

pq set_threads 16

global pq_profile_timing 1

set seed 20260818
set obs 4000000
quietly forvalues c = 1/12 {
	quietly gen double c_`c' = rnormal()
}

tempfile t1
quietly pq save * using "`t1'.parquet", replace

clear
pq use * using "`t1'.parquet", clear
