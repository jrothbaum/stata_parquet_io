*   Empirical check that `pq set_threads` actually changes polars' thread
*   count: read the same file with 1 thread here, and with 16 threads in
*   set_threads_profile_16thread.do, and compare the "[pq profile read ...]
*   total=" timings across the two logs. Must run in its own fresh Stata
*   session (set_threads only works before the first other pq command).

version 16
clear all

pq set_threads 1

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
