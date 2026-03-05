set more off
set varabbrev off

global pq_profile_timing 1
do "csv/benchmark_use_csv_sizes.do"
macro drop pq_profile_timing
