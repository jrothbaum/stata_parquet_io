*   pq set_threads must fail once any other pq command has already run in
*   the session (polars' thread pool is lazily built on first use and only
*   reads POLARS_MAX_THREADS once, so changing it later is a no-op at best).

version 16
clear all

* Sanity: a bad value is rejected before it ever touches the plugin
capture pq set_threads 0
assert _rc == 198

capture pq set_threads -1
assert _rc == 198

capture pq set_threads abc
assert _rc == 198

* Touch polars via an ordinary pq command first ...
tempfile t1
quietly set obs 10
quietly gen x = _n
quietly pq save x using "`t1'.parquet", replace
quietly pq describe using "`t1'.parquet"

* ... now set_threads must refuse, since it is too late in this session
capture pq set_threads 2
assert _rc == 198

di as text "PASS: set_threads correctly refused after a pq command had already run"
