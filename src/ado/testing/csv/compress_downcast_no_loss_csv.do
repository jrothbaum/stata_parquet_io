set more off
clear

input double byte_candidate int_candidate long_candidate double_candidate frac_candidate
-128 -32767 -2147483647 -2147483647 0.1
-1 -129 -32769 -2147483000 1.25
0 0 0 0 -3.5
1 1 1 1 3.14159265358979
99 100 32740 2147483620 123456.789
100 32740 32741 2147483001 -0.0000001
. . . . .
50 -50 2147483620 -2147483001 9999999.5
end

tempfile before parquet
save "`before'.dta", replace

pq save_csv "`parquet'.csv", replace compress
pq use_csv "`parquet'.csv", clear

assert _N == 8

rename * *_pq
merge 1:1 _n using "`before'.dta", nogen assert(3)

foreach v in byte_candidate int_candidate long_candidate double_candidate {
	assert (abs(`v'_pq - `v') <= 1e6) | (missing(`v') & missing(`v'_pq))
}

assert (abs(frac_candidate_pq - float(frac_candidate)) <= 1e-6 * cond(abs(float(frac_candidate)) > 1, abs(float(frac_candidate)), 1)) | (missing(frac_candidate) & missing(frac_candidate_pq))

describe

