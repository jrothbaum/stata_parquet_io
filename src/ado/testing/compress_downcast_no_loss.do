set more off
clear

input double byte_candidate int_candidate long_candidate double_candidate frac_candidate
-128 -32768 -2147483648 -2147483649 0.1
-1 -129 -32769 -2147483649 1.25
0 0 0 0 -3.5
1 1 1 1 3.14159265358979
99 100 32740 2147483620 123456.789
100 32740 32741 2147483621 -0.0000001
. . . . .
50 -50 2147483620 -2147483649 9999999.5
end

tempfile before parquet
save "`before'.dta", replace

pq save "`parquet'.parquet", replace compress
pq use "`parquet'.parquet", clear

assert _N == 8

rename * *_pq
merge 1:1 _n using "`before'.dta", nogen assert(3)

foreach v in byte_candidate int_candidate long_candidate double_candidate {
	assert (`v' == `v'_pq) | (missing(`v') & missing(`v'_pq))
}

assert (abs(frac_candidate - frac_candidate_pq) < 1e-12) | (missing(frac_candidate) & missing(frac_candidate_pq))

describe