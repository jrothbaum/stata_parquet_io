clear
set obs 3

// Create strL column with long strings
gen strL longstr = ""
replace longstr = "a" * 3000 + "hello" in 1
replace longstr = "b" * 3000 + "hell" in 2
replace longstr = "c" * 3000 + "hel" in 3

di "First 20 chars of row 1: " substr(longstr[1], 1, 20)
di "Last 10 chars of row 1: " substr(longstr[1], length(longstr[1])-9, 10)
di "Last 10 chars of row 2: " substr(longstr[2], length(longstr[2])-9, 10)
di "Last 10 chars of row 3: " substr(longstr[3], length(longstr[3])-9, 10)

tempfile test_output
di "Saving to parquet..."
pq save "`test_output'.parquet", replace

di "Loading from parquet..."
pq use "`test_output'.parquet", clear

di "After round-trip:"
di "Last 10 chars of row 1: " substr(longstr[1], length(longstr[1])-9, 10)
di "Last 10 chars of row 2: " substr(longstr[2], length(longstr[2])-9, 10)
di "Last 10 chars of row 3: " substr(longstr[3], length(longstr[3])-9, 10)
