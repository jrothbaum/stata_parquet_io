//	Notes with quotes/backticks must round trip verbatim (PR 56)
clear
set obs 3
gen byte x = _n
notes x: plain note
notes x: He said "hi" and left
notes: dataset "quoted" note
tempfile f
pq save "`f'.parquet", replace statametadata
pq use "`f'.parquet", clear
local n1 : char x[note1]
local n2 : char x[note2]
local d1 : char _dta[note1]
assert `"`n1'"' == "plain note"
assert `"`n2'"' == `"He said "hi" and left"'
assert `"`d1'"' == `"dataset "quoted" note"'
display "PASS note_quotes_roundtrip"
