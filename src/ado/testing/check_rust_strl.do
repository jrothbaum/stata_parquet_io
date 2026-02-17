use "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\test_rust_strl.dta", clear

di "Row 1 length: " length(longstr[1]) ", last 10: " substr(longstr[1], length(longstr[1])-9, 10)
di "Row 2 length: " length(longstr[2]) ", last 10: " substr(longstr[2], length(longstr[2])-9, 10)
di "Row 3 length: " length(longstr[3]) ", last 10: " substr(longstr[3], length(longstr[3])-9, 10)

di ""
di "Expected:"
di "  Row 1: length=3005, last 10=aaaaahello"
di "  Row 2: length=3004, last 10=bbbbbbhell"
di "  Row 3: length=3003, last 10=ccccccchel"
