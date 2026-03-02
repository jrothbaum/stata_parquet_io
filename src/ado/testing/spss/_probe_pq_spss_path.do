clear
capture pq use_spss using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav", clear
local rc1 = _rc
di "RC_USE_SPSS=c1'"

clear
capture pq use using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav", clear format(spss)
local rc2 = _rc
di "RC_USE_FORMAT=c2'"
