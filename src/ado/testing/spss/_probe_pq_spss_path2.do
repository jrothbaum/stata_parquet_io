clear
capture pq use_spss using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav", clear
di "RC1=" _rc

clear
capture pq use using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav", clear format(spss)
di "RC2=" _rc
