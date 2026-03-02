clear
capture pq use_spss using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample.sav", clear
di "RC1=" _rc
clear
capture import spss using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample.sav", clear
di "RC2=" _rc
