clear
import spss using "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav", clear
describe
unab vlist: *
di "VARS: list'"
