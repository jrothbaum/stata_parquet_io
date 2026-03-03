set varabbrev off
set more off

tempfile tparquet tcsv tspss tglob_parquet tglob_csv tglob_spss

clear
input long id byte grp str10 name double x
1 1 "alpha" 10
2 1 "beta"  .
3 2 "gamma" -3.5
4 2 "delta" 0
5 3 "echo"  7.25
end
expand 40
by id, sort: replace id = _n
order id grp name x

pq save "`tparquet'.parquet", replace
pq save_csv "`tcsv'.csv", replace
pq save_spss "`tspss'.sav", replace

pq describe using "`tparquet'.parquet", quietly
assert real("`r(n_columns)'") == 4
assert real("`r(n_rows)'") == _N
assert "`r(name_1)'" == "id"
assert "`r(name_2)'" == "grp"
assert "`r(name_3)'" == "name"
assert "`r(name_4)'" == "x"

pq describe using "`tparquet'.parquet", detailed quietly
assert real("`r(n_columns)'") == 4
assert lower("`r(type_3)'") == "string"
assert real("`r(string_length_3)'") >= 4

save "`tglob_parquet'_2018", replace
replace id = id + 10000
save "`tglob_parquet'_2019", replace
pq save using "`tglob_parquet'_2018.parquet", replace
pq save using "`tglob_parquet'_2019.parquet", replace

pq describe using "`tglob_parquet'_*.parquet", quietly asterisk_to_variable(year)
assert real("`r(n_columns)'") == 5
assert real("`r(n_rows)'") == 400

pq describe using "`tcsv'.csv", quietly format(csv)
assert real("`r(n_columns)'") == 4
assert real("`r(n_rows)'") == 200
pq describe using "`tcsv'.csv", detailed quietly format(csv)
assert real("`r(n_columns)'") == 4
assert real("`r(string_length_3)'") >= 4
pq describe_csv using "`tcsv'.csv", quietly infer_schema_length(0)
assert real("`r(n_columns)'") == 4
assert real("`r(n_rows)'") == 200

pq describe using "`tspss'.sav", quietly format(spss)
assert real("`r(n_columns)'") == 4
assert real("`r(n_rows)'") == 200
pq describe using "`tspss'.sav", detailed quietly format(spss)
assert real("`r(n_columns)'") == 4
assert real("`r(string_length_3)'") >= 4
pq describe_spss using "`tspss'.sav", quietly
assert real("`r(n_columns)'") == 4
assert real("`r(n_rows)'") == 200

save "`tglob_csv'_2018", replace
replace id = id + 10000
save "`tglob_csv'_2019", replace
pq save_csv "`tglob_csv'_2018.csv", replace
pq save_csv "`tglob_csv'_2019.csv", replace
capture pq describe using "`tglob_csv'_*.csv", quietly asterisk_to_variable(year) format(csv)
assert _rc != 0

save "`tglob_spss'_2018", replace
replace id = id + 10000
save "`tglob_spss'_2019", replace
pq save_spss "`tglob_spss'_2018.sav", replace
pq save_spss "`tglob_spss'_2019.sav", replace
capture pq describe using "`tglob_spss'_*.sav", quietly asterisk_to_variable(year) format(spss)
assert _rc != 0

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\sas\data\data_poe\cars.sas7bdat"
capture confirm file "`sas_file'"
if (_rc == 0) {
	pq describe using "`sas_file'", quietly format(sas)
	assert real("`r(n_columns)'") > 0
	assert real("`r(n_rows)'") > 0
	pq describe_sas using "`sas_file'", quietly
	assert real("`r(n_columns)'") > 0
	assert real("`r(n_rows)'") > 0

	pq describe using "`sas_file'", detailed quietly format(sas)
	assert real("`r(n_columns)'") > 0
	assert real("`r(n_rows)'") > 0
}
else {
	di as text "Skipping SAS describe test: missing file `sas_file'"
	assert 1 == 1
}

di as result "describe.do: all describe option tests PASSED"
