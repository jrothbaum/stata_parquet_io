set more off
set varabbrev off

log using "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\testing\_bench_default_vs_legacy_quick_results.log", replace text

local sas_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat\crates\polars_readstat_rs\tests\sas\data\too_big\hhpub25.sas7bdat"
local spss_file "C:\Users\jonro\OneDrive\Documents\Coding\polars_readstat_rs\tests\spss\data\sample_large.sav"

capture confirm file "`sas_file'"
if _rc != 0 {
    di as error "Missing SAS benchmark input: `sas_file'"
    log close
    exit 601
}

capture confirm file "`spss_file'"
if _rc != 0 {
    di as error "Missing SPSS benchmark input: `spss_file'"
    log close
    exit 601
}

local reps_sas = 3
local reps_spss = 8

* SAS baseline row count
pq use_sas using "`sas_file'", clear
count
local sas_n_rows = r(N)

* SAS: default inferred batch
clear
timer clear
forvalues r = 1/`reps_sas' {
    clear
    timer on 1
    pq use_sas using "`sas_file'", clear
    timer off 1
    assert _N == `sas_n_rows'
}

* SAS: legacy explicit batch_size(1000000)
forvalues r = 1/`reps_sas' {
    clear
    timer on 2
    pq use_sas using "`sas_file'", clear batch_size(1000000)
    timer off 2
    assert _N == `sas_n_rows'
}

timer list 1
local sas_default = r(t1) / `reps_sas'
timer list 2
local sas_legacy = r(t2) / `reps_sas'
local sas_speed = `sas_legacy' / max(`sas_default', 1e-9)

di as text "SAS omitted batch vs legacy batch_size(1000000)"
di as result "sas_default | " %8.4f `sas_default'
di as result "sas_legacy  | " %8.4f `sas_legacy'
di as result "sas_speedup | " %5.2f `sas_speed' "x (legacy/default)"

* SPSS baseline row count
pq use_spss using "`spss_file'", clear
count
local spss_n_rows = r(N)

* SPSS: default inferred batch
timer clear
forvalues r = 1/`reps_spss' {
    clear
    timer on 1
    pq use_spss using "`spss_file'", clear
    timer off 1
    assert _N == `spss_n_rows'
}

* SPSS: legacy explicit batch_size(1000000)
forvalues r = 1/`reps_spss' {
    clear
    timer on 2
    pq use_spss using "`spss_file'", clear batch_size(1000000)
    timer off 2
    assert _N == `spss_n_rows'
}

timer list 1
local spss_default = r(t1) / `reps_spss'
timer list 2
local spss_legacy = r(t2) / `reps_spss'
local spss_speed = `spss_legacy' / max(`spss_default', 1e-9)

di as text "SPSS omitted batch vs legacy batch_size(1000000)"
di as result "spss_default | " %8.4f `spss_default'
di as result "spss_legacy  | " %8.4f `spss_legacy'
di as result "spss_speedup | " %5.2f `spss_speed' "x (legacy/default)"

di as text "end of do-file"
log close
