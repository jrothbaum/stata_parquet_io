set varabbrev off

capture program drop create_data
program define create_data
	version 16
	syntax		, 	n_rows(integer)

	clear
	set obs `n_rows'
	quietly gen long id = _n
	quietly gen byte grp = mod(_n, 4)
	quietly gen double amount = rnormal() * 100
end


tempfile pq_meta


//	----------------------------------------------------------------------
//	Test 1: save WITHOUT statametadata -> no labels/notes on reload
//	----------------------------------------------------------------------
create_data, n_rows(200)
label variable amount "Transaction amount"
label define grp_lbl 0 "North" 1 "South" 2 "East" 3 "West", replace
label values grp grp_lbl
label data "Sample data"
notes: created for stata_metadata.do
notes amount: units are USD

pq save "`pq_meta'.parquet", replace

pq use "`pq_meta'.parquet", clear
assert _N == 200

local lbl : variable label amount
if ("`lbl'" != "") {
	di as error "Test 1: expected no variable label without statametadata, got `lbl'"
	exit 9
}
local vl : value label grp
if ("`vl'" != "") {
	di as error "Test 1: expected no value label without statametadata, got `vl'"
	exit 9
}
di as text "Test 1 (save without statametadata -> no labels on reload): PASSED"


//	----------------------------------------------------------------------
//	Test 2: save WITH statametadata -> variable label, value label,
//	dataset label, and notes all round trip
//	----------------------------------------------------------------------
create_data, n_rows(200)
label variable amount "Transaction amount"
label variable grp "Region group"
label define grp_lbl 0 "North" 1 "South" 2 "East" 3 "West", replace
label values grp grp_lbl
label data "Sample data"
notes: created for stata_metadata.do
notes amount: units are USD

pq save "`pq_meta'.parquet", replace statametadata

pq use "`pq_meta'.parquet", clear
assert _N == 200

local lbl : variable label amount
if ("`lbl'" != "Transaction amount") {
	di as error "Test 2: expected amount label 'Transaction amount', got '`lbl''"
	exit 9
}
local lbl_grp : variable label grp
if ("`lbl_grp'" != "Region group") {
	di as error "Test 2: expected grp label 'Region group', got '`lbl_grp''"
	exit 9
}
local vl : value label grp
if ("`vl'" != "grp_lbl") {
	di as error "Test 2: expected value label 'grp_lbl', got '`vl''"
	exit 9
}
local txt0 : label grp_lbl 0
local txt3 : label grp_lbl 3
if ("`txt0'" != "North" | "`txt3'" != "West") {
	di as error "Test 2: value label text mismatch (0='`txt0'', 3='`txt3'')"
	exit 9
}
local dlbl : data label
if ("`dlbl'" != "Sample data") {
	di as error "Test 2: expected dataset label 'Sample data', got '`dlbl''"
	exit 9
}
local dnote_count : char _dta[note0]
if ("`dnote_count'" != "1") {
	di as error "Test 2: expected 1 dataset note, got '`dnote_count''"
	exit 9
}
local anote_count : char amount[note0]
if ("`anote_count'" != "1") {
	di as error "Test 2: expected 1 note on amount, got '`anote_count''"
	exit 9
}
di as text "Test 2 (save with statametadata -> full round trip): PASSED"


//	----------------------------------------------------------------------
//	Test 3: statametadata written, but nostatametadata on read -> skipped
//	----------------------------------------------------------------------
pq use "`pq_meta'.parquet", clear nostatametadata
assert _N == 200

local lbl : variable label amount
if ("`lbl'" != "") {
	di as error "Test 3: expected no label with nostatametadata, got '`lbl''"
	exit 9
}
di as text "Test 3 (nostatametadata skips restoration): PASSED"


//	----------------------------------------------------------------------
//	Test 4: dataset label containing $ and backtick-quote pairs survives
//	verbatim (exercises the macval() handling in the read-side apply
//	block). Built via char()+expression concatenation, not literal `` `$ ``
//	in source, since plain source-level backticks/$ would be consumed by
//	Stata's own parser before `label data' ever runs.
//	----------------------------------------------------------------------
create_data, n_rows(50)
local expected_label = "Cost (" + char(36) + "1000s) and a " + char(96) + "quoted" + char(39) + " phrase"
label data `"`macval(expected_label)'"'

pq save "`pq_meta'.parquet", replace statametadata
pq use "`pq_meta'.parquet", clear

local dlbl : data label
if (`"`macval(dlbl)'"' != `"`macval(expected_label)'"') {
	di as error "Test 4: dataset label corrupted"
	exit 9
}
di as text "Test 4 (dataset label with $ and backtick-quote pairs round trips verbatim): PASSED"


//	----------------------------------------------------------------------
//	Test 5: sparse/negative value-label codes round trip
//	(exercises st_vlload rather than an ado min/max range scan)
//	----------------------------------------------------------------------
create_data, n_rows(50)
quietly replace grp = -99 in 1
quietly replace grp = 500 in 2
label define sparse_lbl -99 "Missing" 0 "North" 500 "Overflow", replace
label values grp sparse_lbl

pq save "`pq_meta'.parquet", replace statametadata
pq use "`pq_meta'.parquet", clear

local txt_neg : label sparse_lbl -99
local txt_big : label sparse_lbl 500
if ("`txt_neg'" != "Missing" | "`txt_big'" != "Overflow") {
	di as error "Test 5: sparse value label mismatch (-99='`txt_neg'', 500='`txt_big'')"
	exit 9
}
di as text "Test 5 (sparse/negative value-label codes round trip): PASSED"


//	----------------------------------------------------------------------
//	Test 6: chunked save still carries statametadata into every chunk
//	----------------------------------------------------------------------
create_data, n_rows(500)
label variable amount "Transaction amount"
label define grp_lbl 0 "North" 1 "South" 2 "East" 3 "West", replace
label values grp grp_lbl

pq save "`pq_meta'.parquet", replace statametadata chunk(100)

pq use "`pq_meta'.parquet", clear
assert _N == 500

local lbl : variable label amount
if ("`lbl'" != "Transaction amount") {
	di as error "Test 6: chunked save lost the variable label, got '`lbl''"
	exit 9
}
local vl : value label grp
if ("`vl'" != "grp_lbl") {
	di as error "Test 6: chunked save lost the value label, got '`vl''"
	exit 9
}
di as text "Test 6 (chunked save carries statametadata into every chunk): PASSED"


//	----------------------------------------------------------------------
//	Test 7: pq metadata lists embedded info without loading the file
//	----------------------------------------------------------------------
create_data, n_rows(50)
label variable amount "Transaction amount"
label data "Metadata listing test"

pq save "`pq_meta'.parquet", replace statametadata

clear
pq metadata using "`pq_meta'.parquet", quietly

if ("`r(pq_meta_present)'" != "1") {
	di as error "Test 7: expected r(pq_meta_present) == 1, got '`r(pq_meta_present)''"
	exit 9
}
if ("`r(pq_meta_dataset_label)'" != "Metadata listing test") {
	di as error "Test 7: expected r(pq_meta_dataset_label) == 'Metadata listing test', got '`r(pq_meta_dataset_label)''"
	exit 9
}
assert _N == 0
di as text "Test 7 (pq metadata lists info via r() without loading data): PASSED"


//	----------------------------------------------------------------------
//	Test 8: pq use, metadata_only applies metadata to the loaded dataset
//	without re-reading any rows
//	----------------------------------------------------------------------
create_data, n_rows(75)
label variable amount "Transaction amount"
label define grp_lbl 0 "North" 1 "South" 2 "East" 3 "West", replace
label values grp grp_lbl
pq save "`pq_meta'.parquet", replace statametadata

create_data, n_rows(75)
quietly sum amount
local ref_sum = r(sum)

pq use "`pq_meta'.parquet", metadata_only

assert _N == 75
local lbl : variable label amount
if ("`lbl'" != "Transaction amount") {
	di as error "Test 8: expected amount label 'Transaction amount', got '`lbl''"
	exit 9
}
local vl : value label grp
if ("`vl'" != "grp_lbl") {
	di as error "Test 8: expected value label 'grp_lbl', got '`vl''"
	exit 9
}
quietly sum amount
if (abs(r(sum) - `ref_sum') > 0.5) {
	di as error "Test 8: metadata_only should not have changed the loaded data"
	exit 9
}
di as text "Test 8 (pq use, metadata_only applies labels without reloading data): PASSED"


//	----------------------------------------------------------------------
//	Test 9: pq append only applies statametadata to newly created
//	variables - a pre-existing variable's label is left alone
//	----------------------------------------------------------------------
create_data, n_rows(40)
label variable amount "Transaction amount"
pq save "`pq_meta'_a.parquet", replace statametadata

//	Build the "using" file from a separate throwaway dataset first, since
//	create_data clears whatever is currently loaded - building it after
//	loading the master below would clobber the master in memory.
create_data, n_rows(20)
quietly gen double extra = rnormal()
label variable amount "Should not overwrite the master"
label variable extra "Extra column from the appended file"
pq save "`pq_meta'_b.parquet", replace statametadata

pq use "`pq_meta'_a.parquet", clear
label variable amount "Customized by user after load"

pq append using "`pq_meta'_b.parquet"
assert _N == 60

local lbl_amount : variable label amount
if ("`lbl_amount'" != "Customized by user after load") {
	di as error "Test 9: append overwrote an existing variable's label, got '`lbl_amount''"
	exit 9
}
local lbl_extra : variable label extra
if ("`lbl_extra'" != "Extra column from the appended file") {
	di as error "Test 9: expected new variable extra to get its label from the append, got '`lbl_extra''"
	exit 9
}
di as text "Test 9 (pq append only applies statametadata to newly created variables): PASSED"


//	----------------------------------------------------------------------
//	Test 10: a variable auto-renamed for a long/invalid Parquet name still
//	gets its real statametadata label restored, and the original Parquet
//	name still round-trips on save - the two mechanisms (rename bookkeeping
//	in a characteristic, real label in the metadata footer) coexist rather
//	than one clobbering the other.
//	----------------------------------------------------------------------
cd "C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\testing"
pq use using "long_colname.parquet", clear

//	num_var is one of two columns that collide on their truncated 32-char
//	prefix; which original long name it maps to is a deterministic function
//	of file column order (see mapping.rs generate_rename_map), but this
//	test doesn't assume a specific one - it captures whatever num_var's
//	original name actually is and checks that same value round-trips.
local num_var this_numeric_column_name_is__001
local original_name : char `num_var'[_pq_parquet_name]
assert `"`original_name'"' != ""

label variable `num_var' "A real label, not rename bookkeeping"

tempfile pq_meta_long
pq save "`pq_meta_long'.parquet", replace statametadata

pq use "`pq_meta_long'.parquet", clear

local lbl : variable label `num_var'
if ("`lbl'" != "A real label, not rename bookkeeping") {
	di as error "Test 10: real label was not restored for an auto-renamed variable, got '`lbl''"
	exit 9
}
local roundtrip_name : char `num_var'[_pq_parquet_name]
if (`"`roundtrip_name'"' != `"`original_name'"') {
	di as error "Test 10: rename-on-save bookkeeping was lost, expected '`original_name'' got '`roundtrip_name''"
	exit 9
}

//	The rename mechanism itself must still work: saving again should write
//	the column back out under its original long Parquet name.
pq save "`pq_meta_long'_reexport.parquet", replace
pq describe using "`pq_meta_long'_reexport.parquet", quietly
local found = 0
forvalues i = 1/`r(n_columns)' {
	if (`"`r(name_`i')'"' == `"`original_name'"') local found = 1
}
if (!`found') {
	di as error "Test 10: re-exported file did not restore the original long Parquet column name"
	exit 9
}

capture erase "`pq_meta_long'.parquet"
capture erase "`pq_meta_long'_reexport.parquet"

di as text "Test 10 (long-name rename bookkeeping and real statametadata label coexist): PASSED"


//	----------------------------------------------------------------------
//	Test 11: partition_by() + statametadata - every partition file carries
//	the metadata, and reading the partitioned directory restores it
//	----------------------------------------------------------------------
create_data, n_rows(100)
quietly gen byte region = mod(_n, 3)
label variable amount "Transaction amount"
label define grp_lbl 0 "North" 1 "South" 2 "East" 3 "West", replace
label values grp grp_lbl

tempfile pq_meta_part
pq save "`pq_meta_part'.parquet", replace partition_by(region) statametadata

pq use "`pq_meta_part'.parquet", clear
assert _N == 100

local lbl : variable label amount
if ("`lbl'" != "Transaction amount") {
	di as error "Test 11: partitioned save lost the variable label, got '`lbl''"
	exit 9
}
local vl : value label grp
if ("`vl'" != "grp_lbl") {
	di as error "Test 11: partitioned save lost the value label, got '`vl''"
	exit 9
}
di as text "Test 11 (partition_by + statametadata carries metadata into every partition): PASSED"


//	----------------------------------------------------------------------
//	Test 12: conflicting Stata metadata across files in the same
//	directory is rejected, but nostatametadata still loads the data
//	----------------------------------------------------------------------
create_data, n_rows(10)
label variable amount "File A label"
tempfile pq_conflict_a
pq save "`pq_conflict_a'.parquet", replace statametadata

create_data, n_rows(10)
label variable amount "File B label"
tempfile pq_conflict_b
pq save "`pq_conflict_b'.parquet", replace statametadata

local conflict_dir "`pq_conflict_a'_dir"
mkdir "`conflict_dir'"
copy "`pq_conflict_a'.parquet" "`conflict_dir'/data_1.parquet"
copy "`pq_conflict_b'.parquet" "`conflict_dir'/data_2.parquet"

capture noisily pq use "`conflict_dir'", clear
if (_rc == 0) {
	di as error "Test 12: expected an error reading a directory with conflicting file metadata"
	exit 9
}

pq use "`conflict_dir'", clear nostatametadata
assert _N == 20
local lbl : variable label amount
if ("`lbl'" != "") {
	di as error "Test 12: nostatametadata should not have restored a label, got '`lbl''"
	exit 9
}
di as text "Test 12 (conflicting file metadata rejected; nostatametadata bypasses it): PASSED"


//	----------------------------------------------------------------------
//	Test 13: a conflicting-metadata directory is rejected BEFORE the
//	currently loaded dataset is cleared - a bad target must not destroy
//	data the caller already had in memory
//	----------------------------------------------------------------------
create_data, n_rows(30)
label variable amount "Data that must survive the failed pq use"
quietly sum amount
local ref_sum = r(sum)

capture noisily pq use "`conflict_dir'", clear
if (_rc == 0) {
	di as error "Test 13: expected an error reading the conflicting-metadata directory"
	exit 9
}
if (_N != 30) {
	di as error "Test 13: pre-existing data was cleared despite the read failing, _N=`=_N'"
	exit 9
}
quietly sum amount
if (abs(r(sum) - `ref_sum') > 0.5) {
	di as error "Test 13: pre-existing data was altered despite the read failing"
	exit 9
}
di as text "Test 13 (conflicting-metadata error leaves existing in-memory data untouched): PASSED"


//	----------------------------------------------------------------------
//	Test 14: display format round trips
//	----------------------------------------------------------------------
create_data, n_rows(20)
quietly gen double asof = td(01jan2020) + _n
format asof %td
format amount %12.2fc

pq save "`pq_meta'.parquet", replace statametadata
pq use "`pq_meta'.parquet", clear

local fmt_asof : format asof
local fmt_amount : format amount
if ("`fmt_asof'" != "%td") {
	di as error "Test 14: expected asof format %td, got '`fmt_asof''"
	exit 9
}
if ("`fmt_amount'" != "%12.2fc") {
	di as error "Test 14: expected amount format %12.2fc, got '`fmt_amount''"
	exit 9
}
di as text "Test 14 (display format round trips via statametadata): PASSED"


//	----------------------------------------------------------------------
//	Test 15: extended missing codes (.a/.z) used in a value-label
//	definition round trip correctly
//	----------------------------------------------------------------------
create_data, n_rows(10)
label define status_lbl 0 "Inactive" 1 "Active" 2 "Pending" .a "Not Applicable" .z "Unknown", replace
label values grp status_lbl

pq save "`pq_meta'.parquet", replace statametadata
pq use "`pq_meta'.parquet", clear

local txt_a : label status_lbl .a
local txt_z : label status_lbl .z
if ("`txt_a'" != "Not Applicable" | "`txt_z'" != "Unknown") {
	di as error "Test 15: extended missing value-label codes did not round trip (.a='`txt_a'', .z='`txt_z'')"
	exit 9
}
di as text "Test 15 (extended missing .a/.z value-label codes round trip): PASSED"


//	----------------------------------------------------------------------
//	Test 16: a genuine strL column's statametadata doesn't interfere with
//	drop_strl - the strL column is dropped, other metadata still applies
//	----------------------------------------------------------------------
clear
set obs 3
quietly gen long id = _n
local longstr = ""
forvalues i = 1/300 {
	local longstr = "`longstr'" + "0123456789"
}
quietly gen strL bigstr = "`longstr'" + string(_n)
quietly gen double amount = _n * 10
label variable amount "Amount with real strL present"

pq save "`pq_meta'.parquet", replace statametadata
capture noisily pq use "`pq_meta'.parquet", clear drop_strl
if (_rc != 0) {
	di as error "Test 16: drop_strl read failed unexpectedly, rc=`_rc'"
	exit 9
}
assert _N == 3
capture confirm variable bigstr
if (_rc == 0) {
	di as error "Test 16: expected bigstr (strL) to be dropped by drop_strl"
	exit 9
}
local lbl : variable label amount
if ("`lbl'" != "Amount with real strL present") {
	di as error "Test 16: statametadata was lost alongside the dropped strL column, got '`lbl''"
	exit 9
}
di as text "Test 16 (drop_strl drops the strL column without breaking other statametadata): PASSED"


capture erase "`pq_meta'.parquet"
capture erase "`pq_meta'_a.parquet"
capture erase "`pq_meta'_b.parquet"

di as result "All stata_metadata tests PASSED"
