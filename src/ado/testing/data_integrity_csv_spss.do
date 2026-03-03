version 16
set varabbrev off

capture program drop write_with_format
program define write_with_format
	syntax , FORMAT(string) PATH(string)
	if ("`format'" == "csv") {
		pq save_csv "`path'", replace
	}
	else if ("`format'" == "spss") {
		pq save_spss "`path'", replace
	}
	else {
		di as error "Unsupported format in write_with_format(): `format'"
		exit 198
	}
end

capture program drop read_with_format
program define read_with_format
	syntax , FORMAT(string) PATH(string) [PRESERVE(string)]
	if ("`format'" == "csv") {
		pq use_csv "`path'", clear
	}
	else if ("`format'" == "spss") {
		pq use_spss "`path'", clear `preserve'
	}
	else {
		di as error "Unsupported format in read_with_format(): `format'"
		exit 198
	}
end

capture program drop run_integrity_for_format
program define run_integrity_for_format
	syntax , FORMAT(string) BASEDTA(string) APPENDDTA(string) MERGEDTA(string) ///
		EXPAPPENDDTA(string) EXPMERGEDTA(string) BASEFILE(string) APPENDFILE(string) MERGEFILE(string)

	di as text "Running data-integrity checks for format(`format')"
	local preserve_opt
	if ("`format'" == "spss") local preserve_opt preserve_order

	// Write source datasets to test format.
	use "`basedta'", clear
	write_with_format, format("`format'") path("`basefile'")

	use "`appenddta'", clear
	write_with_format, format("`format'") path("`appendfile'")

	use "`mergedta'", clear
	write_with_format, format("`format'") path("`mergefile'")

	// 1) Round-trip compare.
	read_with_format, format("`format'") path("`basefile'") preserve("`preserve_opt'")
	assert _N == 5
	sort id
	cf _all using "`basedta'", all
	di as text "  round-trip compare: PASSED"

	// 2) Append compare to explicit expected dataset.
	read_with_format, format("`format'") path("`basefile'") preserve("`preserve_opt'")
	pq append "`appendfile'", format(`format') `preserve_opt'
	assert _N == 8
	sort id
	cf _all using "`expappenddta'", all
	di as text "  append compare: PASSED"

	// 3) Merge compare to explicit expected dataset.
	read_with_format, format("`format'") path("`basefile'") preserve("`preserve_opt'")
	pq merge 1:1 id using "`mergefile'", format(`format') `preserve_opt'
	assert _N == 6
	sort id
	cf _all using "`expmergedta'", all
	di as text "  merge compare: PASSED"
end

tempfile base_dta append_dta merge_dta exp_append_dta exp_merge_dta
tempfile base_csv append_csv merge_csv
tempfile base_spss append_spss merge_spss

// Base dataset
clear
input long id int grp double x str12 name byte flag
1 10 1.5    "alpha" 0
2 10 .      "beta"  1
3 20 -2.0   ""      0
4 20 0      "delta" 1
5 30 3.25   "echo"  0
end
sort id
save "`base_dta'", replace

// Append dataset
clear
input long id int grp double x str12 name byte flag
6 30 7      "foxtrot" 1
7 40 .      "golf"    0
8 40 -1.5   "hotel"   1
end
sort id
save "`append_dta'", replace

// Merge "using" dataset
clear
input long id double z str8 tag
1 100 "m1"
3 300 "m3"
5 500 "m5"
8 800 "m8"
end
sort id
save "`merge_dta'", replace

// Expected append result
use "`base_dta'", clear
append using "`append_dta'"
sort id
save "`exp_append_dta'", replace

// Expected merge result
use "`base_dta'", clear
merge 1:1 id using "`merge_dta'"
sort id
save "`exp_merge_dta'", replace

run_integrity_for_format, format("csv") ///
	basedta("`base_dta'") appenddta("`append_dta'") mergedta("`merge_dta'") ///
	expappenddta("`exp_append_dta'") expmergedta("`exp_merge_dta'") ///
	basefile("`base_csv'.csv") appendfile("`append_csv'.csv") mergefile("`merge_csv'.csv")

run_integrity_for_format, format("spss") ///
	basedta("`base_dta'") appenddta("`append_dta'") mergedta("`merge_dta'") ///
	expappenddta("`exp_append_dta'") expmergedta("`exp_merge_dta'") ///
	basefile("`base_spss'.sav") appendfile("`append_spss'.sav") mergefile("`merge_spss'.sav")

di as result "All explicit data-integrity tests PASSED"
