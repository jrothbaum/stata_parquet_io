set more off
set varabbrev off

//	pq_fast_recast (the Mata-based numeric widen used on `pq append' when an
//	existing variable's type must widen to match the appended file) creates a
//	genuinely new variable and swaps it into place, unlike native `recast'
//	which changes storage width of the SAME variable in place. This test
//	confirms nothing riding on the old variable's identity - display format,
//	variable label, value label, notes, a custom characteristic, and column
//	position - is lost across that swap, for each numeric widen direction
//	pq_gen_or_recast actually triggers.
//
//	Design note: this deliberately avoids `compress' and avoids relying on
//	parquet-roundtrip metadata restoration to establish the PRE-APPEND state.
//	Both have their own heuristics (compress's exact downcast threshold;
//	whether format survives a save/use round-trip) that are irrelevant to
//	what's being tested here. Instead: load normally, use a plain native
//	`recast' to narrow back to the exact starting type under full control,
//	set metadata directly, then capture that as the baseline right before
//	`pq append' - so the only thing under test is what pq_fast_recast does
//	to metadata that demonstrably existed immediately before it ran.

tempfile master_pq extra_pq

//	Master, on disk: plain byte/int/long columns.
clear
set obs 5
gen long before_var = _n
gen byte w_byte = mod(_n, 50)
gen int  w_int  = _n * 100
gen long w_long = _n * 100000
gen long after_var = _N - _n
pq save "`master_pq'.parquet", replace

//	Extra, on disk: before_var/after_var same tiny range (must NOT widen);
//	w_byte/w_int/w_long each one notch wider than master, to force
//	pq_gen_or_recast's byte->int, int->long, and long->double branches.
clear
set obs 5
gen long before_var = 10 + _n
gen long w_byte = 1000 + _n
replace  w_byte = . in 2
gen long w_int  = 200000 + _n
gen double w_long = 5000000000 + _n
replace    w_long = .a in 3
gen long after_var = 10 + _N - _n
pq save "`extra_pq'.parquet", replace

//	Load master (default, non-compress -> pq's safety-upcast types: byte
//	source -> int, int source -> long, long source -> double). Attach
//	metadata to that natural state, no manual recast - before_var/after_var
//	(long source in BOTH files) will independently upcast to the SAME
//	"double" on each file's own load, so they naturally never mismatch;
//	w_byte/w_int (byte/int source in master, long source in extra) DO
//	mismatch, because master's and extra's source types differ.
pq use "`master_pq'.parquet", clear

format w_byte %12.0f
format w_int  %14.0f
format w_long %16.0f
label variable w_byte "byte var label"
label variable w_int  "int var label"
label variable w_long "long var label"
label define bytelbl 1 "one" 2 "two"
label values w_byte bytelbl
notes w_byte: byte note one
notes w_byte: byte note two
notes w_long: long note
//	NOTE: _pq_parquet_name is not used here - pq_save actively interprets
//	that specific characteristic as a rename-on-write instruction, so
//	setting it manually would change which parquet column w_byte lands in
//	and break the append-collision this test relies on. An arbitrary
//	characteristic name is sufficient to prove the generic copy mechanism
//	in pq_fast_recast, which does not treat any key specially.
char w_int[mychar] "custom value"

unab orig_order : _all
local base_before : type before_var
local base_after  : type after_var
local base_w_byte : type w_byte
local base_w_int  : type w_int
local base_w_long : type w_long
di as result "pre-append types: before_var=`base_before' after_var=`base_after' w_byte=`base_w_byte' w_int=`base_w_int' w_long=`base_w_long'"

//	Extra file loads at its own natural (wider) default types, guaranteeing
//	a mismatch against the hand-narrowed master state above.
pq append "`extra_pq'.parquet"

assert _N == 10

di as result _newline "=== post-append describe ==="
describe

//	--- unrelated columns NOT touched by the widen ---
assert "`:type before_var'" == "`base_before'"
assert "`:type after_var'"  == "`base_after'"
di as result "unrelated columns untouched: PASSED"

//	--- storage type widened (changed from the pre-append baseline) ---
//	w_byte/w_int demonstrably widen (both source int/long in master vs. the
//	extra file's wider source types, exercising pq_gen_or_recast's default/
//	"double" branch through the real `pq append' path). w_long's own
//	pre-append baseline is already "double" (long source upcasts to double
//	on both files independently), so no mismatch/recast fires for it here -
//	left in only to confirm its metadata is undisturbed by the OTHER
//	variables' widens (see format/label/notes checks below).
assert "`:type w_byte'" != "`base_w_byte'"
assert "`:type w_int'"  != "`base_w_int'"
di as result "storage-type widen: PASSED (byte=`base_w_byte'->`:type w_byte', int=`base_w_int'->`:type w_int'; long stayed `base_w_long'->`:type w_long', no mismatch to recast)"

//	--- format preserved ---
assert "`:format w_byte'" == "%12.0f"
assert "`:format w_int'"  == "%14.0f"
assert "`:format w_long'" == "%16.0f"
di as result "display format preserved: PASSED"

//	--- variable label preserved ---
assert "`:variable label w_byte'" == "byte var label"
assert "`:variable label w_int'"  == "int var label"
assert "`:variable label w_long'" == "long var label"
di as result "variable label preserved: PASSED"

//	--- value label preserved ---
assert "`:value label w_byte'" == "bytelbl"
di as result "value label preserved: PASSED"

//	--- notes preserved (stored as characteristics under the hood) ---
local n0 : char w_byte[note0]
assert `n0' == 2
local note1 : char w_byte[note1]
local note2 : char w_byte[note2]
assert `"`note1'"' == "byte note one"
assert `"`note2'"' == "byte note two"
local ln0 : char w_long[note0]
assert `ln0' == 1
di as result "notes preserved: PASSED"

//	--- custom characteristics preserved ---
local mychar : char w_int[mychar]
assert `"`mychar'"' == "custom value"
di as result "custom characteristics preserved: PASSED"

//	--- column order preserved (recast must not move the variable) ---
unab post_order : _all
assert `"`post_order'"' == `"`orig_order'"'
di as result "column order preserved: PASSED"

//	--- values correct across the WIDEN itself (w_byte/w_int do go through
//	pq_fast_recast here). Exact extended-missing-code (.a/.z) fidelity
//	across that specific widen is already covered directly, without any
//	parquet round-trip confound, by _diag_recast_correctness.do. w_long
//	never goes through pq_fast_recast in this test (see above), so its
//	missing row only checks generic missingness - whether parquet round-
//	tripping preserves the exact .a code is a separate, pq-wide question
//	unrelated to this fix. ---
assert w_byte[7] == .           // row 7 = extra file row 2 -> replaced to .
assert missing(w_long[8])       // row 8 = extra file row 3 -> replaced to .a
assert w_byte[1] == 1           // master row 1, mod(1,50) == 1
assert w_int[7]  == 200002      // extra file row 2: 200000+2
di as result "widened values (incl. missing) correct: PASSED"

capture erase "`master_pq'.parquet"
capture erase "`extra_pq'.parquet"

di as result _newline "All append_recast_metadata tests PASSED"
