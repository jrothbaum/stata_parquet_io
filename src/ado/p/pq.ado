*! pq Version 3.0.10 12aug2026
*! Read/write Parquet files with Stata
*! Version 3.0.10 - Extend statametadata to partitioned, chunked, streamed, and consolidated
*!                  Parquet output with uniform-footer preflight and transactional consolidation.
*!                  Round-trip display formats, variable notes, the dataset label,
*!                  dataset notes, and original storage types alongside variable and
*!                  value labels.  A storage type is restored by a verified post-load
*!                  recast, so data that no longer fits keeps the wider loaded type.
*!         3.0.9 - Add safe_int64 option: error (naming columns) when Int64/UInt64 values exceed
*!                 +/-2^53 and would silently lose precision as a Stata double; safe_int64 auto-loads
*!                 the affected columns as strings instead of erroring.
*!                 Also fix regression to properly read columns with names longer than 32 characters.
*!         3.0.8 - Fix overflow/null regression (cast to larger type to avoid losing values to reserved nulls)
*!         3.0.7 - Fix for write subset of variables, support for arrow extension types
*!				   Fix for relaxed on directory read
*!				   Auto infer file format on pq read-like
*!         3.0.6 - Update underlying rust library for better SPSS write compatibility
*!         3.0.5 - Fix random_n() off-by-one; fix pq use with wide varlists hitting Stata's plugin-call
*!                 string limit; aggressive memory return on Linux for HPC/cgroup environments. Fix for weird label char edge case
*!         3.0.0 - Add robust SPSS/CSV round-trip support, faster CSV read/write than native stata CSV
*! 				   faster SAS reads.
*!				   Better handling reads of strl columns, handle datasets that exceed the limit
*!				   of Stata's C plugin API.  Better options for low-memory parquet writes
*!         2.0.0 - Fix float32 compress, improve strL (string) load, allow large file load/save
*! 		   1.9.1 - Fix parquet->stata integer cast overflow bug
*!         1.9.0 - Vastly simplified use/append code to make it easier to manage and debug.  No change to API/function signature or functionality
*! 		   1.8.0 - Fix pq append for subsets of variables, add settable batch_size *
*! 		   1.7.4 - fix str length bug for special characters (str lengths is number of bytes not characters) *
*! 		   1.7.3 - Minor change to saves with partition and compress - don't downcast to boolean to avoid a=true/a=false columns (so it's a=1/a=0)*
*! 	       1.7.2 - Fix overzealous compress on parquet use (to respect stata's odd integer limits) *
*! 	       1.7.1 - fix bug where variables that contain another variable in them not loading with *
*!  	   1.7.0 - upgrade to rust polars 0.49, add option to save labels rather than numeric value

capture program drop pq
program define pq
	gettoken todo 0: 0
    local todo `todo'

    if ("`todo'" == "use") {
		//	di `"pq_use_append `0'"'
		//	pq_use_append `0'
		pq_use_append `0'
    }
	else if ("`todo'" == "use_sas") {
		pq_use_sas `0'
	}
	else if ("`todo'" == "use_spss") {
		pq_use_spss `0'
	}
	else if ("`todo'" == "use_csv") {
		pq_use_csv `0'
	}
	else if ("`todo'" == "append") {
		//	di `"pq_use_append `0' append"'
		if strpos(`"`0'"', ",") > 0 {
			// Already has options, just add append
			pq_use_append `0' append
		}
		else {
			// No options yet, need to add comma before append
			pq_use_append `0', append
		}
    }
	else if ("`todo'" == "merge") {
		//	di `"pq_merge `0'"'
		pq_merge `0'
    }
	else if ("`todo'" == "merge_sas") {
		pq_merge_sas `0'
	}
	else if ("`todo'" == "merge_spss") {
		pq_merge_spss `0'
	}
	else if ("`todo'" == "merge_csv") {
		pq_merge_csv `0'
	}
    else if ("`todo'" == "save") {
		//	di `"pq_save `0'"'
        pq_save `0'
    }
    else if ("`todo'" == "save_spss") {
        pq_save_spss `0'
    }
    else if ("`todo'" == "save_csv") {
        pq_save_csv `0'
    }
    else if ("`todo'" == "describe") {
		//	di `"pq_describe `0'"'
        pq_describe `0'
    }
	else if ("`todo'" == "describe_sas") {
		pq_describe_sas `0'
	}
	else if ("`todo'" == "describe_spss") {
		pq_describe_spss `0'
	}
	else if ("`todo'" == "describe_csv") {
		pq_describe_csv `0'
	}
	else if ("`todo'" == "path") {
		//	di `"pq_convert_path `0'"'
		pq_convert_path `0'
	}
    else {
        disp as err `"Unknown sub-comand `todo'"'
        exit 198
    }
end


capture mata: mata drop _pq_metadata_variable_labels
capture mata: mata drop _pq_metadata_value_label_names
capture mata: mata drop _pq_metadata_label_values
capture mata: mata drop _pq_metadata_label_texts
capture mata: mata drop _pq_metadata_formats
capture mata: mata drop _pq_metadata_is_string
capture mata: mata drop _pq_metadata_notes
capture mata: mata drop _pq_metadata_data_notes
capture mata: mata drop _pq_metadata_types
capture mata: mata drop _pq_restore_storage_type()
capture mata: mata drop _pq_recast_excluded()
capture mata: mata drop _pq_cast_targets()
capture mata: mata drop _pq_clear_stata_metadata()
capture mata: mata drop _pq_capture_stata_metadata()
capture mata: mata drop _pq_label_definition_equal()
capture mata: mata drop _pq_read_notes()
capture mata: mata drop _pq_write_notes()
capture mata: mata drop _pq_apply_stata_metadata()
capture mata: mata drop _pq_apply_stata_metadata_newvars()

mata:
_pq_metadata_variable_labels = J(0, 1, "")
_pq_metadata_value_label_names = J(0, 1, "")
_pq_metadata_label_values = asarray_create("string", 1)
_pq_metadata_label_texts = asarray_create("string", 1)
_pq_metadata_formats = J(0, 1, "")
_pq_metadata_is_string = J(0, 1, .)
_pq_metadata_notes = asarray_create("string", 1)
_pq_metadata_data_notes = J(0, 1, "")
_pq_metadata_types = J(0, 1, "")

void _pq_clear_stata_metadata()
{
	external string colvector _pq_metadata_variable_labels
	external string colvector _pq_metadata_value_label_names
	external transmorphic scalar _pq_metadata_label_values
	external transmorphic scalar _pq_metadata_label_texts
	external string colvector _pq_metadata_formats
	external real colvector _pq_metadata_is_string
	external transmorphic scalar _pq_metadata_notes
	external string colvector _pq_metadata_data_notes
	external string colvector _pq_metadata_types

	_pq_metadata_variable_labels = J(0, 1, "")
	_pq_metadata_value_label_names = J(0, 1, "")
	_pq_metadata_label_values = asarray_create("string", 1)
	_pq_metadata_label_texts = asarray_create("string", 1)
	_pq_metadata_formats = J(0, 1, "")
	_pq_metadata_is_string = J(0, 1, .)
	_pq_metadata_notes = asarray_create("string", 1)
	_pq_metadata_data_notes = J(0, 1, "")
	_pq_metadata_types = J(0, 1, "")
}

//	Notes are stored as the characteristics note0 (count) and note1..noteN
//	(text).  Reading and writing them through st_global() keeps the text out
//	of command-line position entirely, so note text containing quotes,
//	backticks, or dollar signs round-trips verbatim.
string colvector _pq_read_notes(string scalar owner)
{
	string colvector notes
	real scalar count, i

	count = strtoreal(st_global(owner + "[note0]"))
	if (count >= . | count < 1) return(J(0, 1, ""))
	count = floor(count)
	notes = J(count, 1, "")
	for (i = 1; i <= count; i++) {
		notes[i] = st_global(owner + "[note" + strofreal(i) + "]")
	}
	return(notes)
}

void _pq_write_notes(string scalar owner, string colvector notes)
{
	real scalar existing, i

	//	Clear any pre-existing notes first so the restore is exact rather
	//	than a merge with whatever the target already carried.
	existing = strtoreal(st_global(owner + "[note0]"))
	if (existing < . & existing >= 1) {
		for (i = 1; i <= floor(existing); i++) {
			st_global(owner + "[note" + strofreal(i) + "]", "")
		}
	}
	st_global(owner + "[note0]", "")

	if (rows(notes) == 0) return
	for (i = 1; i <= rows(notes); i++) {
		st_global(owner + "[note" + strofreal(i) + "]", notes[i])
	}
	st_global(owner + "[note0]", strofreal(rows(notes)))
}

//	Was this column's type chosen by the caller via cast()?  cast() keys are
//	physical Parquet column names, so a column renamed on import is matched
//	through the _pq_parquet_name characteristic that records its origin.
real scalar _pq_recast_excluded(string scalar variable_name,
	string rowvector skip_vars)
{
	string scalar physical_name

	if (cols(skip_vars) == 0) return(0)
	if (anyof(skip_vars, variable_name)) return(1)
	physical_name = st_global(variable_name + "[_pq_parquet_name]")
	if (physical_name != "" & anyof(skip_vars, physical_name)) return(1)
	return(0)
}

//	Restore one variable's original Stata storage type.
//
//	This is deliberately a post-load recast rather than a declared type
//	forced on the reader.  The capsule records the type the data had when
//	it was saved; the file may since have been rewritten by another tool,
//	widened by a relaxed multi-file union, or promoted by safe_int64, and a
//	type asserted before the values are seen would push out-of-range values
//	into missing at rc 0.  recast checks the actual values instead.
//
//	recast REFUSES a lossy change but still returns rc 0 -- it prints
//	"N values would be changed; not changed" -- so success is confirmed by
//	re-reading the type, never by the return code.  A refusal leaves the
//	wider loaded type in place, which holds every value exactly.
void _pq_restore_storage_type(string scalar variable_name,
	string scalar wanted_type)
{
	real scalar variable_index

	if (wanted_type == "") return
	variable_index = st_varindex(variable_name)
	if (variable_index == .) return
	if (st_vartype(variable_index) == wanted_type) return
	//	recast cannot cross the string/numeric boundary, and a read-time
	//	cast that changed the class is the user's request, not drift to undo.
	if (st_isstrvar(variable_index) != (substr(wanted_type, 1, 3) == "str")) {
		return
	}
	_stata("quietly recast " + wanted_type + " " + variable_name)
}

//	Column names named as keys in a cast() JSON payload, e.g.
//	{"a":"float","b":"string"}.  Only those columns had their type chosen by
//	the caller; every other column should still get its saved type back.
//	Scanned character by character rather than tokenised: Mata's tokens()
//	treats the double quote as a delimiter, so it cannot be used to walk a
//	JSON payload.  A quoted token counts as a key only when the next
//	non-blank character is ':'; the value that follows is then skipped so
//	its text can never be mistaken for a key.
void _pq_cast_targets(string scalar cast_local, string scalar out_local)
{
	string scalar payload, targets, key
	real scalar position, length, quote_start

	payload = st_local(cast_local)
	targets = ""
	length = strlen(payload)
	position = 1
	while (position <= length) {
		if (substr(payload, position, 1) != char(34)) {
			position++
			continue
		}
		quote_start = position + 1
		position = quote_start
		while (position <= length &
			substr(payload, position, 1) != char(34)) position++
		key = substr(payload, quote_start, position - quote_start)
		position++
		while (position <= length &
			substr(payload, position, 1) == " ") position++
		if (position <= length & substr(payload, position, 1) == ":") {
			if (key != "") {
				targets = targets + (targets == "" ? "" : " ") + key
			}
			position++
			while (position <= length &
				substr(payload, position, 1) == " ") position++
			if (position <= length &
				substr(payload, position, 1) == char(34)) {
				position++
				while (position <= length &
					substr(payload, position, 1) != char(34)) position++
				position++
			}
		}
	}
	st_local(out_local, targets)
}

void _pq_capture_stata_metadata(string scalar capsule_vars_local)
{
	external string colvector _pq_metadata_variable_labels
	external string colvector _pq_metadata_value_label_names
	external transmorphic scalar _pq_metadata_label_values
	external transmorphic scalar _pq_metadata_label_texts
	external string colvector _pq_metadata_formats
	external real colvector _pq_metadata_is_string
	external transmorphic scalar _pq_metadata_notes
	external string colvector _pq_metadata_data_notes
	external string colvector _pq_metadata_types

	string rowvector capsule_vars
	string scalar value_label_name
	real colvector values
	string colvector texts
	real scalar i, variable_index

	capsule_vars = tokens(st_local(capsule_vars_local))
	_pq_clear_stata_metadata()
	_pq_metadata_variable_labels = J(cols(capsule_vars), 1, "")
	_pq_metadata_value_label_names = J(cols(capsule_vars), 1, "")
	_pq_metadata_formats = J(cols(capsule_vars), 1, "")
	_pq_metadata_is_string = J(cols(capsule_vars), 1, .)
	_pq_metadata_types = J(cols(capsule_vars), 1, "")

	for (i = 1; i <= cols(capsule_vars); i++) {
		variable_index = st_varindex(capsule_vars[i])
		if (variable_index == .) {
			errprintf("Stata metadata capsule variable not found: %s\n", capsule_vars[i])
			_error(198)
		}
		_pq_metadata_variable_labels[i] = st_varlabel(variable_index)
		_pq_metadata_formats[i] = st_varformat(variable_index)
		_pq_metadata_is_string[i] = st_isstrvar(variable_index)
		_pq_metadata_types[i] = st_vartype(variable_index)
		asarray(_pq_metadata_notes, strofreal(i),
			_pq_read_notes(capsule_vars[i]))
		value_label_name = st_varvaluelabel(variable_index)
		_pq_metadata_value_label_names[i] = value_label_name
		if (value_label_name != "" &
			!asarray_contains(_pq_metadata_label_values, value_label_name)) {
			st_vlload(value_label_name, values, texts)
			asarray(_pq_metadata_label_values, value_label_name, values)
			asarray(_pq_metadata_label_texts, value_label_name, texts)
		}
	}

	_pq_metadata_data_notes = _pq_read_notes("_dta")
}

real scalar _pq_label_definition_equal(
	string scalar value_label_name,
	real colvector expected_values,
	string colvector expected_texts)
{
	real colvector existing_values
	string colvector existing_texts

	st_vlload(value_label_name, existing_values, existing_texts)
	if (rows(existing_values) != rows(expected_values) |
		rows(existing_texts) != rows(expected_texts)) return(0)
	if (rows(expected_values) > 0 &
		any(existing_values :!= expected_values)) return(0)
	if (rows(expected_texts) > 0 &
		any(existing_texts :!= expected_texts)) return(0)
	return(1)
}

void _pq_apply_stata_metadata(string scalar target_vars_local,
	real scalar allow_recast, string scalar recast_skip_local)
{
	external string colvector _pq_metadata_variable_labels
	external string colvector _pq_metadata_value_label_names
	external transmorphic scalar _pq_metadata_label_values
	external transmorphic scalar _pq_metadata_label_texts
	external string colvector _pq_metadata_formats
	external real colvector _pq_metadata_is_string
	external transmorphic scalar _pq_metadata_notes
	external string colvector _pq_metadata_data_notes
	external string colvector _pq_metadata_types

	string rowvector target_vars, skip_vars
	string scalar value_label_name
	real colvector values
	string colvector texts
	real scalar i, variable_index

	target_vars = tokens(st_local(target_vars_local))
	skip_vars = tokens(st_local(recast_skip_local))
	//	Every captured attribute is a parallel array indexed by capsule
	//	position.  Check all of them, not just the labels, so a vector left
	//	stale or short can never be read against the wrong variable.
	if (cols(target_vars) != rows(_pq_metadata_variable_labels) |
		cols(target_vars) != rows(_pq_metadata_value_label_names) |
		cols(target_vars) != rows(_pq_metadata_formats) |
		cols(target_vars) != rows(_pq_metadata_is_string) |
		cols(target_vars) != rows(_pq_metadata_types)) {
		errprintf("Invalid Stata metadata target map\n")
		_error(198)
	}

	// Validate every target and every existing definition before changing metadata.
	for (i = 1; i <= cols(target_vars); i++) {
		variable_index = st_varindex(target_vars[i])
		if (variable_index == .) {
			errprintf("Stata metadata target variable not found: %s\n", target_vars[i])
			_error(198)
		}
		value_label_name = _pq_metadata_value_label_names[i]
		if (value_label_name != "") {
			if (st_isstrvar(variable_index)) {
				errprintf("Cannot attach numeric value label to string variable: %s\n", target_vars[i])
				_error(198)
			}
			values = asarray(_pq_metadata_label_values, value_label_name)
			texts = asarray(_pq_metadata_label_texts, value_label_name)
			if (st_vlexists(value_label_name) &
				!_pq_label_definition_equal(value_label_name, values, texts)) {
				errprintf("Conflicting value-label definition: %s\n", value_label_name)
				_error(198)
			}
		}
	}

	// Define each shared mapping once, then attach exact names and variable labels.
	for (i = 1; i <= cols(target_vars); i++) {
		value_label_name = _pq_metadata_value_label_names[i]
		if (value_label_name != "") {
			if (!st_vlexists(value_label_name)) {
				values = asarray(_pq_metadata_label_values, value_label_name)
				texts = asarray(_pq_metadata_label_texts, value_label_name)
				st_vlmodify(value_label_name, values, texts)
			}
		}
	}
	for (i = 1; i <= cols(target_vars); i++) {
		if (allow_recast & !_pq_recast_excluded(target_vars[i], skip_vars)) {
			_pq_restore_storage_type(target_vars[i], _pq_metadata_types[i])
		}
		variable_index = st_varindex(target_vars[i])
		st_varlabel(variable_index, _pq_metadata_variable_labels[i])
		//	A read-time cast can change a column's string/numeric class; a
		//	format from the other class would be rejected, so skip it rather
		//	than fail the whole restore.
		if (_pq_metadata_formats[i] != "" &
			st_isstrvar(variable_index) == _pq_metadata_is_string[i]) {
			st_varformat(variable_index, _pq_metadata_formats[i])
		}
		_pq_write_notes(target_vars[i],
			asarray(_pq_metadata_notes, strofreal(i)))
		value_label_name = _pq_metadata_value_label_names[i]
		if (value_label_name != "") {
			st_varvaluelabel(variable_index, value_label_name)
		}
	}

	//	Dataset-level notes belong to the file being loaded, so they are
	//	restored only on this whole-dataset path, never when appending into
	//	an existing dataset that already has its own.
	_pq_write_notes("_dta", _pq_metadata_data_notes)
}

void _pq_apply_stata_metadata_newvars(
	string scalar target_vars_local,
	string scalar existing_vars_local,
	real scalar allow_recast, string scalar recast_skip_local)
{
	external string colvector _pq_metadata_variable_labels
	external string colvector _pq_metadata_value_label_names
	external transmorphic scalar _pq_metadata_label_values
	external transmorphic scalar _pq_metadata_label_texts
	external string colvector _pq_metadata_formats
	external real colvector _pq_metadata_is_string
	external transmorphic scalar _pq_metadata_notes
	external string colvector _pq_metadata_types

	string rowvector target_vars, existing_vars, skip_vars
	string scalar value_label_name
	real colvector values
	string colvector texts
	real scalar i, variable_index, is_new

	target_vars = tokens(st_local(target_vars_local))
	existing_vars = tokens(st_local(existing_vars_local))
	skip_vars = tokens(st_local(recast_skip_local))
	//	Every captured attribute is a parallel array indexed by capsule
	//	position.  Check all of them, not just the labels, so a vector left
	//	stale or short can never be read against the wrong variable.
	if (cols(target_vars) != rows(_pq_metadata_variable_labels) |
		cols(target_vars) != rows(_pq_metadata_value_label_names) |
		cols(target_vars) != rows(_pq_metadata_formats) |
		cols(target_vars) != rows(_pq_metadata_is_string) |
		cols(target_vars) != rows(_pq_metadata_types)) {
		errprintf("Invalid Stata metadata target map\n")
		_error(198)
	}

	for (i = 1; i <= cols(target_vars); i++) {
		variable_index = st_varindex(target_vars[i])
		if (variable_index == .) {
			errprintf("Stata metadata target variable not found: %s\n", target_vars[i])
			_error(198)
		}
		value_label_name = _pq_metadata_value_label_names[i]
		if (value_label_name != "") {
			if (st_isstrvar(variable_index)) {
				is_new = !anyof(existing_vars, target_vars[i])
				if (is_new) {
					errprintf("Cannot attach numeric value label to string variable: %s\n", target_vars[i])
					_error(198)
				}
			}
		}
	}

	for (i = 1; i <= cols(target_vars); i++) {
		value_label_name = _pq_metadata_value_label_names[i]
		if (value_label_name != "") {
			if (!st_vlexists(value_label_name)) {
				values = asarray(_pq_metadata_label_values, value_label_name)
				texts = asarray(_pq_metadata_label_texts, value_label_name)
				st_vlmodify(value_label_name, values, texts)
			}
		}
	}
	for (i = 1; i <= cols(target_vars); i++) {
		is_new = !anyof(existing_vars, target_vars[i])
		if (is_new) {
			if (allow_recast &
				!_pq_recast_excluded(target_vars[i], skip_vars)) {
				_pq_restore_storage_type(target_vars[i],
					_pq_metadata_types[i])
			}
			variable_index = st_varindex(target_vars[i])
			st_varlabel(variable_index, _pq_metadata_variable_labels[i])
			if (_pq_metadata_formats[i] != "" &
				st_isstrvar(variable_index) == _pq_metadata_is_string[i]) {
				st_varformat(variable_index, _pq_metadata_formats[i])
			}
			_pq_write_notes(target_vars[i],
				asarray(_pq_metadata_notes, strofreal(i)))
			value_label_name = _pq_metadata_value_label_names[i]
			if (value_label_name != "") {
				st_varvaluelabel(variable_index, value_label_name)
			}
		}
	}
}
end

capture program drop pq_use_sas
program define pq_use_sas
	if strpos(`"`0'"', ",") > 0 {
		pq_use_append `0' format(sas)
	}
	else {
		pq_use_append `0', format(sas)
	}
end

capture program drop pq_use_spss
program define pq_use_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_use_append `0' format(spss)
	}
	else {
		pq_use_append `0', format(spss)
	}
end

capture program drop pq_use_csv
program define pq_use_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_use_append `0' format(csv)
	}
	else {
		pq_use_append `0', format(csv)
	}
end

capture program drop pq_save_spss
program define pq_save_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_save `0' format(spss)
	}
	else {
		pq_save `0', format(spss)
	}
end

capture program drop pq_save_csv
program define pq_save_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_save `0' format(csv)
	}
	else {
		pq_save `0', format(csv)
	}
end

capture program drop pq_describe_sas
program define pq_describe_sas
	if strpos(`"`0'"', ",") > 0 {
		pq_describe `0' format(sas)
	}
	else {
		pq_describe `0', format(sas)
	}
end

capture program drop pq_describe_spss
program define pq_describe_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_describe `0' format(spss)
	}
	else {
		pq_describe `0', format(spss)
	}
end

capture program drop pq_describe_csv
program define pq_describe_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_describe `0' format(csv)
	}
	else {
		pq_describe `0', format(csv)
	}
end

capture program drop pq_merge_sas
program define pq_merge_sas
	if strpos(`"`0'"', ",") > 0 {
		pq_merge `0' format(sas)
	}
	else {
		pq_merge `0', format(sas)
	}
end

capture program drop pq_merge_spss
program define pq_merge_spss
	if strpos(`"`0'"', ",") > 0 {
		pq_merge `0' format(spss)
	}
	else {
		pq_merge `0', format(spss)
	}
end

capture program drop pq_merge_csv
program define pq_merge_csv
	if strpos(`"`0'"', ",") > 0 {
		pq_merge `0' format(csv)
	}
	else {
		pq_merge `0', format(csv)
	}
end



capture program drop pq_merge
program pq_merge
    version 16.0
    
    gettoken mtype 0 : 0, parse(" ,")
	local origmtype `"`mtype'"'
	//	di "mtype: `mtype'"
	local varlist_n
	/* ------------------------------------------------------------ */
				/* parsing				*/
				/* we have pulled off <mtype> from 0	*/
	gettoken token : 0, parse(" ,")
	if ("`token'"=="_n") {
		if ("`mtype'"!="1:1") {
			error_seq_not11 "`mtype'" "`origmtype'"
			/*NOTREACHED*/
		}
		gettoken token 0 : 0, parse(" ,")
		local varlist_n _n
	}

	syntax [varlist(default=none)] using/ [,	///
		  ASSERT(string)			///
		  GENerate(name)			///
		  FORCE					///
		  KEEP(string)				///
		  KEEPUSing(string)			///
		noLabels				///
		  NOGENerate			        ///
		noNOTEs					///
		  REPLACE				///
		noREPort				///
		  SORTED				///
		  UPDATE       				///
		  in(string) 				///
		if(string asis) 		///
		relaxed 				///
		asterisk_to_variable(string)	///
		sort(string)			///
		compress				///
		compress_string_to_numeric	///
			random_n(integer 0)		///
			random_share(real 0.0)	///
			random_seed(integer 0)	///
			batch_size(string)	///
			infer_schema_length(integer 10000)	///
			parse_dates				///
			preserve_order			///
			drop(string)			///
			drop_strl					///
			format(string)			///
		]



	pq_convert_path `"`using'"'
	local using = r(fullpath)
	if "`keepusing'" != "" {
		if ("`varlist_n'" == "_n")	local using_vars `keepusing'
		else 						local using_vars `varlist' `keepusing' 
	}
	else {
		local using_vars
	}
	local format_opt
	if ("`format'" != "") local format_opt format(`format')
	local batch_size_opt
	if ("`batch_size'" != "") local batch_size_opt batch_size(`batch_size')

	tempfile t_save
	tempname f_pq
	frame create `f_pq'
	frame `f_pq' {
		pq use `using_vars' using `"`using'"', 	clear in(`in') 					///
												if(`if') 						///
												`relaxed' 						///
												asterisk_to_variable(`asterisk_to_variable')	///
																			sort(`varlist')					///
												`compress'						///
												`compress_string_to_numeric'	///
												random_n(`random_n')			///
												random_share(`random_share')	///
												random_seed(`random_seed')		///
												`batch_size_opt'				///
												infer_schema_length(`infer_schema_length')	///
												`parse_dates'				///
												`preserve_order'				///
												`format_opt'					///
												drop(`drop')					///
												`drop_strl'
		quietly save `t_save'
	}
	/*
	di `"merge `origmtype' `varlist_n'`varlist' using "`t_save'",	gen(`generate') 	///"'
	di `"												`nogenerate'			///"'
	di `"												`nolabel'				///"'
	di `"												`nonotes'				///"'
	di `"												`update'				///"'
	di `"												`replace'				///"'
	di `"												`noreport'				///"'
	di `"												`force'					///"'
	di `"												assert(`assert')		///"'
	di `"												keep(`keep')"'
	*/

	
	di "Merging to data"
	merge `origmtype' `varlist_n'`varlist' using "`t_save'",	gen(`generate') 	///
													`nogenerate'			///
													`labels'				///
													`notes'					///
													`update'				///
													`replace'				///
													`report'				///
													`sorted'				///
													`force'					///
													assert(`assert')		///
													keep(`keep')



	frame drop `f_pq'

end


capture program drop pq_use_append
program define pq_use_append, nclass
	    version 16.0
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	local pq_metadata_capsule
	capture noisily {

    local input_args = `"`0'"'

	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local namelist = substr(`"`input_args'"', 1, `using_pos'-1)
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
	}
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
    
        // namelist is empty since no "using" separator
        local namelist ""
    }
	
	syntax using/ [, 	in(string) 				///
						if(string asis) 		///
						relaxed 				///
						asterisk_to_variable(string)	///
										sort(string)			///
						compress				///
						compress_string_to_numeric	///
						clear					///
						random_n(integer 0)		///
						random_share(real 0.0)	///
						random_seed(integer 0)	///
						infer_schema_length(integer 10000)	///
						parse_dates				///
						batch_size(string)	///
						max_obs_per_batch(integer 0)	///
						preserve_order			///
						drop(string)			///
						drop_strl				///
						format(string)			///
						fast					///
						append				///
						cast(string asis)		///
						lax				///
						safe_int64			///
						NOSTATAMETADATA		///
						binary_to_string]

	local pq_namelist_buf `"`namelist'"'
		
	pq_register_plugin
	
	pq_convert_path `"`using'"'
	local using = r(fullpath)
	pq_infer_format, path("`using'") format("`format'") preserveexistingdir
	local source_format = r(format)
	local using `"`r(path)'"'
	if !inlist("`source_format'", "parquet", "sas", "spss", "csv") {
		display as error `"Unsupported format(`format'): expected parquet, sas, spss, or csv"'
		exit 198
	}
	
	local b_append = "`append'" != ""

	
	if (`=_N' > 0 & !`b_append' & "`clear'" == "") {
		display as error "There is already data loaded, pass clear if you want to load a file"
		exit 2000
	}

	if (`random_share' > 1) {
		display as error `"Cannot set random_share > 1 (`random_share')"'
		exit 198
	}

	if (`infer_schema_length' < 0) {
		display as error `"infer_schema_length() must be >= 0, passed `infer_schema_length'"'
		exit 198
	}
	if ("`batch_size'" != "") {
		capture confirm integer number `batch_size'
		if (_rc) {
			display as error `"batch_size() must be a positive integer, passed `batch_size'"'
			exit 198
		}
		local batch_size_num = real("`batch_size'")
		if (`batch_size_num' <= 0) {
			display as error `"batch_size() must be > 0, passed `batch_size'"'
			exit 198
		}
	}

	if ("`source_format'" != "parquet") {
		if ("`relaxed'" != "") {
			display as error "relaxed is only supported for parquet input"
			exit 198
		}
		if ("`asterisk_to_variable'" != "") {
			display as error "asterisk_to_variable() is only supported for parquet input"
			exit 198
		}
	}

	local b_preserve_order = "`preserve_order'" != ""
	if (`b_preserve_order' & !inlist("`source_format'", "sas", "spss")) {
		di as text "note: preserve_order ignored for format(`source_format'); only used for sas/spss reads."
		local b_preserve_order = 0
	}
	local b_parse_dates = "`parse_dates'" != ""
	pq_normalize_csv_opts, source_format(`source_format') infer_schema_length(`infer_schema_length') b_parse_dates(`b_parse_dates')
	local infer_schema_length_for_plugin = r(infer_schema_length_for_plugin)
	local parse_dates_for_plugin = r(parse_dates_for_plugin)
	local b_fast = "`fast'" != ""
	local preflight_stata_metadata = "`source_format'" == "parquet" & ///
		"`nostatametadata'" == ""
	local batch_size_for_plugin -1
	if ("`batch_size'" != "") local batch_size_for_plugin = real("`batch_size'")

	// Set default for max_obs_per_batch if not specified
	if (`max_obs_per_batch' == 0) {
		local max_obs_per_batch = 2147483647  // i32::MAX
	}

	if ("`in'" != "") {
		local offset = substr("`in'", 1, strpos("`in'", "/") -1)
		local offset = max(`offset',0)
		local last_n = substr("`in'", strpos("`in'", "/") + 1, .)
	}
	else {
		local offset = 0
		local last_n = 0
	}
	
	//	Process the if statement, if passed
	if (`"`if'"' != "") {
		//	Detect Stata date functions for parquet only.
		if ("`source_format'" == "parquet") {
			if (regexm(`"`if'"', "t[cdwmqhC]\(")) {
				di as error "if() expression contains a Stata date function (td, tc, tC, tw, tm, tq, or th)."
				di as error "Parquet dates use Unix epoch (01jan1970); Stata date functions use 01jan1960."
				di as error "Use Polars date/datetime literals instead, e.g.:"
				di as error `"  %td (daily date): if(date_col >= date('01jan2020','%d%b%Y'))"'
				di as error `"  %tc (datetime):   if(dt_col >= TIMESTAMP '2020-01-01 00:00:00')"'
				exit 198
			}
		}
		local greater_than = strpos(`"`if'"', ">") > 0
		if (`greater_than') {
			di as error "pq will interpret > as in SQL, which is different than Stata."
			di as error "	It will not include . as > any value."
		}
		//	di `"plugin call polars_parquet_plugin, if "`if'""'
		plugin call polars_parquet_plugin, if `"`if'"'
		if ("`sql_if'" != "" & inlist("`source_format'", "sas", "spss", "csv")) {
			di as text "note: sql_if on `source_format' currently scans source data twice (describe + read); this can be slow on large files."
		}
	}
	else {
		local sql_if
	}
	
	//	Initialize "mapping" to tell plugin to read from macro variables
	local mapping from_macros
	local b_quiet = 1
	local b_detailed = 1
	local b_compress = "`compress'" != ""
	local b_compress_string_to_numeric = "`compress_string_to_numeric'" != ""
	
	//	di `"plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "`sql_if'" "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric'"'
	// Rust resolves wildcards and applies drop() inside file_summary(), then sets
	// matched_vars. drop_strl columns (binary parquet type) are filtered below.
	local b_binary_to_string = ("`binary_to_string'" != "")
	local b_cast_strict = ("`lax'" == "")
	local b_safe_int64 = ("`safe_int64'" != "")
	local pq_cast_buf `cast'
	// Validate footer uniformity before describe can scan or cache source data.
	// Name mapping still occurs after describe establishes physical/final names.
	if (`preflight_stata_metadata') {
		capture noisily plugin call polars_parquet_plugin, validate_stata_metadata ///
			"`using'"
		local _metadata_rc = _rc
		if (`_metadata_rc') exit `_metadata_rc'
	}
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' `"`sql_if'"' "`asterisk_to_variable'" `b_compress' `b_compress_string_to_numeric' "`source_format'" `infer_schema_length_for_plugin' `parse_dates_for_plugin' `b_fast' 100 "pq_namelist_buf" "`drop'" "pq_cast_buf" `b_binary_to_string' `b_cast_strict' `b_safe_int64'
	if (_rc) {
		if (`"`pq_cast_error'"' != "") di as error "`pq_cast_error'"
		exit _rc
	}

	local vars_in_file
	local n_renamed = 0
	forvalues i = 1/`n_columns' {
		local vars_in_file `vars_in_file' `name_`i''

		local renamei `rename_`i''
		if ("`renamei'" != "") {
			local n_renamed = `n_renamed' + 1
			local rename_from_`n_renamed' `name_`i''
			local rename_to_`n_renamed' `renamei'
		}
	}

	// matched_vars is set by the Rust describe plugin (wildcards expanded, drop applied).
	// Handle drop_strl separately: remove binary ("strl") columns identified from schema.
	if "`drop_strl'" == "drop_strl" {
		local strl_drop_list
		forvalues i = 1/`n_columns' {
			if "`type_`i''" == "strl" {
				local strl_drop_list `strl_drop_list' `name_`i''
			}
		}
		if "`strl_drop_list'" != "" {
			local new_matched
			foreach vari in `matched_vars' {
				if !`:list vari in strl_drop_list' {
					local new_matched `new_matched' `vari'
				}
			}
			local matched_vars `new_matched'
			local matched_var_count : word count `matched_vars'
			if (`matched_var_count' > 0) {
				forvalues i = 1/`matched_var_count' {
					local matched_var_`i' : word `i' of `matched_vars'
				}
			}
		}
	}

	// Validate every selected Parquet footer before clearing or extending data.
	// nostatametadata skips both this preflight and later restoration.
	if (`preflight_stata_metadata') {
		tempfile pq_metadata_capsule
		capture noisily plugin call polars_parquet_plugin, extract_stata_metadata ///
			"`using'" "`pq_metadata_capsule'"
		local _metadata_rc = _rc
		if (`_metadata_rc') exit `_metadata_rc'
	}

	if (!`b_append' & "`clear'" != "") clear

	local match_all = ("`namelist'" == "" | "`namelist'" == "*") & "`drop'" == ""
	
	//	Get the list of already existing variables
	capture unab all_vars: *
	local n_vars_already : word count `all_vars'
	
	//	Create the empty data, if needed, or add rows, if needed
	if (`last_n' == 0)	local last_n = `n_rows'
	local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))

	if (`random_n' > `row_to_read') {
		di "random_n (`random_n') > number of rows to read (`row_to_read')"
	}

	if (`random_n' > 0 & `random_n' < `row_to_read') {
		local random_share = `random_n'/`row_to_read'
		local row_to_read = `random_n'
	}
	else if (`random_share' > 0)						local row_to_read = floor(`random_share'*`row_to_read')

	//	di "local row_to_read = max(0,min(`n_rows',`last_n') - `offset' + (`offset' > 0))"

	local n_obs_already = _N

	//	Build list of strL column names from describe output.
	//	A column is treated as strL if ANY of:
	//	  (a) parquet describe says type is "strl"
	//	  (b) parquet max string length > 2045 bytes (DTA_MAX_STR)
	//	  (c) the existing Stata variable is already strL (for append)
	local strl_col_names
	local non_strl_matched_vars
	foreach vari in `matched_vars' {
		local var_number: list posof "`vari'" in vars_in_file
		local typei `type_`var_number''
		local str_len_i `string_length_`var_number''

		//	Check if existing Stata variable is strL (Stata returns "strL" with capital L)
		local ti
		capture confirm variable `vari', exact
		if _rc == 0 {
			local ti : type `vari'
		}

		if (("`typei'" == "strl") | (`str_len_i' > 2045) | (lower("`ti'") == "strl")) {
			local strl_col_names `strl_col_names' `vari'
		}
		else {
			local non_strl_matched_vars `non_strl_matched_vars' `vari'
		}
	}

	//	Tell polars to concatenate file list with "vertical_relaxed"
	local vertical_relaxed = "`relaxed'" != ""
	local offset_for_plugin = max(0,`offset' - 1)

	//	Check if batching is needed due to observation limit
	local needs_batching = (`row_to_read' > `max_obs_per_batch')

	if (`needs_batching' & `random_share' > 0) {
		di as error "random_n/random_share is not supported when the dataset exceeds `max_obs_per_batch' rows (overflow batching)."
		exit 198
	}

	if (`needs_batching') {
		//	Large dataset detected - split into two batches
		display as text "Large dataset detected: `row_to_read' rows > `max_obs_per_batch' limit"
		display as text "Processing in 2 batches..."

		//	BATCH 1: First max_obs_per_batch rows via normal flow
		local row_to_read_first = `max_obs_per_batch'
		local original_row_to_read = `row_to_read'
		local row_to_read = `row_to_read_first'
	}

	//	Handle strL columns via .dta if any exist
	//	The strl .dta is written as a side effect of the read plugin call below,
	//	so sampling is guaranteed consistent between strl and non-strl columns.
	local has_strl = "`strl_col_names'" != ""
	local temp_strl_dta
	if (`has_strl') {
		tempfile temp_strl_tmp
		local temp_strl_dta : subinstr local temp_strl_tmp ".tmp" ".dta", all
		if ("`temp_strl_dta'" == "`temp_strl_tmp'") {
			local temp_strl_dta "`temp_strl_tmp'.dta"
		}
	}

	//	Detect all-strL append: when every matched variable is strL, the plugin
	//	writes only temp_strl_dta (no Stata matrix writes) so we skip `set obs'.
	//	Blob collision is prevented by the Rust writer using n_obs_already as an
	//	offset for the `o` identifier, so the new blobs start at n_obs_already+1.
	local n_strl_matched: word count `strl_col_names'
	local n_matched_total: word count `matched_vars'
	local all_strl_append = (`b_append' & `n_matched_total' > 0 & `n_matched_total' == `n_strl_matched')

	local n_obs_after = `n_obs_already' + `row_to_read'
	if (!`all_strl_append') {
		quietly set obs `n_obs_after'
	}

	local match_vars_non_binary

	local dropped_vars = 0

	local var_position = 0
	local rename_count = 0
	local rename_list
	local strl_rename_count = 0
	foreach vari in `matched_vars' {
		local var_position = `var_position' + 1
		local var_number: list posof "`vari'" in vars_in_file
		local type `type_`var_number''
		local string_length `string_length_`var_number''

		//	Set rename_to to nothing
		local rename_to

		//	Does it need to be renamed?
		local name_to_create `vari'
		forvalues i = 1/`n_renamed' {
			local rename_from `rename_from_`i''

			if ("`vari'" == "`rename_from'") {
				local rename_to `rename_to_`i''
				local name_to_create `rename_to'
				continue, break
			}
		}

		//	Skip columns designated as strL - they are loaded via .dta.
		//	This includes parquet type "strl" and promoted long strings.
		local is_strl_col : list posof "`vari'" in strl_col_names
		if ("`type'" == "strl" | `is_strl_col' > 0) {
			// Defer characteristics until the strL .dta has created the variable.
			if ("`rename_to'" != "") {
				local rename_list `rename_list' `name_to_create'
				local rename_count = `rename_count' + 1
				local rename_from_`rename_count' `vari'
				local strl_rename_count = `strl_rename_count' + 1
				local strl_rename_target_`strl_rename_count' `name_to_create'
				local strl_rename_source_`strl_rename_count' `vari'
			}
			//	Don't add strL to match_vars_non_binary - they're not sent to read plugin
			continue
		}

		pq_gen_or_recast,	name(`name_to_create')			///
							type_new(`type')				///
							str_length(`string_length')

		local keep = 1

		if ("`type'" == "datetime") {
			format `name_to_create' %tc
		}
		else if ("`type'" == "date") {
			format `name_to_create' %td
		}
		else if ("`type'" == "time") {
			format `name_to_create' %tchh:mm:ss
		}
		else if ("`type'" == "binary") {
			di "Dropping `name_to_create' as cannot process binary columns"
			local keep = 0
		}

		if ("`rename_to'" != "") {
			local rename_list `rename_list' `name_to_create'
			local rename_count = `rename_count' + 1
			local rename_from_`rename_count' `vari'

			char `name_to_create'[_pq_parquet_name] `"`vari'"'
		}

		if (`keep') {
			local match_vars_non_binary `match_vars_non_binary' `vari'
		}
	}


	//	Make a list of the loaded variables (excluding strL)
	local n_matched_vars: word count `match_vars_non_binary'

	local i = 0
	if `n_matched_vars' > 0 foreach vari of varlist * {
		//	Actual variable index
		local i = `i' + 1

		//	vari is the final name, but if it was renamed, we
		//		need to get the original value to get the index
		//		of the variable in the original list
		local i_rename : list posof "`vari'" in rename_list
		if (`i_rename' > 0)		local vari_original `rename_from_`i_rename''
		else					local vari_original `vari'


		//	Index of the actual new variables (possible != i for append)
		local i_matched : list posof "`vari_original'" in match_vars_non_binary

		if (`i_matched' > 0) {
			local v_to_read_index_`i_matched' `i'
			//	Name used to look up the column in the Polars batch, which always
			//	keeps the original (possibly >32 char) parquet column name - not
			//	the (possibly truncated/renamed) Stata variable name `vari'.
			local v_to_read_name_`i_matched' `vari_original'
			local v_to_read_type_`i_matched': type `vari'
			local v_to_read_type_`i_matched' = lower("`v_to_read_type_`i_matched''")
			//	For getting the polars and polars assigned stata type and passing back to read
			local i_original : list posof "`vari_original'" in vars_in_file


			//	Get the originally set stata type
			local v_to_read_type_`i_matched' `type_`i_original''
			//	Get the polars type from the earlier list
			local v_to_read_p_type_`i_matched' `polars_type_`i_original''

			//	display "`v_to_read_index_`i_matched'': `v_to_read_name_`i_matched'', `v_to_read_type_`i_matched'', `v_to_read_p_type_`i_matched''"
		}
	}

	local offset = `offset_for_plugin'

	//	asterisk_to_variable - for files /file/*.parquet, convert
	//		* to a variable, so /file/2019.parquet, file/2020.parquet
	//		will have the item in asterisk_to_variable as 2019 and 2020
	//		for the records on the file

	//	strl col names and dta path are passed so the plugin writes the strl .dta
	//	in the same scan as the non-strl columns (consistent sampling)
	capture noisily plugin call polars_parquet_plugin, read "`using'" "from_macro" `row_to_read' `offset' `"`sql_if'"' `"`mapping'"' `vertical_relaxed' "`asterisk_to_variable'" "`sort'" `n_obs_already' `random_share' `random_seed' `batch_size_for_plugin' "`strl_col_names'" "`temp_strl_dta'" "`source_format'" `b_preserve_order' `infer_schema_length_for_plugin' `parse_dates_for_plugin'
	local _read_rc = _rc
	if (`_read_rc') {
		if (`b_append' & !`all_strl_append' & `n_obs_already' < _N) {
			quietly keep in 1/`n_obs_already'
		}
		if (`"`pq_cast_error'"' != "") di as error "`pq_cast_error'"
		exit `_read_rc'
	}

	//	Merge strL columns from .dta written by plugin into the current dataset
	//	The .dta always contains _pq_strl_key (1-based row index) added by Rust.
	if (`has_strl') {
		if (`n_matched_vars' == 0) {
			//	All variables are strL; `set obs' was skipped for the append path.
			if (!`b_append') {
				//	Non-append: dataset is empty, load the strL .dta directly.
				quietly use "`temp_strl_dta'", clear
			}
			else {
				//	All-strL append: blob `o` values in temp_strl_dta are offset by
				//	n_obs_already (done in Rust), so they can't collide with the master.
				quietly append using "`temp_strl_dta'"
			}
			capture erase "`temp_strl_dta'"
		}
		else if (!`b_append') {
			//	Mixed strL + non-strL, non-append: gen key=_n, merge, drop key.
			quietly capture drop _pq_strl_key
			quietly gen long _pq_strl_key = _n
			quietly merge 1:1 _pq_strl_key using "`temp_strl_dta'", nogen
		}
		else {
			//	Mixed strL + non-strL, append: gen key=_n, merge update, drop key.
			quietly capture drop _pq_strl_key
			quietly gen long _pq_strl_key = _n
			quietly merge 1:1 _pq_strl_key using "`temp_strl_dta'", update nogen
		}
		//	Drop the key column that Rust added (present in all paths after use/merge/append)
		quietly capture drop _pq_strl_key
		capture erase "`temp_strl_dta'"

		if (`strl_rename_count' > 0) {
			forvalues i = 1/`strl_rename_count' {
				local strl_target `strl_rename_target_`i''
				local strl_source `strl_rename_source_`i''
				confirm variable `strl_target', exact
				char `strl_target'[_pq_parquet_name] `"`strl_source'"'
			}
		}

		//	Restore original column order
		local ordered_vars
		foreach vari in `matched_vars' {
			local rname
			forvalues ri = 1/`rename_count' {
				if ("`vari'" == "`rename_from_`ri''") {
					local pos : list posof "`rename_from_`ri''" in rename_list
					if (`pos' > 0) {
						local rname : word `pos' of `rename_list'
					}
				}
			}
			if ("`rname'" != "") {
				local ordered_vars `ordered_vars' `rname'
			}
			else {
				local ordered_vars `ordered_vars' `vari'
			}
		}
		capture order `ordered_vars'
	}

	//	BATCH 2: Overflow rows via .dta append (if batching was needed)
	if (`needs_batching') {
		display as text "Batch 1 complete. Processing overflow batch..."

		//	Calculate overflow parameters
		local overflow_offset = `offset_for_plugin' + `max_obs_per_batch'
		local overflow_count = `original_row_to_read' - `max_obs_per_batch'

		//	Create temp file for overflow .dta
		tempfile temp_overflow_tmp
		local temp_overflow_dta : subinstr local temp_overflow_tmp ".tmp" ".dta", all
		if ("`temp_overflow_dta'" == "`temp_overflow_tmp'") {
			local temp_overflow_dta "`temp_overflow_tmp'.dta"
		}

		//	Build column list for overflow (all matched vars, including strL)
		local overflow_columns `matched_vars'

		//	Set up relax option for overflow call
		if ("`relaxed'" != "") {
			local relax_opt "relax"
		}
		else {
			local relax_opt ""
		}

		//	Call helper to write overflow batch to .dta
		pq_write_overflow_dta, using("`using'") output("`temp_overflow_dta'") ///
			offset(`overflow_offset') n_rows(`overflow_count') ///
			columns("`overflow_columns'") if_clause(`"`sql_if'"') ///
			`relax_opt' asterisk_to_variable("`asterisk_to_variable'") ///
			random_share(`random_share') random_seed(`random_seed') format(`source_format') ///
			infer_schema_length(`infer_schema_length_for_plugin') ///
			parse_dates(`parse_dates_for_plugin')

		//	Append the overflow .dta
		quietly append using "`temp_overflow_dta'"
		capture erase "`temp_overflow_dta'"

		display as text "Overflow batch complete. Total rows loaded: `=_N'"
	}

	// Apply the one agreed capsule only after all selected column data loaded.
	// The capsule is data, never Stata code.
	if (`preflight_stata_metadata') {
		if ("`pq_meta_present'" == "1" & `pq_meta_count' > 0) {
			local pq_capsule_vars
			local pq_target_vars
			local pq_all_capsule_vars
			forvalues i = 1/`pq_meta_all_count' {
				local pq_all_capsule_vars `pq_all_capsule_vars' `pq_meta_all_capsule_`i''
			}
			forvalues i = 1/`pq_meta_count' {
				local pq_capsule_vars `pq_capsule_vars' `pq_meta_capsule_`i''
				local pq_target_vars `pq_target_vars' `pq_meta_target_`i''
			}
			//	compress and compress_string_to_numeric restate every
			//	column's type, so they suppress the recast wholesale.  cast()
			//	names specific columns, so only those are exempted and every
			//	other column still gets its saved type back.
			local pq_norecast
			if ("`compress'" != "" | "`compress_string_to_numeric'" != "") {
				local pq_norecast norecast
			}
			local pq_recast_skip
			if (`"`cast'"' != "") {
				//	cast(string asis) keeps the caller's compound-quote
				//	delimiters in the macro value.  Assigning bare, as the
				//	plugin call does, strips them; keeping them would
				//	mis-pair every quote in the JSON payload.
				local pq_cast_payload `cast'
				mata: _pq_cast_targets("pq_cast_payload", "pq_recast_skip")
			}
			if (`b_append') {
				capture noisily pq_restore_stata_metadata_append, ///
					capsule("`pq_metadata_capsule'") ///
					allcapsulevars("`pq_all_capsule_vars'") ///
					capsulevars("`pq_capsule_vars'") ///
					targetvars("`pq_target_vars'") ///
					existingvars("`all_vars'") `pq_norecast' ///
					recastskip("`pq_recast_skip'")
			}
			else {
				capture noisily pq_restore_stata_metadata, ///
					capsule("`pq_metadata_capsule'") ///
					allcapsulevars("`pq_all_capsule_vars'") ///
					capsulevars("`pq_capsule_vars'") ///
					targetvars("`pq_target_vars'") `pq_norecast' ///
					recastskip("`pq_recast_skip'")
			}
			local _metadata_rc = _rc
			capture erase "`pq_metadata_capsule'"
			if (`_metadata_rc') exit `_metadata_rc'
		}
		else {
			capture erase "`pq_metadata_capsule'"
		}
	}
	quietly version
	}
	local rc = _rc
	if ("`pq_metadata_capsule'" != "") capture erase "`pq_metadata_capsule'"
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
end

capture program drop pq_gen_or_recast
program pq_gen_or_recast
	version 16
	syntax  ,	name(string)				///
			 	type_new(string)			///
				str_length(integer)
	
	local string_length = max(1,`str_length')
	if ("`type_new'" == "datetime")			local type_new double
	else if ("`type_new'" == "time")		local type_new double
	else if ("`type_new'" == "date")		local type_new long
	else if ("`type_new'" == "string")		local type_str str`string_length'
	
	capture confirm variable `name', exact
	local b_gen = _rc > 0

	local vartype
	if (!`b_gen')	local vartype: type `name'

	//	di _newline(2)
	//	di "name: 		`name'"
	//	di "type_new: 		`type_new'"
	//	di "string_length:	`string_length'"
	//	di "vartype: 		`vartype'"
	
	if ("`type_new'" == "string") {
		if `b_gen' {
			quietly gen `type_str' `name' = ""
		}
		else {
			// Check if it's a fixed-length string type (str1, str2, etc.)
			if regexm("`vartype'", "^str([0-9]+)$") {
				local current_length = regexs(1)
				
				if `string_length' > `current_length' {
					recast str`string_length' `name'
				}
			}
			else if inlist("`vartype'", "byte", "int", "long", "float", "double") {
				tostring `name', replace force
			}
		}
	}
	else if ("`type_new'" == "strl") {
		if `b_gen' {
			quietly gen strL `name' = ""
		}
		else {
			// Check if it's a fixed-length string type (str1, str2, etc.)
			if regexm("`vartype'", "^str([0-9]+)$") {
				recast strL `name'
			}
			else if inlist("`vartype'", "byte", "int", "long", "float", "double") {
				tostring `name', replace force
				recast strL `name'
			}
		}
	}
	else if ("`type_new'" == "float") {
		if `b_gen' {
			quietly gen float `type' `name' = .
		}
		else {
			if inlist("`vartype'", "long","double") {
				recast double `name'
			}
			else if inlist("`vartype'", "byte", "int") {
				recast float `name'
			}
		}
	}
	else if ("`type_new'" == "long") {
		if `b_gen' {
			quietly gen long `name' = .
		}
		else {
			if inlist("`vartype'", "byte", "int") {
				recast long `name'
			}
			else if inlist("`vartype'", "float") {
				recast double `name'
			}
		}
	}
	else if ("`type_new'" == "int") {
		if `b_gen' {
			quietly gen int `name' = .
		}
		else {
			if inlist("`vartype'", "byte") {
				recast int `name'
			}
		}
	}
	else if ("`type_new'" == "byte") {
		if `b_gen' {
			quietly gen byte `name' = .
		}
		else {
			if inlist("`vartype'", "int","long","float","double") {
				recast `vartype' `name'
			}
		}
	}
	else if ("`type_new'" == "binary") {
		di "Dropping `name' as cannot process binary columns"
	}
	else {
		if `b_gen' {
			quietly gen double `name' = .
		}
		else {
			if inlist("`vartype'", "byte", "int", "long", "float") {
				recast double `name'
			}
		}
	}
end

capture program drop pq_describe
program pq_describe, rclass
    version 16.0
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	capture noisily {

	local input_args = `"`0'"'

	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local pre_using = substr(`"`input_args'"', 1, `using_pos'-1)

		if `"`pre_using'"' != "" {
			di as error "varlist not allowed"
			error 101
		}
        local rest = substr(`"`input_args'"', `using_pos'+6, .)
		local 0 = `"using `rest'"'
    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"using `input_args'"'
        
        // As intended, pre_using needs to be blank

    }

    // Parse syntax
    syntax  using/, 					///
			[quietly					///
			 detailed					///
			 asterisk_to_variable(string) ///
			 format(string)				///
			 infer_schema_length(integer 10000) ///
			 parse_dates]

	pq_register_plugin
	local b_quiet = ("`quietly'" != "")
	local b_detailed = ("`detailed'" != "")
	
	pq_convert_path `"`using'"'
	local using = r(fullpath)
	pq_infer_format, path("`using'") format("`format'") preserveexistingdir
	local source_format = r(format)
	local using `"`r(path)'"'
	if !inlist("`source_format'", "parquet", "sas", "spss", "csv") {
		display as error `"Unsupported format(`format'): expected parquet, sas, spss, or csv"'
		exit 198
	}
	if ("`source_format'" != "parquet" & "`asterisk_to_variable'" != "") {
		display as error "asterisk_to_variable() is only supported for parquet input"
		exit 198
	}
	if (`infer_schema_length' < 0) {
		display as error `"infer_schema_length() must be >= 0, passed `infer_schema_length'"'
		exit 198
	}
	local b_parse_dates = "`parse_dates'" != ""
	pq_normalize_csv_opts, source_format(`source_format') infer_schema_length(`infer_schema_length') b_parse_dates(`b_parse_dates')
	local infer_schema_length_for_plugin = r(infer_schema_length_for_plugin)
	local parse_dates_for_plugin = r(parse_dates_for_plugin)

	//	Trailing zeros are compress indicators
	plugin call polars_parquet_plugin, describe "`using'" `b_quiet' `b_detailed' "" "`asterisk_to_variable'" 0 0 "`source_format'" `infer_schema_length_for_plugin' `parse_dates_for_plugin'

	
	local macros_to_return n_rows n_columns //	mapping
	forvalues i = 1/`n_columns' {
		local macros_to_return `macros_to_return' type_`i' name_`i' rename_`i' 
		
		if (`b_detailed')	local macros_to_return `macros_to_return' string_length_`i'
		
	}
	
	foreach maci in `macros_to_return' {
		return local `maci' = `"``maci''"'
	}
	}
	local rc = _rc
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
end




capture program drop pq_save
program define pq_save, nclass
	version 16.0
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	local original_frame
	local save_for_chunks
	local _stream_saved = 0
	capture noisily {

	
    local input_args = `"`0'"'
    //	di `"`input_args"'
	// Check if "using" is present in arguments
    local using_pos = strpos(`" `input_args' "', " using ")
    
    if `using_pos' > 0{
        // 	Extract everything before "using"
        local varlist = substr(`"`input_args'"', 1, `using_pos'-1)
		if (strtrim("`varlist'") == "")	local varlist *

		local rest = substr(`"`input_args'"', `using_pos'+6, .)

		local 0 = `"`varlist' using `rest'"'
    }
    else {
        // No "using" - parse everything as filename and options
        local 0 = `"* using `input_args'"'
        
        // namelist is empty since no "using" separator
    }

	syntax varlist using/ [, replace 						///
						   if(string asis) 					///
						   NOAUTORENAME						///
						   partition_by(varlist)			///
						   compression(string)				///
						   compression_level(integer -1)	///
						   NOPARTITIONOVERWRITE				///
						   compress							///
						   compress_string_to_numeric		///
						   chunk(integer 2147483647)		///
						   stream							///
						   CONSolidate						///
						   DO_not_reload					///
						   label 							///	
						   STATAMETADATA					///
						   format(string)					///
						   ]	//	in(string) 

	if ("`label'" != "" & "`statametadata'" != "") {
		display as error "label and statametadata may not be combined"
		exit 198
	}
        
	//	if "`partition_by'" != "" {
	//		di as error "Hive partitioning not implemented yet"
	//		exit 198
	//	}
	if (!inlist("`compression'", "", "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd")) {
		display as error `"Acceptable options for compression are "lz4", "uncompressed", "snappy", "gzip", "lzo", "brotli", "zstd", and "" ("" will be zstd), passed "`compression'""'
		exit 198
	}
	
	if "`do_not_reload'" != "" & "`stream'" == "" {
		di as text "note: do_not_reload ignored without stream"
	}

	if `compression_level' != -1 {
		local check_compression_level = 0
		if inlist("`compression'", "", "zstd") {
			local check_compression_level = 1
			local compression_level_min = 1
			local compression_level_max = 22
		}
		else if "`compression'"== "brotli" {
			local check_compression_level = 1
			local compression_level_min = 0
			local compression_level_max = 11
		}
		else if "`compression'"== "gzip" {
			local check_compression_level = 1
			local compression_level_min = 0
			local compression_level_max = 9
		}
		
		if `check_compression_level' {
			if !inrange(`compression_level', `compression_level_min', `compression_level_max') {
				display as error `"Acceptable compression_level range for compression = "`compression'" (zstd if blank) [`compression_level_min', `compression_level_max'], passed "`compression_level'""'
				exit 198
				
			}
		}
	}
		
	//	Currently not available to have an in statement on write
	local in
	pq_register_plugin
	
	pq_convert_path `"`using'"'
	local using = r(fullpath)
	local directory_opt
	if ("`partition_by'" != "" | (`=_N' > `chunk' & "`consolidate'" == "")) {
		local directory_opt directory
	}
	pq_infer_format, path("`using'") format("`format'") `directory_opt'
	local source_format = r(format)
	local using `"`r(path)'"'
	if !inlist("`source_format'", "parquet", "spss", "csv") {
		display as error `"Unsupported save format(`format'): expected parquet, spss, or csv"'
		exit 198
	}
	if ("`statametadata'" != "" & "`source_format'" != "parquet") {
		display as error "statametadata is only supported for Parquet output"
		exit 198
	}
	if ("`statametadata'" != "" & "`consolidate'" != "" & ///
		"`partition_by'" != "") {
		display as error "statametadata with partition_by() and consolidate is not supported"
		exit 198
	}

	if "`replace'" == "" {
		//	Check if file exists as file or path
		quietly local is_file = fileexists("`using'")
		mata: st_local("is_directory",  strofreal(direxists("`using'")))

		if `is_file' | `is_directory' {
			di as error "File exists: `using'"
			di as error `" 	Add ", replace" if you want to overwrite the file"'
			error 602
		}
	}


	local StataColumnInfo from_macros
	local var_count = 0
	local n_rename = 0
	unab _all_variables_ordered : _all
	


	local vars_labeled	
	local original_order
	local stata_metadata_vars
	local stata_metadata_varlist
	local metadata_capsule
	if ("`statametadata'" != "") {
		//	Capsule membership is every saved variable, unconditionally.
		//
		//	Two reasons it is not conditioned on the data having labels.
		//	First, display formats and storage types are metadata too, and
		//	every variable has both, so "this dataset has nothing to carry"
		//	is never true once they are in scope -- an unlabelled byte/int
		//	dataset is exactly the case where the saved type matters most.
		//	Second, any trigger that consulted formats or types would not be
		//	storage-type independent: "default format" is defined relative to
		//	a storage type, and a relaxed schema can give one logical column
		//	different types in different fragments, so the trigger could fire
		//	for one fragment of a save and not another.  statametadata is an
		//	explicit opt-in, so the capsule is simply always written.
		local stata_metadata_vars `varlist'
	}
	if ("`label'" == "label") {
		quietly ds
		local original_order `r(varlist)'
		
		//	Do any variables have labels?
		foreach vari in `varlist' {
			local labeli : value label `vari'
			if "`labeli'" != "" {
				local vars_labeled `vars_labeled' `vari'
				tempvar `vari'

				//	Move the "true" value to a tempvar
				quietly rename `vari' ``vari''

				//	Create a decoded value in the original variable name
				decode ``vari'', gen(`vari')
				//	tab ``vari'' `vari'
			}
		}

		quietly order `original_order'
	}

	foreach vari in `varlist' {
		local var_count = `var_count' + 1
		local typei: type `vari'
		local formati: format `vari'
		local str_length 0
		
		
		if ((substr("`typei'",1,3) == "str") & (lower("`typei'") != "strl")) {
			local str_length = substr("`typei'",4,.)
			local typei String
		}
		else {
			local typei = strproper("`typei'")
		}
		
		local name_`var_count' `vari'
		local dtype_`var_count' `typei'
		local format_`var_count' `formati'
		local str_length_`var_count' `str_length'
		local col_`var_count' : list posof "`vari'" in _all_variables_ordered
		
		//	Rename?
		if ("`noautorename'" == "") {
			local parquet_name_char : char `vari'[_pq_parquet_name]
			if (`"`parquet_name_char'"' != "") {
				local n_rename = `n_rename' + 1
				local rename_from_`n_rename' `vari'
				local rename_to_`n_rename' `parquet_name_char'
				continue
			}
			//	capture: labels with backticks (e.g. `87) cause r(132) "too few quotes"
			//	when expanded inside compound quotes -- silently skip rename check

			local labeli: variable label `vari'
			local labeli: subinstr local labeli "\`" "'", all

			if regexm(`"`labeli'"', "^\{parquet_name:([^}]*)\}") {
				//	Extract the value between "parquet_name:" and "}"

				local n_rename = `n_rename' + 1
				local rename_from_`n_rename' `vari'
				local rename_to_`n_rename' = regexs(1)

				//	di "n_rename: `n_rename'"
				//	di "	from: `rename_from_`n_rename''"
				//	di "	to:   `rename_to_`n_rename''"
			}
		}
	}
	
	
	
	if ("`in'" != "") {
		local offset = substr("`in'", 1, strpos("`in'", "/") -1)
		local offset = max(`offset',0)
		local last_n = substr("`in'", strpos("`in'", "/") + 1, .)
		local n_rows = `last_n' - `offset' + 1
	}
	else {
		local offset = 0
		local last_n = 0
		local n_rows = 0
	}
	
	
	//	Process the if statement, if passed
	if (`"`if'"' != "") {
		local greater_than = strpos(`"`if'"', ">") > 0
		if (`greater_than') {
			di as error "pq will interpret > as in SQL, which is different than Stata."
			di as error "	It will not include . as > any value."
		}

		plugin call polars_parquet_plugin, if `"`if'"'
	}
	else {
		local sql_if
	}



	local offset = max(0,`offset' - 1)
	
	local overwrite_partition = "`nopartitionoverwrite'" == ""
	local b_compress = "`compress'" != ""
	local b_compress_string_to_numeric = "`compress_string_to_numeric'" != ""
	if ("`source_format'" != "parquet") {
		if ("`partition_by'" != "") {
			di as error "partition_by() is only supported for parquet output"
			exit 198
		}
		if ("`nopartitionoverwrite'" != "") {
			di as error "nopartitionoverwrite is only supported for parquet output"
			exit 198
		}
		if ("`compression'" != "" | `compression_level' != -1) {
			di as error "compression() and compression_level() are only supported for parquet output"
			exit 198
		}
		if ("`stream'" != "" | "`consolidate'" != "" | `chunk' != 2147483647) {
			di as error "stream/chunk/consolidate are only supported for parquet output"
			exit 198
		}
	}
	


	local n_rows = _N
	if ("`partition_by'" != "" & `n_rows' == 0) {
		display as error "partition_by() requires at least one observation"
		exit 198
	}
	if ("`statametadata'" != "" & "`stata_metadata_vars'" != "") {
		tempfile metadata_capsule
		pq_make_stata_metadata_capsule, capsule("`metadata_capsule'") ///
			variables("`stata_metadata_vars'")
		local stata_metadata_varlist `stata_metadata_vars'
	}

	if (`n_rows' > `chunk') {
		if ("`partition_by'" != "") & (`b_compress' | `b_compress_string_to_numeric') {
			di "Compression disabled for chunked writing as the schema could vary for each chunk"
			di "	which can cause errors for parquet reads"

			local b_compress = 0
			local b_compress_string_to_numeric = 0
		}


		* check and delete file
		local needs_dir = "`partition_by'" == ""
		if ("`partition_by'" == "" | `overwrite_partition') {
			plugin call polars_parquet_plugin, clean_path "`using'" `needs_dir'
		}
		
		local n_chunks = ceil(`n_rows'/`chunk')
		di "Writing file in `n_chunks' chunks of up to `=strtrim(string(`chunk',"%20.0gc"))' rows"

		if ("`stream'" != "") {
			di "	streaming, save temporary file"
			tempfile save_for_chunks
			quietly save "`save_for_chunks'"
			local _stream_saved = 1
		}
		else {
			tempname save_for_chunks
			capture frame drop `save_for_chunks'
			quietly frame
			local original_frame = r(currentframe)
		}

		local chunk_suffix
		
		local end_row = `offset'
		forvalues i = 1/`n_chunks' {
			if ("`partition_by'" == "")	local chunk_suffix /data_`i'.parquet

			local overwrite_chunk_output = `overwrite_partition' & (`i' == 1)
			local append_chunk_partition = !`overwrite_chunk_output'

			local start_row = `end_row' + 1
			local end_row = `start_row' + `chunk' - 1
			local end_row = min(`end_row', `n_rows')
			local rows_to_read = `end_row' - `start_row' + 1

			if ("`stream'" != "") {
				di "	chunk `i': loading rows `start_row'-`end_row'
				quietly use `varlist' in `start_row'/`end_row' using "`save_for_chunks'" 
			}
			else {
				di "	chunk `i': creating frame with rows `start_row'-`end_row'
				frame put `varlist' in `start_row'/`end_row', into(`save_for_chunks')

				frame change `save_for_chunks'
			}

			//	di "`using'`chunk_suffix'"
			plugin call polars_parquet_plugin, save "`using'`chunk_suffix'" "from_macro" `rows_to_read' 0 `"`sql_if'"' `"`StataColumnInfo'"' "`partition_by'" "`compression'" "`compression_level'" `overwrite_chunk_output' `b_compress' `b_compress_string_to_numeric' 1 `append_chunk_partition' "`source_format'" "`metadata_capsule'" "from_macro"


			if ("`stream'" == "") {
				frame change `original_frame'
				capture frame drop `save_for_chunks'
			}
			
		}

		if ("`partition_by'" == "") & ("`consolidate'" != "") {
			di "	consolidating chunked file into a single file"
			plugin call polars_parquet_plugin, consolidate "`using'"
		}
		if ("`stream'" != "") {
			if ("`do_not_reload'" == "") {
				di "	streaming finished, reload data"
				quietly use "`save_for_chunks'", clear
			}
			else {
				clear
				di "	streaming finished, do_not_reload set, so data not reloaded"
			}
			capture erase `save_for_chunks'.dta
		}

	}
	else {
		//	di `"plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' "`sql_if'" "`StataColumnInfo'" "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric' 0"'
		local append_partition = ("`partition_by'" != "" & !`overwrite_partition')
		capture noisily plugin call polars_parquet_plugin, save "`using'" "from_macro" `n_rows' `offset' `"`sql_if'"' `"`StataColumnInfo'"' "`partition_by'" "`compression'" "`compression_level'" `overwrite_partition' `b_compress' `b_compress_string_to_numeric' 0 `append_partition' "`source_format'" "`metadata_capsule'" "from_macro"
		local _save_rc = _rc
		if (`_save_rc') exit `_save_rc'
	}
	quietly version
	}
	local rc = _rc

	// Centralized cleanup for all ordinary, chunked, and streamed exits.
	if ("`original_frame'" != "") {
		capture frame change `original_frame'
		capture frame drop `save_for_chunks'
	}
	if ("`stream'" != "" & "`save_for_chunks'" != "") {
		if (`rc' & `_stream_saved') capture noisily use "`save_for_chunks'", clear
		capture erase "`save_for_chunks'"
		capture erase "`save_for_chunks'.dta"
	}
	if ("`metadata_capsule'" != "") capture erase "`metadata_capsule'"

	// Reset variables temporarily decoded by label after returning to the source frame.
	local _restore_decoded = "`vars_labeled'" != ""
	if (!`rc' & "`stream'" != "" & "`do_not_reload'" != "") local _restore_decoded = 0
	if (`_restore_decoded') {
		capture noisily {
			foreach vari in `vars_labeled' {
				quietly drop `vari'
				quietly rename ``vari'' `vari'
			}
			quietly order `original_order'
		}
		local _label_cleanup_rc = _rc
		if (!`rc' & `_label_cleanup_rc') local rc = `_label_cleanup_rc'
	}
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
end


capture program drop pq_make_stata_metadata_capsule
program define pq_make_stata_metadata_capsule, nclass
	version 16.0
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	tempname metadata_frame
	local _frame_created = 0
	capture noisily {
		syntax, CAPSule(string) VARiables(varlist)
		local source_n = _N
		if (`source_n' == 0) {
			frame put `variables', into(`metadata_frame')
		}
		else {
			frame put `variables' in 1, into(`metadata_frame')
		}
		local _frame_created = 1
		if (`source_n' > 0) frame `metadata_frame': quietly drop in 1
		frame `metadata_frame': quietly save "`capsule'", replace
	}
	local rc = _rc
	if (`_frame_created') capture frame drop `metadata_frame'
	if (`rc') capture erase "`capsule'"
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
end


capture program drop pq_restore_stata_metadata
program define pq_restore_stata_metadata, nclass
	version 16.0
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	tempname metadata_frame
	local _frame_created = 0
	capture noisily {
		syntax, CAPSule(string) ALLCapsulevars(string) CAPSulevars(string) ///
			TARGETvars(string) [NORECAST RECASTSKIP(string)]
		confirm file "`capsule'"
		local all_capsule_count : word count `allcapsulevars'
		local capsule_count : word count `capsulevars'
		local target_count : word count `targetvars'
		if (`all_capsule_count' == 0 | `capsule_count' == 0 | ///
			`capsule_count' != `target_count') {
			display as error "Invalid Stata metadata column map"
			exit 198
		}
		foreach vari in `targetvars' {
			confirm variable `vari', exact
		}

		frame create `metadata_frame'
		local _frame_created = 1
		frame `metadata_frame': quietly use "`capsule'", clear
		frame `metadata_frame': quietly count
		if (r(N) != 0) {
			display as error "Embedded Stata metadata capsule must have zero observations"
			exit 198
		}
		frame `metadata_frame': unab capsule_file_vars : _all
		local capsule_file_vars_sorted : list sort capsule_file_vars
		local mapped_capsule_vars_sorted : list sort allcapsulevars
		if (`"`capsule_file_vars_sorted'"' != `"`mapped_capsule_vars_sorted'"') {
			display as error "Embedded Stata metadata capsule variables do not match its column map"
			display as error "  capsule variables: `capsule_file_vars_sorted'"
			display as error "  mapped variables: `mapped_capsule_vars_sorted'"
			exit 198
		}
		foreach vari in `capsulevars' {
			frame `metadata_frame': confirm variable `vari', exact
		}

		frame `metadata_frame': mata: _pq_capture_stata_metadata("capsulevars")
		frame `metadata_frame': local pq_data_label : data label
		local pq_allow_recast = ("`norecast'" == "")
		mata: _pq_apply_stata_metadata("targetvars", `pq_allow_recast', "recastskip")
		//	The dataset label is the one attribute Mata cannot read or write,
		//	so it is restored here.  Compound quotes handle an embedded double
		//	quote, and macval() is required on top of them: without it Stata
		//	rescans the substituted text, so a label containing $name is
		//	replaced by that global's value and a `...' sequence is dropped.
		label data `"`macval(pq_data_label)'"'
	}
	local rc = _rc
	if (`_frame_created') capture frame drop `metadata_frame'
	capture mata: _pq_clear_stata_metadata()
	local _metadata_cleanup_rc = _rc
	if (!`rc' & `_metadata_cleanup_rc') local rc = `_metadata_cleanup_rc'
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
end


capture program drop pq_restore_stata_metadata_append
program define pq_restore_stata_metadata_append, nclass
	version 16.0
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	tempname metadata_frame
	local _frame_created = 0
	capture noisily {
		syntax, CAPSule(string) ALLCapsulevars(string) CAPSulevars(string) ///
			TARGETvars(string) EXISTINGvars(string) [NORECAST RECASTSKIP(string)]
		confirm file "`capsule'"
		local all_capsule_count : word count `allcapsulevars'
		local capsule_count : word count `capsulevars'
		local target_count : word count `targetvars'
		if (`all_capsule_count' == 0 | `capsule_count' == 0 | ///
			`capsule_count' != `target_count') {
			display as error "Invalid Stata metadata column map"
			exit 198
		}
		foreach vari in `targetvars' {
			confirm variable `vari', exact
		}

		frame create `metadata_frame'
		local _frame_created = 1
		frame `metadata_frame': quietly use "`capsule'", clear
		frame `metadata_frame': quietly count
		if (r(N) != 0) {
			display as error "Embedded Stata metadata capsule must have zero observations"
			exit 198
		}
		frame `metadata_frame': unab capsule_file_vars : _all
		local capsule_file_vars_sorted : list sort capsule_file_vars
		local mapped_capsule_vars_sorted : list sort allcapsulevars
		if (`"`capsule_file_vars_sorted'"' != `"`mapped_capsule_vars_sorted'"') {
			display as error "Embedded Stata metadata capsule variables do not match its column map"
			display as error "  capsule variables: `capsule_file_vars_sorted'"
			display as error "  mapped variables: `mapped_capsule_vars_sorted'"
			exit 198
		}
		foreach vari in `capsulevars' {
			frame `metadata_frame': confirm variable `vari', exact
		}

		frame `metadata_frame': mata: _pq_capture_stata_metadata("capsulevars")
		local pq_allow_recast = ("`norecast'" == "")
		mata: _pq_apply_stata_metadata_newvars("targetvars", "existingvars", `pq_allow_recast', "recastskip")
	}
	local rc = _rc
	if (`_frame_created') capture frame drop `metadata_frame'
	capture mata: _pq_clear_stata_metadata()
	local _metadata_cleanup_rc = _rc
	if (!`rc' & `_metadata_cleanup_rc') local rc = `_metadata_cleanup_rc'
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
end


capture program drop pq_write_overflow_dta
program pq_write_overflow_dta
	syntax, using(string) output(string) offset(integer) n_rows(integer) ///
	        columns(string) [if_clause(string) relax asterisk_to_variable(string) ///
	        random_share(real 0) random_seed(integer 0) format(string) ///
	        infer_schema_length(integer 10000) parse_dates(integer 0)]

	if (`infer_schema_length' < 0) {
		display as error `"infer_schema_length() must be >= 0, passed `infer_schema_length'"'
		exit 198
	}

	pq_infer_format, path("`using'") format("`format'")
	local source_format = r(format)
	if !inlist("`source_format'", "parquet", "sas", "spss", "csv") {
		display as error `"Unsupported format(`format'): expected parquet, sas, spss, or csv"'
		exit 198
	}
	local parse_dates_for_plugin = `parse_dates'
	if ("`source_format'" != "csv") {
		local parse_dates_for_plugin = 0
	}

	// Set up relax flag
	if ("`relax'" != "") {
		local b_relax 1
	}
	else {
		local b_relax 0
	}

	// Call plugin to write overflow rows to .dta
	// This writes ALL columns (both strL and non-strL) for the overflow slice
	// Args: parquet_path, dta_output, columns, n_rows, offset, sql_if, relax, asterisk_to_variable, random_share, random_seed
	plugin call polars_parquet_plugin, write_overflow_dta "`using'" "`output'" "`columns'" `n_rows' `offset' `"`if_clause'"' `b_relax' "`asterisk_to_variable'" `random_share' `random_seed' "`source_format'" `infer_schema_length' `parse_dates_for_plugin'
end


capture program drop pq_normalize_csv_opts
program pq_normalize_csv_opts, rclass
	//	Normalize infer_schema_length and parse_dates for non-CSV formats.
	//	CSV-only options are silently reset to defaults for other formats.
	syntax, source_format(string) infer_schema_length(integer) b_parse_dates(integer)
	local infer_schema_length_for_plugin = `infer_schema_length'
	if ("`source_format'" != "csv") {
		if (`infer_schema_length' != 10000) {
			di as text "note: infer_schema_length() ignored for format(`source_format'); only used for csv reads."
		}
		local infer_schema_length_for_plugin = 10000
	}
	local parse_dates_for_plugin = `b_parse_dates'
	if ("`source_format'" != "csv") {
		if (`b_parse_dates') {
			di as text "note: parse_dates ignored for format(`source_format'); only used for csv reads."
		}
		local parse_dates_for_plugin = 0
	}
	return local infer_schema_length_for_plugin = `infer_schema_length_for_plugin'
	return local parse_dates_for_plugin = `parse_dates_for_plugin'
end


capture program drop pq_register_plugin
program pq_register_plugin

	//	di "PLUGIN CHECK"
	capture plugin call polars_parquet_plugin, setup_check ""
	
	if (_rc > 0) {
		// Plugin is not loaded, so initialize it
		capture program polars_parquet_plugin, plugin using("pq.plugin")


		capture plugin call polars_parquet_plugin, setup_check ""
		if (_rc > 0) {
            // OS specific check here
            local os = "`c(os)'"
            
            if ("`os'" == "Windows")		local plugin_file = "pq.dll"
            else if ("`os'" == "MacOSX")	local plugin_file = "pq.dylib"
            else if ("`os'" == "Unix")		local plugin_file = "pq.so"
            else {
                display as error "Unsupported operating system: `os'"
                exit 198
            }
            
            // Try loading the OS-specific plugin
            capture program polars_parquet_plugin, plugin using("`plugin_file'")
            
            if (_rc > 0) {
                display as error "Failed to load plugin `plugin_file' for `os'"
                display as error "Make sure the plugin file exists in the current directory or ado path"
                exit _rc
            }
		}
	}
end




capture program drop pq_infer_format
program define pq_infer_format, rclass
	version 16
	local _orig_varabbrev = c(varabbrev)
	set varabbrev off
	local fmt
	local resolved_path
	capture noisily {
		syntax, path(string) [format(string) directory preserveexistingdir]
		local resolved_path `"`path'"'
		local fmt = lower("`format'")
		if ("`fmt'" == "") {
			local p = lower(`"`resolved_path'"')
			if regexm("`p'", "\.sas7bdat$")       local fmt sas
			else if regexm("`p'", "\.(sav|zsav)$") local fmt spss
			else if regexm("`p'", "\.csv$")        local fmt csv
			else                                    local fmt parquet
		}

		if ("`fmt'" == "parquet" & "`directory'" == "") {
			local pq_path_is_directory = 0
			if ("`preserveexistingdir'" != "") {
				mata: st_local("pq_path_is_directory", strofreal(direxists(st_local("resolved_path"))))
			}
			if (!`pq_path_is_directory' & ///
				!regexm(`"`resolved_path'"', "\.[^/\\]+$")) {
				local resolved_path `"`resolved_path'.parquet"'
			}
		}
	}
	local rc = _rc
	set varabbrev `_orig_varabbrev'
	if `rc' exit `rc'
	return local format "`fmt'"
	return local path `"`resolved_path'"'
end


capture program drop pq_convert_path
program define pq_convert_path, rclass
	version 16
    syntax anything
    
	local filepath `anything'
    
    // Handle the case where filepath might be in quotes
    if `"`filepath'"' == "" {
        local filepath `"`0'"'
    }
    
    // Get current working directory
    local cwd = c(pwd)
    
    // Check operating system
    local os = c(os)
    local is_windows = ("`os'" == "Windows")
    
    // Clean up the input path
    local filepath = trim("`filepath'")
    
    // Debug: show what we're working with
    //	di "Input filepath: [`filepath']"
    //	di "Current directory: [`cwd']"
    //	di "OS: [`os']"
    
    // Check if path is already absolute
    local is_absolute = 0
    
    if `is_windows' {
        // Windows: Check for drive letter (C:) or UNC path (\\)
        if regexm("`filepath'", "^[A-Za-z]:") | regexm("`filepath'", "^\\\\") {
            local is_absolute = 1
        }
    }
    else {
        // Unix: Check if starts with /
        if regexm("`filepath'", "^/") {
            local is_absolute = 1
        }
    }
    
    //	di "Is absolute: `is_absolute'"
    
    // If already absolute, return as-is
    if `is_absolute' {
        local fullpath "`filepath'"
    }
    else {
        // Handle relative paths
        if substr("`filepath'", 1, 2) == "./" {
            // Remove leading "./"
            local filepath = substr("`filepath'", 3, .)
            //	di "After removing ./: [`filepath']"
        }
        else if substr("`filepath'", 1, 2) == ".\" {
            // Remove leading ".\"
            local filepath = substr("`filepath'", 3, .)
            //	di "After removing .\: [`filepath']"
        }
        else if substr("`filepath'", 1, 3) == "../" {
            // Handle parent directory with forward slash
            local filepath = substr("`filepath'", 4, .)
            // Get parent directory manually
            if `is_windows' {
                local lastslash = strpos(reverse("`cwd'"), "\")
                if `lastslash' > 0 {
                    local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                }
            }
            else {
                local lastslash = strpos(reverse("`cwd'"), "/")
                if `lastslash' > 0 {
                    local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                }
            }
            //	di "After removing ../: [`filepath'], parent dir: [`cwd']"
        }
        else if substr("`filepath'", 1, 3) == "..\" {
            // Handle parent directory with backslash
            local filepath = substr("`filepath'", 4, .)
            // Get parent directory manually
            local lastslash = strpos(reverse("`cwd'"), "\")
            if `lastslash' > 0 {
                local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
            }
            //	di "After removing ..\: [`filepath'], parent dir: [`cwd']"
        }
        
        // Handle multiple parent directory references
        while substr("`filepath'", 1, 3) == "../" | substr("`filepath'", 1, 3) == "..\" {
            if substr("`filepath'", 1, 3) == "../" {
                local filepath = substr("`filepath'", 4, .)
                // Get parent directory manually
                if `is_windows' {
                    local lastslash = strpos(reverse("`cwd'"), "\")
                    if `lastslash' > 0 {
                        local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                    }
                }
                else {
                    local lastslash = strpos(reverse("`cwd'"), "/")
                    if `lastslash' > 0 {
                        local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                    }
                }
                //	di "Additional ../: [`filepath'], new parent: [`cwd']"
            }
            else if substr("`filepath'", 1, 3) == "..\" {
                local filepath = substr("`filepath'", 4, .)
                // Get parent directory manually
                local lastslash = strpos(reverse("`cwd'"), "\")
                if `lastslash' > 0 {
                    local cwd = substr("`cwd'", 1, length("`cwd'") - `lastslash')
                }
                //	di "Additional ..\: [`filepath'], new parent: [`cwd']"
            }
        }
        
        // Combine current directory with relative path
        if `is_windows' {
            local fullpath "`cwd'\\`filepath'"
        }
        else {
            local fullpath "`cwd'/`filepath'"
        }
        
        //	di "After combination: [`fullpath']"
    }
    
    // Clean up any double separators
    if `is_windows' {
        while regexm("`fullpath'", "\\\\\\\\") {
            local fullpath = regexr("`fullpath'", "\\\\\\\\", "\\\\")
        }
    }
    else {
        while regexm("`fullpath'", "//") {
            local fullpath = regexr("`fullpath'", "//", "/")
        }
    }
    
    // Return the full path
    return local fullpath "`fullpath'"
end
