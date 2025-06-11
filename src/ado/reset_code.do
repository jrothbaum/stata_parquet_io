discard
local source_path_ado C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\src\ado\p
local source_path_dll C:\Users\jonro\OneDrive\Documents\Coding\stata_parquet_io\target\release
local destination_path_ado C:\Users\jonro\ado\plus\p
local ado_files pq.pkg pq.sthlp pq.ado
foreach fi in `ado_files' {
	copy "`source_path_ado'/`fi'" "`destination_path_ado'/`fi'", replace 
}

local fi stata_parquet_io.dll
copy "`source_path_dll'/`fi'" "`destination_path_ado'/pq.plugin", replace
discard
clear