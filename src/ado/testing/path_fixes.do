local os = c(os)
local is_windows = ("`os'" == "Windows")


if (`is_windows') {
	cd "C:\Users\jonro\Downloads"
	pq path "test_save_if.parquet"
	return list

	pq path  "C:\\absolute\\path\\file.dta" 
	return list

	pq path  "C:\absolute\path\file.dta" 
	return list

	pq path ".\test_save_if.parquet"
	return list
	
	
	pq path ".\\test_save_if.parquet"
	return list

	pq path "..\Downloads\test_save_if.parquet"
	return list
	
	pq use "test_save_if.parquet", clear
	sum
	
	pq use "..\Downloads\test_save_if.parquet", clear
	sum
	
	pq use "C:\Users\jonro\Downloads\test_save_if.parquet", clear
	sum
}
else {
	
}