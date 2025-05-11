use std::thread;
use std::env;

pub const DAY_SHIFT_SAS_STATA: i32 = 3653;
pub const SEC_SHIFT_SAS_STATA: i64 = 315619200;

pub const SEC_MILLISECOND: i64 = 1_000;
pub const SEC_MICROSECOND: i64 = 1_000_000;
pub const SEC_NANOSECOND: i64 = 1_000_000_000;



pub fn get_thread_count() -> usize {
    // First try to get the thread count from POLARS_MAX_THREADS env var
    match env::var("POLARS_MAX_THREADS") {
        Ok(threads_str) => {
            // Try to parse the environment variable as a usize
            match threads_str.parse::<usize>() {
                Ok(threads) => threads,
                Err(_) => {
                    // If parsing fails, fall back to system thread count
                    thread::available_parallelism()
                        .map(|p| p.get())
                        .unwrap_or(1)
                }
            }
        },
        Err(_) => {
            // If environment variable is not set, use system thread count
            thread::available_parallelism()
                .map(|p| p.get())
                .unwrap_or(1)
        }
    }
}


#[derive(Copy,Clone,Debug)]
pub enum ParallelizationStrategy {
    ByRow,
    ByColumn,
}

// Simple decision function
pub fn determine_parallelization_strategy(
    n_columns: usize,
    n_rows: usize,
    available_cores: usize
) -> ParallelizationStrategy {
    // Column parallelism when:
    // 1. We have significantly more columns than CPU cores
    // 2. We have relatively few rows compared to columns
    if n_columns > available_cores * 2 && n_rows < 100_000 {
        ParallelizationStrategy::ByColumn
    } else {
        // Default to row parallelism in most other cases
        ParallelizationStrategy::ByRow
    }
}
