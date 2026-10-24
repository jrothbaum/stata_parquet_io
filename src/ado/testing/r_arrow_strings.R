# Reads the files written by r_arrow_strings.do with R's arrow package.
# Fails if any string column is declared as utf8_view (apache/arrow#45523).
# Run from src/ado/testing after r_arrow_strings.do:
#   Rscript r_arrow_strings.R

library(arrow)

outdir <- "r_arrow_strings_out"

check_schema <- function(schema, label) {
  types <- vapply(schema$fields, function(f) f$type$ToString(), character(1))
  cat(label, ":", paste(names(schema), types, sep = "=", collapse = ", "), "\n")
  stopifnot(!any(grepl("view", types)))
}

for (f in c("plain.parquet", "consolidated.parquet")) {
  path <- file.path(outdir, f)
  check_schema(read_parquet(path, as_data_frame = FALSE)$schema, f)
  df <- read_parquet(path)
  stopifnot(nrow(df) == 100)
  stopifnot(df$name[df$id == 1] == "name_1")
  stopifnot(startsWith(df$note[df$id == 7], "note 7 "))
}

# pq also stores the partition column in each file (as int8 here), so
# declare its type rather than letting arrow infer int32 from the path.
ds <- open_dataset(file.path(outdir, "partitioned"),
                   partitioning = hive_partition(grp = int8()))
check_schema(ds$schema, "partitioned")
df <- as.data.frame(ds$NewScan()$Finish()$ToTable())
stopifnot(nrow(df) == 100)
stopifnot(df$name[df$id == 1] == "name_1")

cat("PASS: R arrow read all pq outputs\n")
