#!/usr/bin/env Rscript
# Round-trip Stata metadata through Parquet from R, using the `arrow` package.
#
# Reads/writes the JSON envelope stored under the Parquet footer key
# `org.stata.pq.labels.v1` -- the same envelope `pq save`/`pq use` use and the
# Python example (python_stata_metadata.py) reads/writes. A file written here is
# therefore restored in full by `pq use` in Stata, and everything `pq save
# ..., statametadata` writes can be read back here.
#
# Requires: arrow, jsonlite      install.packages(c("arrow", "jsonlite"))
#
# Usage:
#   Rscript examples/r_stata_metadata.R write  <path.parquet>
#   Rscript examples/r_stata_metadata.R verify <a.parquet> <b.parquet>
#
# See examples/README.md for the JSON schema.

suppressMessages({
  library(arrow)
  library(jsonlite)
})

STATA_META_KEY <- "org.stata.pq.labels.v1"
STATA_META_VERSION <- 1L

# ---- write / read helpers -------------------------------------------------

# Assemble an envelope, dropping empty fields (as Stata does). Note/dataset-note
# vectors are wrapped in I() so a single note still serializes as a JSON array.
build_envelope <- function(variables = NULL, value_labels = NULL,
                           dataset_label = NULL, dataset_notes = NULL) {
  env <- list(version = STATA_META_VERSION)
  if (!is.null(dataset_label)) env$dataset_label <- dataset_label
  if (!is.null(dataset_notes)) env$dataset_notes <- I(as.character(dataset_notes))
  if (!is.null(variables)) {
    env$variables <- lapply(variables, function(v) {
      v <- v[!vapply(v, is.null, logical(1))]
      if (!is.null(v$notes)) v$notes <- I(as.character(v$notes))
      v
    })
  }
  if (!is.null(value_labels)) env$value_labels <- value_labels
  env
}

# Write `df` to `path`, embedding `envelope` in the Parquet FOOTER metadata.
write_stata_parquet <- function(path, df, envelope) {
  tab <- arrow::as_arrow_table(df)
  md <- tab$metadata
  if (is.null(md)) md <- list()
  md[[STATA_META_KEY]] <- as.character(
    jsonlite::toJSON(envelope, auto_unbox = TRUE)
  )
  tab$metadata <- md
  arrow::write_parquet(tab, path)
  invisible(path)
}

# Parse the envelope from a file's footer metadata, or NULL if absent.
read_stata_metadata <- function(path) {
  tab <- arrow::read_parquet(path, as_data_frame = FALSE)
  raw <- tab$metadata[[STATA_META_KEY]]
  if (is.null(raw)) return(NULL)
  jsonlite::fromJSON(raw, simplifyVector = FALSE)
}

# Canonical form for comparison: recursively sort named lists, then serialize.
canon <- function(x) {
  rec <- function(o) {
    if (is.list(o)) {
      if (!is.null(names(o))) o <- o[order(names(o))]
      lapply(o, rec)
    } else {
      o
    }
  }
  as.character(jsonlite::toJSON(rec(x), auto_unbox = TRUE))
}

# ---- demo + verify --------------------------------------------------------

demo_table_and_envelope <- function() {
  df <- data.frame(
    id    = c(1L, 2L, 3L, 4L),
    grp   = c(0L, 1L, 1L, 0L),
    score = c(1.5, 2.0, 3.25, 4.0),
    name  = c("alpha", "bravo", "charlie", "delta"),
    stringsAsFactors = FALSE
  )
  variables <- list(
    id    = list(label = "Row identifier", format = "%9.0g", stata_type = "long"),
    grp   = list(label = "Treatment group", value_label = "grplbl",
                 format = "%9.0g", stata_type = "long",
                 notes = "0 = control, 1 = treatment"),
    score = list(label = "Test score", format = "%9.2f", stata_type = "double"),
    name  = list(label = "Participant name")
  )
  value_labels <- list(grplbl = list("0" = "Control", "1" = "Treatment"))
  env <- build_envelope(
    variables     = variables,
    value_labels  = value_labels,
    dataset_label = "R-authored demo dataset",
    dataset_notes = "Written by examples/r_stata_metadata.R"
  )
  list(df = df, env = env)
}

verify <- function(a, b) {
  ok <- TRUE

  da <- as.data.frame(arrow::read_parquet(a))
  db <- as.data.frame(arrow::read_parquet(b))
  if (isTRUE(all.equal(da, db))) {
    cat(sprintf("DATA     : identical (%d rows x %d cols)\n", nrow(da), ncol(da)))
  } else {
    ok <- FALSE
    cat("DATA     : DIFFER\n")
  }

  ma <- read_stata_metadata(a)
  mb <- read_stata_metadata(b)
  # format / stata_type are hints Stata may normalize (e.g. add %9s/string for
  # a string column the writer left bare), so compare the core fields strictly
  # and report the hints separately, as python_verify.py does.
  split_hints <- function(m) {
    hints <- list()
    for (nm in names(m$variables)) {
      v <- m$variables[[nm]]
      h <- v[intersect(names(v), c("format", "stata_type"))]
      if (length(h)) hints[[nm]] <- h
      m$variables[[nm]] <- v[setdiff(names(v), c("format", "stata_type"))]
    }
    list(core = m, hints = hints)
  }
  sa <- split_hints(ma)
  sb <- split_hints(mb)
  if (identical(canon(sa$core), canon(sb$core))) {
    cat("METADATA : core fields identical (version, labels, value labels, notes)\n")
  } else {
    ok <- FALSE
    cat("METADATA : core fields DIFFER\n")
    cat("  file A:", canon(sa$core), "\n")
    cat("  file B:", canon(sb$core), "\n")
  }
  if (identical(canon(sa$hints), canon(sb$hints))) {
    cat("HINTS    : format / stata_type identical\n")
  } else {
    cat("HINTS    : DIFFER (Stata may have normalized these)\n")
    cat("  file A:", canon(sa$hints), "\n")
    cat("  file B:", canon(sb$hints), "\n")
  }

  cat("\nRESULT   :", if (ok) "round trip is lossless\n" else "round trip LOST something\n")
  if (!ok) quit(status = 1)
}

# ---- CLI ------------------------------------------------------------------

args <- commandArgs(trailingOnly = TRUE)
if (length(args) >= 1 && args[1] == "write") {
  path <- if (length(args) >= 2) args[2] else "r_made.parquet"
  de <- demo_table_and_envelope()
  write_stata_parquet(path, de$df, de$env)
  back <- read_stata_metadata(path)
  stopifnot(!is.null(back), back$dataset_label == "R-authored demo dataset")
  cat(sprintf("Wrote %s with Stata metadata under %s\n", path, STATA_META_KEY))
  cat(as.character(jsonlite::toJSON(back, auto_unbox = TRUE, pretty = TRUE)), "\n")
} else if (length(args) >= 3 && args[1] == "verify") {
  verify(args[2], args[3])
} else {
  cat("Usage:\n",
      "  Rscript examples/r_stata_metadata.R write  <path.parquet>\n",
      "  Rscript examples/r_stata_metadata.R verify <a.parquet> <b.parquet>\n",
      sep = "")
  quit(status = 2)
}
