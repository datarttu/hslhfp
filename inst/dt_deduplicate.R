#!/usr/bin/env Rscript
#'
#' Sort and deduplicate route_dir_oday.csv.gz files.
#' Deduplication is done based on ENTIRE duplicate rows.
#'
#' Usage: either
#' > ./dt_deduplicate.R <input_dir> <output_dir>
#' OR
#' > ./dt_deduplicate.R <input_output_dir>
#' In the latter, the input files are overwritten as output files.

suppressMessages(library(data.table))
suppressMessages(library(purrr))

#' This is needed for .csv.gz reading with fread to work
stopifnot('R.utils' %in% installed.packages())

args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop('Usage: ./dt_deduplicate.R <input_dir> [<output_dir>]')
}
INPUT_DIR <- args[1]
stopifnot(dir.exists(INPUT_DIR))
OUTPUT_DIR <- ifelse(length(args) > 1, args[2], INPUT_DIR)
stopifnot(dir.exists(OUTPUT_DIR))

tmessage <- function(...) {
  message('[', Sys.time(), ']; deduplicate; ', ...)
}

deduplicate_file <- function(infile, outfile, file_seq = NA, total_files = NA) {
  progmeter <- ifelse(
    !is.na(file_seq) & !is.na(total_files),
    sprintf('%04d/%d', file_seq, total_files),
    ''
  )
  dt <- fread(file = infile)
  nrow_orig <- nrow(dt)
  if (nrow_orig == 0) {
    tmessage(
      sprintf(
        '%d/%d; no rows in %s, skipping',
        file_seq, total_files, infile
      )
    )
    return()
  }

  dt_dedup <- unique(
    dt[order(route, dir, oday, start, oper, veh, tst, event_type), ]
  )
  nrow_dedup <- nrow(dt_dedup)
  n_duplicated <- nrow_orig - nrow_dedup

  fwrite(x = dt_dedup, file = outfile)

  tmessage(
    sprintf(
      '%s; %d duplicates removed from %s and %d rows saved to %s',
      progmeter, n_duplicated, infile, nrow_dedup, outfile
    )
  )
}

file_paths <- data.frame(
  infile = list.files(path = INPUT_DIR, pattern = '*.csv.gz',
                      full.names = TRUE),
  stringsAsFactors = FALSE
)
file_paths$outfile <- gsub(
  pattern = INPUT_DIR,
  replacement = OUTPUT_DIR,
  x = file_paths$infile
)
file_paths$file_seq <- seq_along(file_paths$infile)
file_paths$total_files <- nrow(file_paths)

tmessage('Starting deduplication for ',
         nrow(file_paths),
         ' files')

foo <- purrr::pwalk(.l = file_paths,
                    .f = deduplicate_file)

tmessage('All files filtered')
