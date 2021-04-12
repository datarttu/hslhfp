#!/usr/bin/env Rscript
#'
#' Filter rows by attributes, in this case:
#'
#'  - is_ongoing must be TRUE
#'
#' And validation conditions:
#'
#'  - tst, start, oper and veh must not be NA
#'  - lat and long coordinates must be within a reasonable HSL bounging box
#'    and not NA
#'
#' Unlike dt_deduplicate.R, here we process all the .csv.gz files
#' in a directory in the same R process.
#'
#' Usage: either
#' > ./dt_filter.R <input_dir> <output_dir>
#' OR
#' > ./dt_filter.R <input_output_dir>
#' In the latter, the input files are overwritten as output files.

suppressMessages(library(data.table))
suppressMessages(library(purrr))
suppressMessages(library(stringr))

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
  message('[', Sys.time(), ']; filter is_ongoing; ', ...)
}

filter_file <- function(infile, outfile) {
  dt <- fread(file = infile)
  nrow_orig <- nrow(dt)
  tmessage(infile, ' read')
  tmessage(nrow_orig, ' original rows')

  #' First, get rid of non-service observations
  #' (this is not a validation error case)
  dt <- dt[is_ongoing == TRUE, ]
  dt <- dt[, is_ongoing := NULL]
  tmessage(nrow(dt), ' rows with is_ongoing == TRUE kept, is_ongoing column dropped')

  #' Validation filters
  dt[, errors := '']

  dt[is.na(tst), errors := paste0(errors, ', ', 'NA tst')]
  dt[is.na(start), errors := paste0(errors, ', ', 'NA start')]
  dt[is.na(oper), errors := paste0(errors, ', ', 'NA oper')]
  dt[is.na(veh), errors := paste0(errors, ', ', 'NA veh')]
  dt[is.na(lat), errors := paste0(errors, ', ', 'NA lat')]
  dt[is.na(long), errors := paste0(errors, ', ', 'NA long')]
  dt[lat < 59.8 | lat > 60.7, errors := paste0(errors, ', ', 'OOB lat')]
  dt[long < 23.0 | long > 26.0, errors := paste0(errors, ', ', 'OOB long')]
  dt[, errors := str_remove(errors, '^,')]

  n_err_rows <- nrow(dt[nchar(errors) > 0, ])
  if (n_err_rows > 0) {
    err_combos <- dt[nchar(errors) > 0, .N, by = errors]
    err_combos <- paste(
      paste0(err_combos$errors, ': ', err_combos$N),
      collapse = ', '
    )
    tmessage(n_err_rows, ' rows with errors to drop. ', err_combos)
    dt <- dt[nchar(errors) == 0, ]
  }
  dt[, errors := NULL]

  fwrite(x = dt, file = outfile)
  tmessage(nrow(dt), ' rows written to ', outfile)
}

file_paths <- data.frame(
  infile_path = list.files(path = INPUT_DIR, pattern = '*.csv.gz',
                           full.names = TRUE),
  stringsAsFactors = FALSE
)
file_paths$outfile_path <- gsub(
  pattern = INPUT_DIR,
  replacement = OUTPUT_DIR,
  x = file_paths$infile_path
)

if (nrow(file_paths) == 0) {
  stop('No files to filter, exiting',
       call. = FALSE)
}

tmessage('Starting row filtering for ',
         nrow(file_paths),
         ' files')

foo <- purrr::walk2(.x = file_paths$infile_path,
                    .y = file_paths$outfile_path,
                    .f = filter_file)

tmessage('All files filtered')
