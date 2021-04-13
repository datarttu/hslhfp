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
  stop('Usage: ./dt_filter.R <input_dir> [<output_dir>]')
}
INPUT_DIR <- args[1]
stopifnot(dir.exists(INPUT_DIR))
OUTPUT_DIR <- ifelse(length(args) > 1, args[2], INPUT_DIR)
stopifnot(dir.exists(OUTPUT_DIR))

tmessage <- function(...) {
  message('[', Sys.time(), ']; filter; ', ...)
}

filter_file <- function(infile, outfile, file_seq = NA, total_files = NA) {
  progmeter <- ifelse(
    !is.na(file_seq) & !is.na(total_files),
    sprintf('%04d/%d; ', file_seq, total_files),
    ''
  )
  dt <- fread(file = infile)
  nrow_orig <- nrow(dt)
  if (nrow_orig == 0) {
    tmessage(
      sprintf(
        '%sno rows in %s, skipping',
        progmeter, infile
      )
    )
    return()
  }

  #' FIXME: a temporary solution to shortening dataset labeling in logs...
  dataset_name <- unique(dt[, .(route, dir, oday)])[1, ]
  dataset_name$oday <- format(dataset_name$oday, '%Y-%m-%d')
  dataset_name <- paste0(dataset_name, collapse = '_')

  #' First, get rid of non-service observations
  #' (this is not a validation error case)
  dt <- dt[is_ongoing == TRUE, ]
  dt <- dt[, is_ongoing := NULL]
  nrow_ongoing <- nrow(dt)

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
    err_info <- sprintf('%d / %d (%.2f %%) rows with errors dropped. (%s)',
                        n_err_rows, nrow_ongoing, n_err_rows / nrow_ongoing, err_combos)
    dt <- dt[nchar(errors) == 0, ]
  } else {
    err_info <- 'No rows with errors dropped'
  }
  dt[, errors := NULL]

  fwrite(x = dt, file = outfile)
  tmessage(
    sprintf(
      '%s%s; %d / %d (%.2f %%) is_ongoing == FALSE rows dropped. %s. %d rows saved.',
      progmeter, dataset_name,
      nrow_orig - nrow_ongoing, nrow_orig, (nrow_orig - nrow_ongoing) / nrow_orig,
      err_info, nrow(dt)
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

tmessage('Starting row filtering for ',
         nrow(file_paths),
         ' files')

foo <- purrr::pwalk(.l = file_paths,
                    .f = filter_file)

tmessage('All files filtered')
