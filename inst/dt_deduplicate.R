#!/usr/bin/env Rscript
#'
#' Sort and deduplicate contents of a route_dir_oday csv file.
#' Deduplication is done based on ENTIRE duplicate rows.
#'
#' Usage: either
#' > ./dt_deduplicate.R <input_file.csv.gz> <output_file.csv.gz>
#' OR
#' > ./dt_deduplicate.R <input_output_file.csv.gz>
#' In the latter, the input file is overwritten as output file.

suppressMessages(library(data.table))

#' This is needed for .csv.gz reading with fread to work
stopifnot('R.utils' %in% installed.packages())

args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop('Usage: ./dt_deduplicate.R <input_file> [<output_file>]')
}
INPUT_FILE <- args[1]
# INPUT_FILE <- '/home/keripukki/dataa/sujuiko/hfp_tidy/hfp_1500_1_2020-09-23.csv.gz'
stopifnot(file.exists(INPUT_FILE))
OUTPUT_FILE <- ifelse(length(args) > 1, args[2], INPUT_FILE)
# OUTPUT_FILE <- '/home/keripukki/dataa/sujuiko/hfp_tidy/dedup_1500_1_2020-09-23.csv.gz'

tmessage <- function(...) {
  message('[', Sys.time(), ']; deduplicate; ', ...)
}

tmessage('Reading ', INPUT_FILE)

dt <- fread(file = INPUT_FILE)
nrow_orig <- nrow(dt)
if (nrow_orig == 0) {
  tmessage('No rows in original file, exiting')
  stop(call. = FALSE)
}
tmessage(nrow(dt), ' rows in original file')

dt_dedup <- unique(
  dt[order(route, dir, oday, start, oper, veh, tst, event_type), ]
)
nrow_dedup <- nrow(dt_dedup)
n_duplicated <- nrow_orig - nrow_dedup

tmessage(n_duplicated, ' duplicate rows removed')

fwrite(x = dt_dedup, file = OUTPUT_FILE)
tmessage(nrow_dedup, ' rows written to ', OUTPUT_FILE)
