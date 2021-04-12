#!/usr/bin/env Rscript
#'
#' Normalize HFP data into journey and obs tables.
#'
#' - For each `route_dir_oday.csv.gz` file:
#'   - If more than one route-dir-oday combo present, raise a warning
#'     and only use the first combo as dataset identifier
#'   - Calculate unique journey id `jrnid`
#'   - Leave per-observation fields along with `jrnid` into table `obs`
#'   - Summarise per-journey fields along with `jrnid` into table `jrn`
#'   - Write into `obs_{route_dir_oday}.csv.gz`
#'     and `jrn_{route_dir_oday}.csv.gz`
#' - Write a metadata table containing the dataset names and row counts
#'   for each route-dir-oday dataset
#'
#' Usage: either
#' > ./dt_normalize_jrns.R <input_dir> <output_dir>
#' OR
#' > ./dt_normalize_jrns.R <input_output_dir>

suppressMessages(library(data.table))
suppressMessages(library(purrr))
suppressMessages(library(stringr))
suppressMessages(library(digest))

#' This is needed for .csv.gz reading with fread to work
stopifnot('R.utils' %in% installed.packages())

args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop('Usage: ./dt_normalize_jrns.R <input_dir> [<output_dir>]')
}
INPUT_DIR <- args[1]
stopifnot(dir.exists(INPUT_DIR))
OUTPUT_DIR <- ifelse(length(args) > 1, args[2], INPUT_DIR)
stopifnot(dir.exists(OUTPUT_DIR))

tmessage <- function(...) {
  message('[', Sys.time(), ']; normalize; ', ...)
}

input_files <- list.files(
  path = INPUT_DIR,
  pattern = '*.csv.gz',
  full.names = TRUE
)

#' This makes a vectorized version of the digest() function with md5 algorithm
md5 <- digest::getVDigest(algo = 'md5')

handle_file <- function(infile, outdir) {
  tmessage('Reading ', infile)
  dt <- fread(file = infile)
  if (nrow(dt) == 0) {
    warning('No data, skipping')
    return()
  }

  route_dir_oday_combos <- unique(dt[, .(route, dir, oday)])
  if (nrow(route_dir_oday_combos) > 1) {
    warning('Multiple route-dir-oday combinations in file: using the first only')
  }
  dataset_name <- route_dir_oday_combos[1, sprintf(
    '%s_%d_%s', route, dir, format(oday, '%Y-%m-%d'))]

  dt[, jrnid := md5(
    object = sprintf(
      '%s_%d_%s_%s_%d_%d',
      route, dir, format(oday, '%Y-%m-%d'), start, oper, veh),
    serialize = FALSE)]
  obs <- dt[, !c('route', 'dir', 'oday', 'start', 'oper', 'veh')]
  obs <- obs[order(jrnid, tst, event_type)]
  jrn <- unique(dt[, c('jrnid', 'route', 'dir', 'oday', 'start', 'oper', 'veh')])
  jrn <- jrn[order(route, dir, oday, start, oper, veh), ]

  obs_file <- sprintf('obs_%s.csv.gz', dataset_name)
  obs_path <- file.path(outdir, obs_file)
  fwrite(x = obs, file = obs_path)
  obs_nrow <- nrow(obs)
  tmessage(obs_nrow, ' rows written to ', obs_path)

  jrn_file <- sprintf('jrn_%s.csv.gz', dataset_name)
  jrn_path <- file.path(outdir, jrn_file)
  fwrite(x = jrn, file = jrn_path)
  jrn_nrow <- nrow(jrn)
  tmessage(jrn_nrow, ' rows written to ', jrn_path)

  return(list(
    dataset = dataset_name,
    nrow_obs = obs_nrow,
    nrow_jrn = jrn_nrow
  ))
}

res <- purrr::map2_dfr(.x = input_files, .y = OUTPUT_DIR, .f = handle_file)
res_path <- file.path(OUTPUT_DIR, 'jrn_obs_metadata.csv')
fwrite(x = res, file = res_path)
tmessage('jrn & obs metadata written to ', res_path)
