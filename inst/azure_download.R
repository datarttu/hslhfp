#!/usr/bin/env Rscript
#'
#' Download datasets from storage.
#' Work in progress!!

azure_storage_url <- Sys.getenv('AZURE_STORAGE_URL')
stopifnot(azure_storage_url != '')
target_hfp_dir <- Sys.getenv('TARGET_HFP_DIR')
stopifnot(target_hfp_dir != '')
stopifnot(dir.exists(target_hfp_dir))

tmessage <- function(...) {
  message('[', Sys.time(), '] ', ...)
}

suppressMessages(library(dplyr))
suppressMessages(library(purrr))
suppressMessages(library(vroom))
suppressMessages(library(hslhfp))

args <- commandArgs(trailingOnly = TRUE)

if (length(args) != 3) {
  stop('Usage: ./azure_download.R <yyyy-mm-dd> <blob_prefix1,blob_prefix2,...> <result_prefix1,result_prefix2,...>')
}

d <- args[1]
blob_prefixes <- strsplit(args[2], split = ',') %>%
  unlist()
res_prefixes <- strsplit(args[3], split = ',') %>%
  unlist()
stopifnot(length(blob_prefixes) == length(res_prefixes))

tmessage(sprintf('Starting downloads
                 from %s with date %s,
                 prefixes %s and %s,
                 saving results to %s',
                 azure_storage_url,
                 d,
                 paste(blob_prefixes, collapse = ','),
                 paste(res_prefixes, collapse = ','),
                 target_hfp_dir))

# TODO Drop obligatory directory for raw hfp files from the list_downloadable func.
raw_hfp_dir <- tempdir()
on.exit(unlink(raw_hfp_dir))

candidate_urls <- pmap_dfr(
  .l = tibble(d = d,
              storage_url = azure_storage_url,
              blob_prefix = blob_prefixes,
              result_prefix = res_prefixes,
              target_dir = raw_hfp_dir),
  .f = list_downloadable_by_date
)
available_urls <- candidate_urls %>%
  filter(remote_file_exists)
n_urls <- nrow(available_urls)
tmessage(sprintf('%d remote files available for download (%d tried in total)',
                 nrow(candidate_urls),
                 n_urls))

raw_col_names <- raw_hfp_col_names()

# Testing with a custom subset of columns
col_spec <- cols_only(
  event_type = col_character(),
  route = col_character(),
  dir = col_integer(),
  oper = col_integer(),
  veh = col_integer(),
  oday = col_double(),
  start = col_character(),
  stop = col_character(),
  tst = col_double(),
  tsi = col_double(),
  loc = col_character(),
  lat = col_double(),
  long = col_double(),
  spd = col_double(),
  odo = col_integer(),
  drst = col_logical(),
  occu = col_integer()
)

i <- 0
for (url in available_urls$full_url) {
  i <- i + 1
  tmessage(sprintf('%d/%d %s ...',
                   i, n_urls, url))
  Sys.sleep(0.5)
}

