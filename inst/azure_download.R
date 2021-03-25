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
suppressMessages(library(glue))
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
  stop = col_integer(),
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

# This is used for saving results by group into different files
outfile_template <- file.path(target_hfp_dir, 'hfp_{oday}_{route}_{dir}.csv.gz')

i <- 0

for (url in available_urls$full_url) {
  i <- i + 1
  tmessage(sprintf('%d/%d %s ...',
                   i, n_urls, url))

  # TODO For some reason routes with whitespace, e.g. "1006 3" are read incorrectly,
  #      including everything after the comma
  raw_df <- vroom(
    file = url,
    delim = ',',
    col_names = raw_col_names,
    col_types = col_spec
  )
  tmessage(sprintf('%d lines read', nrow(raw_df)))

  # TODO Route filter as script argument?
  res_df <- raw_df %>%
    filter(route %in% c('1056', '1059'))

  # TODO NA odays and dirs should be filtered here

  tmessage(sprintf('%d lines after filtering', nrow(res_df)))

  res_df <- cast_datetime_cols(res_df)
  res_df <- datetimes_as_character(res_df)

  # TODO Reorder columns by col_spec inside a function
  res_df <- res_df %>%
    select(names(col_spec$cols))

  # TODO Rethink this pipeline & make into function,
  #      currently it causes vroom::problems() warnings
  res_df <- res_df %>%
    group_by(route, dir, oday)

  # TODO Stop if group keys do not match the variables in the template.
  #      Or take the keys from the template,
  #      and stop in the beginning if a key column is not in the col_spec?
  grp_df <- group_keys(res_df) %>%
    mutate(outfile = glue(outfile_template))
  res_list <- group_split(res_df) %>%
    setNames(grp_df$outfile)
  res_split <- tibble(path = names(res_list),
                      x = res_list)

  # TODO: Return df of filename & append==TRUE/FALSE
  #       so newly written / appended files can be reported along the way.
  pwalk(res_split, hfp_vroom_write)

  tmessage(sprintf('%d files written or appended to', nrow(res_split)))

}
