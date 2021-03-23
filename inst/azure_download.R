#!/usr/bin/env Rscript
#'
#' Download datasets from storage by given dates.
#' NOTE: AZURE_STORAGE_URL and RAW_HFP_DIR must be defined in .Renviron file.

readRenviron('../.Renviron')

args <- commandArgs(trailingOnly = TRUE)

if (length(args) == 0) {
  stop('Example usage: ./azure_download.R 2020-09-21 2020-09-22')
}

dates <- args

for (d in dates) {
  tryCatch({
    hslhfp::download_blobs_by_date(d)
  }, finally = next)
}
