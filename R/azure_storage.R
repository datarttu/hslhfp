#' List csv available & not locally existing csv dumps from Azure storage
#'
#' Returns a tibble of datasets by date `d`, indicated whether the datasets
#' 1) exist in Azure Storage and
#' 2) do not already exist in the `target_dir`.
#'
#' @param d A Date or a string of format `YYYY-MM-DD`
#' @param storage_url A storage URL string (defaults to env var `AZURE_STORAGE_URL`)
#' @param blob_prefix A prefix defining a subset of blobs (defaults to `csv/VehiclePosition`)
#' @param result_prefix A prefix to be added to the result file names (defaults to `vp`)
#' @param target_dir Path to the target dir of downloaded files (defaults to env var `RAW_HFP_DIR`)
#'
#' @return A tibble containing remote and local dataset info.
#'
#' @export
list_downloadable_by_date <- function(d, storage_url = Sys.getenv('AZURE_STORAGE_URL'),
                                      blob_prefix = 'csv/VehiclePosition',
                                      result_prefix = 'vp',
                                      target_dir = Sys.getenv('RAW_HFP_DIR')
) {
  assertthat::is.scalar(d)
  assertthat::assert_that(assertthat::is.date(d) |
                            stringr::str_detect(d, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$')
  )
  assertthat::assert_that(dir.exists(target_dir))

  if (inherits(d, 'Date')) {
    d <- as.character(d, format = '%Y-%m-%d')
  }

  storage_url <- stringr::str_replace(storage_url, '/$', '') %>%
    stringr::str_replace('^/', '')
  blob_prefix <- stringr::str_replace(blob_prefix, '/$', '') %>%
    stringr::str_replace('^/', '')
  target_dir <- stringr::str_replace(target_dir, '/$', '')

  candidates <- tibble::tibble(
    date_hour_str = sprintf('%s-%02d', d, seq(0, 23, 1)),
    csv_name = sprintf('%s_%s.csv', result_prefix, date_hour_str),
    local_path = file.path(target_dir, csv_name),
    full_url = sprintf('%s/%s/%s.csv', storage_url, blob_prefix, date_hour_str)
  ) %>%
    dplyr::mutate(local_file_exists = purrr::map_lgl(local_path, file.exists) |
                    purrr::map_lgl(paste0(local_path, '.gz'), file.exists)) %>%
    dplyr::mutate(remote_file_exists = !(purrr::map_lgl(full_url, httr::http_error)))

  return(candidates)
}

#' Download csv dumps by date from Azure storage
#'
#' HFP csv dumps are saved by date and hour in the HSL Azure blob storage.
#' This function tries to download all the available dumps from a given date.
#' If a csv file with the same name already exists in `target_dir`,
#' it is not downloaded (and should be removed or renamed manually, if necessary).
#'
#' @param d A Date or a string of format `YYYY-MM-DD`
#' @param storage_url A storage URL string (defaults to env var `AZURE_STORAGE_URL`)
#' @param blob_prefix A prefix defining a subset of blobs (defaults to `csv/VehiclePosition`)
#' @param result_prefix A prefix to be added to the result file names (defaults to `vp`)
#' @param target_dir Path to the target dir of downloaded files (defaults to env var `RAW_HFP_DIR`)
#' @param verbose If `TRUE`, warn about missing or locally existing datasets and report downloads
#'
#' @export
download_blobs_by_date <- function(d, storage_url = Sys.getenv('AZURE_STORAGE_URL'),
                                   blob_prefix = 'csv/VehiclePosition',
                                   result_prefix = 'vp',
                                   target_dir = Sys.getenv('RAW_HFP_DIR'),
                                   verbose = TRUE) {
  candidates <- list_downloadable_by_date(d, storage_url, blob_prefix, target_dir)

  remote_missing <- candidates %>%
    dplyr::filter(!remote_file_exists)
  if (verbose & nrow(remote_missing) > 0) {
    warning('Following files not available for download:\n',
            paste('  ', remote_missing$csv_name, collapse = '\n'),
            call. = FALSE)
  }

  locally_existing <- candidates %>%
    dplyr::filter(local_file_exists & remote_file_exists)
  if (verbose & nrow(locally_existing) > 0) {
    warning('Following files already in ', target_dir, ':\n',
            paste('  ', locally_existing$csv_name, collapse = '\n'),
            call. = FALSE)
  }

  td <- candidates %>%
    dplyr::filter(remote_file_exists & !local_file_exists)
  if (nrow(td) == 0) {
    if (verbose) {
      message('No files to download, exiting')
    }
    return(NULL)
  }

  dwnl_len <- nrow(td)
  for (i in seq(1, dwnl_len, 1)) {
    dwnl_start <- Sys.time()
    url <- td$full_url[i]
    message('Downloading ', i, '/', dwnl_len, ': ', url)
    utils::download.file(url,
                         destfile = td$local_path[i],
                         quiet = TRUE)
    message('Compressing ', td$local_path[i])
    R.utils::gzip(td$local_path[i])
    dwnl_end <- Sys.time()
    message(td$local_path[i], '.gz ready (start ',
            as.character(dwnl_start, format = '%Y-%m-%d %H:%M:%S%z'),
            '; end ',
            as.character(dwnl_start, format = '%Y-%m-%d %H:%M:%S%z'),
            '; duration ',
            round(as.numeric(dwnl_end - dwnl_start, units = 'secs'), 1),
            ' s')
  }
}
