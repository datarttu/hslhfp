#' Format dates and timestamps for csv output
#'
#' @param x A data frame of HFP data
#' @param ts_format Datetime format string for POSIXct columns
#' @param date_format Date format string for Date columns
#'
#' @return A data frame
#'
#' @export
datetimes_as_character <- function(x,
                                   ts_format = '%Y-%m-%dT%H:%M:%OS3Z',
                                   date_format = '%Y-%m-%d') {
  x <- dplyr::mutate(
    x,
    dplyr::across(lubridate::is.POSIXct,
                  function(el) as.character(el, format = ts_format))
  )
  x <- dplyr::mutate(
    x,
    dplyr::across(lubridate::is.Date,
                  function(el) as.character(el, format = date_format))
  )
  return(x)
}

#' Wrapper for `vroom::vroom_write` with custom default values
#'
#' If `path` already exists, new data is appended to the file,
#' otherwise a new file with col names is created.
#'
#' @param x A data frame of HFP data
#' @param path Path to write to
#' @param delim Delimiter to use in result file
#' @param na String used for missing values
#' @param quote Quote character columns ("needed", "all" or "none")
#' @param progress Show progress bar
#'
#' @export
hfp_vroom_write <- function(x, path, delim = ',', na = '',
                            quote = 'needed', progress = FALSE) {
  append <- file.exists(path)
  vroom::vroom_write(x = x, path = path, delim = delim, na = na,
                     append = append, quote = quote, progress = FALSE)
}
