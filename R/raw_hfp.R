#' Get default column names for raw HFP data
#'
#' Returns a character vector of default column names of raw HFP event data
#' in storage (44 columns in total).
#' The original csv datasets do not have column names.
#'
#' @return A character vector of column names
#'
#' @export
raw_hfp_col_names <- function() {
  return(
    c(
      'acc',
      'desi',
      'dir',
      'direction_id',
      'dl',
      'dr_type',
      'drst',
      'event_type',
      'geohash_level',
      'hdg',
      'headsign',
      'is_ongoing',
      'journey_start_time',
      'journey_type',
      'jrn',
      'lat',
      'line',
      'loc',
      'long',
      'mode',
      'next_stop_id',
      'occu',
      'oday',
      'odo',
      'oper',
      'owner_operator_id',
      'received_at',
      'route_id',
      'route',
      'seq',
      'spd',
      'start',
      'stop',
      'topic_latitude',
      'topic_longitude',
      'topic_prefix',
      'topic_version',
      'tsi',
      'tst',
      'unique_vehicle_id',
      'uuid',
      'veh',
      'vehicle_number',
      'version'
    )
  )
}

#' Get default column type specification for raw HFP data
#'
#' Returns a [`cols()`](https://readr.tidyverse.org/reference/cols.html) specification
#' of default columns in raw HFP event data in storage (44 columns in total).
#'
#' @export
raw_hfp_col_spec <- function() {
  return(
    vroom::cols(
      acc = vroom::col_double(),
      desi = vroom::col_integer(),
      dir = vroom::col_integer(),
      direction_id = vroom::col_integer(),
      dl = vroom::col_integer(),
      dr_type = vroom::col_character(),
      drst = vroom::col_logical(),
      event_type = vroom::col_character(),
      geohash_level = vroom::col_integer(),
      hdg = vroom::col_integer(),
      headsign = vroom::col_character(),
      is_ongoing = vroom::col_logical(),
      journey_start_time = vroom::col_character(),
      journey_type = vroom::col_character(),
      jrn = vroom::col_integer(),
      lat = vroom::col_double(),
      line = vroom::col_integer(),
      loc = vroom::col_character(),
      long = vroom::col_double(),
      mode = vroom::col_character(),
      next_stop_id = vroom::col_integer(),
      occu = vroom::col_integer(),
      oday = vroom::col_double(),
      odo = vroom::col_integer(),
      oper = vroom::col_integer(),
      owner_operator_id = vroom::col_character(),
      received_at = vroom::col_double(),
      route_id = vroom::col_character(),
      route = vroom::col_character(),
      seq = vroom::col_integer(),
      spd = vroom::col_double(),
      start = vroom::col_character(),
      stop = vroom::col_integer(),
      topic_latitude = vroom::col_double(),
      topic_longitude = vroom::col_double(),
      topic_prefix = vroom::col_character(),
      topic_version = vroom::col_character(),
      tsi = vroom::col_double(),
      tst = vroom::col_double(),
      unique_vehicle_id = vroom::col_character(),
      uuid = vroom::col_character(),
      veh = vroom::col_integer(),
      vehicle_number = vroom::col_integer(),
      version = vroom::col_integer()
    )
  )
}

#' Cast UNIX epoch milliseconds to UTC datetime timestamp
#'
#' @param x A numeric vector of UTC milliseconds since 1970-01-01
#'
#' @return A `lubridate` date-time / POSIXct object
#' @export
milliseconds_to_timestamp <- function(x) {
  assertthat::assert_that(class(x) %in% c('numeric', 'integer'))
  x <- x / 1000.0
  x <- lubridate::as_datetime(x, origin = lubridate::origin, tz = 'UTC')
  return(x)
}

#' Cast UNIX epoch milliseconds to UTC date
#'
#' @param x A numeric vector of UTC milliseconds since 1970-01-01
#'
#' @return A Date object
#' @export
milliseconds_to_date <- function(x) {
  x <- milliseconds_to_timestamp(x)
  x <- lubridate::date(x)
  return(x)
}

#' Cast UNIX milliseconds columns to date / timestamp
#'
#' @param x A data frame (of raw HFP data)
#' @param ts_cols Chr names of columns to cast from ms to timestamp
#' @param date_cols Chr names of columns to cast from ms to date
#'
#' @return A data frame
#' @export
cast_datetime_cols <- function(x,
                               ts_cols = c('received_at', 'tst'),
                               date_cols = c('oday')) {
  ts_cols <- intersect(ts_cols, colnames(x))
  if (length(ts_cols) > 0) {
    x <- dplyr::mutate(x, dplyr::across({{ts_cols}}, milliseconds_to_timestamp))
  }
  date_cols <- intersect(date_cols, colnames(x))
  if (length(date_cols) > 0) {
    x <- dplyr::mutate(x, dplyr::across({{date_cols}}, milliseconds_to_date))
  }
  return(x)
}
