#!/usr/bin/env Rscript
#'
#' Download and prepare hfp data using data.table.
#' This might be faster than dplyr in our case.
#'
#' WIP!
#' -> Decompose this into functions later if seems to work nicely.
#'
#' Handle all HFP data for a DATE:
#' - Get raw data from all hours and required event types,
#'   keep only required cols and type-cast date and timestmap cols
#'   filter by route and omit rows with NA values in important cols,
#'   save to hfp_yyyy-mm-dd_route_dir.csv.gz files
#'   - Report N of discarded rows for different reasons as msg to stdout,
#'     use structured log entries
#' - Re-read the result file, sort and deduplicate entire rows,
#'   overwrite .csv.gz file
#'   - Report N of rows discarded by deduplication

suppressMessages(library(data.table))
suppressMessages(library(stringr))
suppressMessages(library(purrr))

azure_storage_url <- Sys.getenv('AZURE_STORAGE_URL')
stopifnot(azure_storage_url != '')
target_hfp_dir <- Sys.getenv('TARGET_HFP_DIR')
stopifnot(target_hfp_dir != '')
stopifnot(dir.exists(target_hfp_dir))

#' Command line arguments
#' TODO: Could we use named args?
#' ARG 1: date of which the data is downloaded
#' ARG 2: comma-separated blob prefixes
#' ARG 3: path to text file containing route codes to include (no header)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 3) {
  stop('Usage: ./dt_pipeline.R <yyyy-mm-dd> <blob_prefix1,blob_prefix2,...> <routes_file>')
}
DATE_SEL <- args[1]
stopifnot(str_detect(DATE_SEL, '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'))

BLOB_PREFIXES <- strsplit(args[2], split = ',') %>%
  unlist()

ROUTE_SEL_FILE <- args[3]
stopifnot(file.exists(ROUTE_SEL_FILE))
route_sel <- fread(file = ROUTE_SEL_FILE, header = FALSE,
                   colClasses = 'character', col.names = 'route')$route

#' Structured log entries:
#' - Timestamp
#' - Dataset date
#' - Message
tmessage <- function(...) {
  message('[', Sys.time(), ']; ', DATE_SEL, '; ', ...)
}

#' Col names and types
#' fread uses default names V1, V2, ... if headers are not used.
result_col_select <- c('V8' = 'character',
                       'V29' = 'character',
                       'V3' = 'integer',
                       'V25' = 'integer',
                       'V42' = 'integer',
                       'V23' = 'integer64',
                       'V32' = 'character',
                       'V33' = 'integer',
                       'V39' = 'integer64',
                       'V38' = 'integer64',
                       'V18' = 'character',
                       'V16' = 'numeric',
                       'V19' = 'numeric',
                       'V31' = 'numeric',
                       'V24' = 'numeric',
                       'V7' = 'logical',
                       'V22' = 'numeric',
                       'V27' = 'integer64',
                       'V12' = 'logical')
result_col_names <- c("event_type",
                      "route",
                      "dir",
                      "oper",
                      "veh",
                      "oday",
                      "start",
                      "stop",
                      "tst",
                      "tsi",
                      "loc",
                      "lat",
                      "long",
                      "spd",
                      "odo",
                      "drst",
                      "occu",
                      "received_at",
                      "is_ongoing")

#' fwrite wrapper that appends if the file already exists,
#' otherwise writes headers too
fwrite_append_on_existing <- function(x, file, ...) {
  fwrite(x = x, file = file, append = file.exists(file), ...)
}

tmessage('Starting downloads from ', azure_storage_url,
         ' to ', target_hfp_dir)
for (pref in BLOB_PREFIXES) {
  for (hr in 0:23) {
    source_blob <- sprintf('%s/%s-%02d.csv',
                           pref, DATE_SEL, hr)
    raw_filename <- tempfile()
    tmessage('Downloading ', source_blob, ' to ', raw_filename)
    from_url <- sprintf('%s/%s', azure_storage_url, source_blob)
    res <- tryCatch(
      {
        download.file(url = from_url, destfile = raw_filename, quiet = TRUE)
      },
      error = function(e) e
    )
    if (inherits(res, 'error')) {
      tmessage('Cannot download ', from_url, ', skipping to next')
      unlink(raw_filename)
      next
    }

    tmessage('Downloaded ', source_blob, ' read to dt ...')
    dt <- fread(file = raw_filename,
                header = FALSE,
                select = result_col_select,
                col.names = result_col_names)

    #' Store nrows after each step to a data frame for controlled reporting
    nrow_report <- data.frame(step = 'raw data',
                              nrow = nrow(dt),
                              stringsAsFactors = FALSE)
    tmessage('dt read')

    dt <- dt[!is.na(route) & !is.na(dir) & !is.na(oday), ]
    nrow_report <- rbind(nrow_report,
                         list('route, dir and oday not NA', nrow(dt)))

    dt <- dt[route %in% route_sel, ]
    nrow_report <- rbind(nrow_report,
                         list(sprintf('route in selected %d routes', length(route_sel)), nrow(dt)))
    nrow_report_string <- paste(
      paste0(nrow_report$step, ': ', nrow_report$nrow),
      collapse = ', '
    )
    tmessage('dt filtered, nrows: ', nrow_report_string)

    if (nrow(dt) == 0) {
      tmessage('0 rows left, skipping to next')
      unlink(raw_filename)
      next
    }

    #' Type conversions: must be done here at least for oday
    #' so we can use it in the output file names.
    dt[, oday := as.Date(as.POSIXct(oday / 1000, tz = 'UTC', origin = '1970-01-01'))]
    dt[, tst := as.POSIXct(tst / 1000, tz = 'UTC', origin = '1970-01-01')]
    dt[, received_at := as.POSIXct(received_at / 1000, tz = 'UTC', origin = '1970-01-01')]
    tmessage('oday, tst and received_at types converted')

    dt[, fname_out := sprintf('%s/hfp_%s_%d_%s.csv.gz', target_hfp_dir, route, dir, format(oday, '%Y-%m-%d'))]
    dt_list <- split(dt, by = 'fname_out', keep.by = FALSE)
    tmessage('Writing dt to ', length(dt_list), ' files by route, dir, oday in ', target_hfp_dir)

    invisible(purrr::walk2(.x = dt_list, .y = names(dt_list), .f = fwrite_append_on_existing))
    tmessage(sprintf('%s %02d ready', pref, hr))

    unlink(raw_filename)
  }
}
