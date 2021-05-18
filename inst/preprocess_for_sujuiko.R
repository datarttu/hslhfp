#' Preprocessing of a HFP dataset for the sujuiko model
#'
#' - Reads a HFP dataset name as command line argument, like
#'   {route}_{dir}_{oday}
#' - Normalizes and filters the data so only valid journeys and observations
#'   can be imported to the database
#' - Writes three files:
#'   - jrn_{route}_{dir}_{oday}.csv.gz
#'   - obs_{route}_{dir}_{oday}.csv.gz
#'   - log_{route}_{dir}_{oday}.csv.gz
#'
#' Arttu K / HSL 5/2021
library(data.table)
library(sf)
library(Rcpp)
library(digest)
library(stringr)

validate_hfp_name <- function(x) {
  x <- str_split_fixed(x, '_', n = 3)
  return(
    # Route part of right length (should also check for alphanum + space only)
    str_length(x[, 1]) > 3 &
      str_length(x[, 1]) < 7 &
      # Dir part exactly 1 or 2
      (x[, 2] == '1' | x[, 2] == '2') &
      # Date part is of right length and format (date parts not checked...)
      str_length(x[, 3]) == 10 &
      str_detect(x[, 3], '[0-9]{4}-[0-9]{2}-[0-9]{2}')
  )
}

args <- commandArgs(trailingOnly = TRUE)
INPUT_HFP_NAME <- args[1]
stopifnot(validate_hfp_name(INPUT_HFP_NAME))

# In-out file paths ----

INPUT_HFP_PATH <- file.path(Sys.getenv('TARGET_HFP_DIR'),
                            'raw',
                            sprintf('hfp_%s.csv.gz', INPUT_HFP_NAME))
stopifnot(file.exists(INPUT_HFP_PATH))
output_dir <- file.path(Sys.getenv('TARGET_HFP_DIR'), 'final')
stopifnot(dir.exists(output_dir))
OUTPUT_JRN_PATH <- file.path(output_dir, sprintf('jrn_%s.csv.gz', INPUT_HFP_NAME))
OUTPUT_OBS_PATH <- file.path(output_dir, sprintf('obs_%s.csv.gz', INPUT_HFP_NAME))
OUTPUT_LOG_PATH <- file.path(output_dir, sprintf('log_%s.csv.gz', INPUT_HFP_NAME))

message(OUTPUT_JRN_PATH)

# Parameters ----

#' We have several parameters for filtering & validation
#' that are more or less arbitrary and we might want to change them
#' and see how it affects the results.

#' HSL bounding box - WGS84 coordinate ranges the points must fall within
#' so they are considered valid and not removed.
ACCEPT_LONG_RANGE <- c(24.0, 25.6)
ACCEPT_LAT_RANGE <- c(60.0, 60.5)

#' Acceptable difference between point tst and its received_at in seconds.
#' Points registered too early or late can indicate device clock errors.
ACCEPT_TST_RECEIVED_DIFF_RANGE_SECS <- c(-60, 120)

#' Acceptable difference between point tst and the scheduled journey start time in minutes.
#' Points too early or way too late are not considered valid.
ACCEPT_TST_START_DIFF_RANGE_MINS <- c(-5, 120)

#' Maximum allowed speed between consecutive points in m/s.
MAX_POINT_SPD_METERS_PER_SEC <- 30

#' Gap between points in seconds, if this is exceeded then the second point
#' starts a new subjourney that is treated as a separate group of points.
SUBJRN_THRESHOLD_SECS <- 120

#' Subjourney must last at least this long by min and max timestamps,
#' otherwise it is dropped.
MIN_SUBJRN_DURATION_MINS <- 5

#' Acceptable relation between cumulative gps distance and odo distance.
#' 1 = odometer perfectly corresponds to the gps distance (or at least its variation
#' is canceled out). Usually the GPS overestimates the real distance a bit due to
#' high sampling rate and jitter.
ACCEPT_GPS_VS_ODO_SLOPE_RANGE <- c(0.9, 1.1)

#' Minimum total time in seconds that consecutive points with the same odometer value
#' must have so they are considered a "halt", i.e. the vehicle is stopped at one point.
MIN_HALT_DURATION_SECS <- 5

#' Proportion (0 ... 1.0) of points with NA drst of all points of a journey:
#' if this is exceeded, then the door sensor is considered defect for the
#' whole journey.
DRST_DEFECT_THRESHOLD_PROPORTION <- 0.5

#' Proportion of points with drst == TRUE (doors open) of all moving points of a journey:
#' if this is exceeded, then the door sensor is considered to work in reverse
#' and all drst values of that journey are inverted to fix this.
DRST_INVERT_THRESHOLD_PROPORTION <- 0.5

# Define logging logic ----
logdt <- data.table(
  dataset_name = INPUT_HFP_PATH,
  log_timestamp = Sys.time(),
  action = 'Start processing',
  jrnid = NA_character_,
  n_rows = NA_integer_
)

logger <- function(action, x) {
  if (!('jrnid' %in% colnames(x))) {
    x$jrnid <- NA_character_
  }
  if (nrow(x) == 0) {
    x = data.table(jrnid = NA_character_, n_rows = 0)
  }
  logdt <<- rbind(
    logdt,
    data.table(
      dataset_name = INPUT_HFP_NAME,
      log_timestamp = Sys.time(),
      action = action,
      jrnid = x$jrnid,
      n_rows = x$n_rows
    )
  )
}

# Read in, start filtering ----
dt <- fread(file = INPUT_HFP_PATH)
logger('Raw file read', data.table(n_rows = nrow(dt)))

dt[, drop_row := FALSE]

dt[is_ongoing == FALSE | is.na(is_ongoing), drop_row := TRUE]
logger('is_ongoing FALSE or NA', dt[drop_row == TRUE, .(n_rows = .N)])
dt <- dt[drop_row == FALSE, ]

dt <- dt[, .(received_at, tst, event_type,
             route, dir, oday, start, oper, veh, long, lat, odo, drst, drop_row)]

#' This is done step by step so we can debug individual conditions if needed.
dt[is.na(event_type), drop_row := TRUE]
dt[is.na(received_at), drop_row := TRUE]
dt[is.na(tst), drop_row := TRUE]
dt[is.na(route), drop_row := TRUE]
dt[is.na(dir), drop_row := TRUE]
dt[is.na(oday), drop_row := TRUE]
dt[is.na(start), drop_row := TRUE]
dt[is.na(oper), drop_row := TRUE]
dt[is.na(veh), drop_row := TRUE]
logger('NA event_type, received_at, tst, route, dir, oday, start, oper or veh',
       dt[drop_row == TRUE, .(n_rows = .N)])
dt <- dt[drop_row == FALSE, ]

# Normalize -> obs, jrn ----
#' This makes a vectorized version of the digest() function with md5 algorithm
md5 <- digest::getVDigest(algo = 'md5')
dt[, jrnid := md5(
  object = sprintf(
    '%s_%d_%s_%s_%d_%d',
    route, dir, format(oday, '%Y-%m-%d'), start, oper, veh),
  serialize = FALSE)]

obs <- dt[, !c('route', 'dir', 'oday', 'start', 'oper', 'veh')]
obs <- obs[order(jrnid, tst, event_type)]
jrn <- unique(dt[, c('jrnid', 'route', 'dir', 'oday', 'start', 'oper', 'veh')])
jrn <- jrn[order(route, dir, oday, start, oper, veh), ]
logger('Normalized into journeys', jrn[, .(n_rows = .N)])
logger('Initial N of obs per jrnid', obs[, .(n_rows = .N), by = jrnid])

# UTC journey start time ----
#' Note that oday and start are assumed in local Europe/Helsinki time.
jrn[, start_tst := as.POSIXct(
  x = sprintf('%s %s', oday, start),
  tz = 'Europe/Helsinki',
  format = '%Y-%m-%d %H:%M:%S'
)]

# Drop invalid rows by values ----

obs[(is.na(long) | is.na(lat)),
    drop_row := TRUE]
logger('Rows with NA long or NA lat',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

obs[!(long %between% ACCEPT_LONG_RANGE & lat %between% ACCEPT_LAT_RANGE),
    drop_row := TRUE]
logger('Rows with lat or long out of HSL bounds',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

obs[is.na(odo), drop_row := TRUE]
logger('Rows with NA odo',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

obs[event_type != 'VP', drop_row := TRUE]
logger('Rows with event_type != VP',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

obs[!(as.numeric(
  received_at - tst, units = 'secs'
) %between% ACCEPT_TST_RECEIVED_DIFF_RANGE_SECS),
drop_row := TRUE]
logger('Rows with received_at - tst diff too large',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

obs <- merge(obs, jrn[, .(jrnid, start_tst)], by = 'jrnid', all.x = TRUE)
obs[!(as.numeric(
  tst - start_tst, units = 'mins'
) %between% ACCEPT_TST_START_DIFF_RANGE_MINS),
drop_row := TRUE]
logger('Rows with tst - start_tst diff too large',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]
obs <- obs[, -c('start_tst')]

# Drop duplicates ----
obs <- obs[order(jrnid, tst, received_at)]
obs[, drop_row := duplicated(obs, by = c('jrnid', 'tst'))]
logger('Duplicate rows',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

# From WGS84 to TM35 ----
obs <- st_as_sf(obs, coords = c('long', 'lat'), crs = 4326)
obs <- st_transform(obs, crs = 3067)
obs$X <- st_coordinates(obs)[, 1]
obs$Y <- st_coordinates(obs)[, 2]
obs <- st_drop_geometry(obs)
obs <- as.data.table(obs)

# Remove outliers by speed ----
#' Speed in m/s is calculated for consecutive points but only with respect to
#' the previous _valid_ point. If a point exceeds the speed limit, it is invalid.
#' This "dynamic" calculation, where the next iteration depends on the result
#' of the previous one, is not easily done with R and therefore we use C++
#' that handles simple loops way faster than R.
#' Note that the first point of a journey is always considered valid.
#' In rare situations where the first point is far away from the others,
#' this can lead to spurious results.
cppFunction(
  'LogicalVector dynam_speed_valid(
  NumericVector x, NumericVector y, NumericVector t, double threshold) {

  int n = x.size();
  LogicalVector out(n);
  double delta_dist;
  double delta_t;
  double last_valid_x = x[0];
  double last_valid_y = y[0];
  double last_valid_t = t[0];
  out[0] = true;

  for(int i = 1; i < n; i++) {
    delta_dist = sqrt(pow(x[i] - last_valid_x, 2.0) + pow(y[i] - last_valid_y, 2.0));
    delta_t = t[i] - last_valid_t;
    if (delta_dist / delta_t <= threshold) {
      out[i] = true;
      last_valid_x = x[i];
      last_valid_y = y[i];
      last_valid_t = t[i];
    } else {
      out[i] = false;
    }
  }
  return out;
}'
)
obs <- obs[, drop_row := !dynam_speed_valid(x = X,
                                            y = Y,
                                            t = as.numeric(tst),
                                            threshold = MAX_POINT_SPD_METERS_PER_SEC),
           by = jrnid]
logger('Point speed limit exceeded',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

# Determine subjourneys ----
#' Subjourney = if two consecutive points have a long time difference,
#' then the journey is split there into subparts that are later treated as
#' separate groups. For example, trajectories along links should not be
#' interpolated between subjourneys since the section in between is considered
#' unrealiable in terms of data coverage.
obs[, subjrn := as.integer(as.numeric(
  tst - shift(tst, type = 'lag'), units = 'secs'
) > SUBJRN_THRESHOLD_SECS),
by = jrnid]
obs[is.na(subjrn), subjrn := 1]
obs[, subjrn := cumsum(subjrn), by = jrnid]

logger('Number of subjourneys', obs[, .(n_rows = max(subjrn)), by = jrnid])

# Drop subjourneys with small N ----
obs[,
    subjrn_duration_mins := as.numeric(max(tst) - min(tst), units = 'mins'),
    by = .(jrnid, subjrn)]
obs[subjrn_duration_mins < MIN_SUBJRN_DURATION_MINS, drop_row := TRUE]
logger('Short subjourneys dropped',
       unique(obs[drop_row == TRUE, .(jrnid, subjrn)])[, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]
obs <- obs[, -c('subjrn_duration_mins')]

# Odo and GPS distances ----
obs[,
    delta_odo_prev_m := odo - shift(odo, type = 'lag'),
    by = .(jrnid, subjrn)]
obs[is.na(delta_odo_prev_m), delta_odo_prev_m := 0]
obs[delta_odo_prev_m < 0, drop_row := TRUE]
logger('Odo difference to prev point < 0',
       obs[drop_row == TRUE, .(n_rows = .N), by = jrnid])
obs <- obs[drop_row == FALSE, ]

obs[,
    delta_gps_prev_m := sqrt(
      (X - shift(X, type = 'lag'))**2 + (Y - shift(Y, type = 'lag'))**2
    ),
    by = .(jrnid, subjrn)]
obs[is.na(delta_gps_prev_m), delta_gps_prev_m := 0.0]

obs[, odo_distance := cumsum(delta_odo_prev_m), by = .(jrnid, subjrn)]
obs[, gps_distance := cumsum(delta_gps_prev_m), by = .(jrnid, subjrn)]

get_slope <- function(df) {
  res <- lm(gps_distance ~ odo_distance, df)
  coeff <- summary(res)$coefficients
  return(coeff[2, 1])
}
gps_odo_slopes <- obs[, .(slope = get_slope(.SD)), by = .(jrnid, subjrn)]
gps_odo_slopes[, outof_lims := FALSE]
gps_odo_slopes[!(slope %between% ACCEPT_GPS_VS_ODO_SLOPE_RANGE),
               outof_lims := TRUE]
logger('Subjourney GPS vs ODO slope not acceptable',
       gps_odo_slopes[outof_lims == TRUE, .(n_rows = .N), by = jrnid])
obs <- merge(obs, gps_odo_slopes[, .(jrnid, subjrn, outof_lims)],
             by = c('jrnid', 'subjrn'), all.x = TRUE)
obs[, outof_lims := NULL]
obs[, gps_distance := NULL]
obs[, delta_gps_prev_m := NULL]
obs[, delta_odo_prev_m := NULL]

# Stop detection ----
#'
#' We detect halts ("stops" are used for transit stops...) by cumulative odometer
#' values by subjourneys. The data was therefore cleaned up above such that
#' there cannot be negative odometer value differences, for example.
#' So, as long as the odometer value remains unchanged, we assume the vehicle
#' is stopped, with the exception that very short stops (< 5 s) are not taken
#' into account. The odometer tends to advance in 4-5 m steps so without this
#' constraint we would get an unrealistic set of very short stops.
#'
#' Once the halt "groups" have been defined, we set X and Y coordinates of their
#' points to the median values by group to eliminate the GPS jitter.
obs[, halt_grp := .GRP, by = .(jrnid, subjrn, odo_distance)]
obs[, halt_duration_s := as.numeric(max(tst) - min(tst)), by = halt_grp]
obs[halt_duration_s < MIN_HALT_DURATION_SECS, halt_grp := NA]

obs[!is.na(halt_grp),
    `:=`(median_X = median(X),
         median_Y = median(Y)),
    by = halt_grp]

logger('N of stops per journey',
       unique(obs[!is.na(halt_grp), .(jrnid, halt_grp)])[, .(n_rows = .N), by = jrnid])
obs[!is.na(halt_grp), `:=`(X = median_X, Y = median_Y)]

# Door status & redundant points ----
#' We can get rid of a potentially high number of redundant points where
#' nothing changes, i.e. the vehicle is stopped, and we only leave the change points,
#' meaning the first and last point of each halt as well as points where doors
#' are opened or closed (drst changes).
#' To distinguish these cases from actual data gaps, we write down the number of
#' redundant points deleted that the last point before them "represents".
#'
#' Note that when we compare drst to the previous drst, NA != NA would return NA.
#' Therefore we map boolean drst to 0 = FALSE, 1 = TRUE and -1 = NA.
#' It is important to know whether the door status works at all.
obs[, drst_mapped := as.numeric(drst)]
obs[is.na(drst_mapped), drst_mapped := -1]
obs[,
    drst_changed := as.numeric(drst_mapped != shift(drst_mapped, fill = -1, type = 'lag')),
    by = .(jrnid, subjrn)]
logger('N of door status changes',
       obs[drst_changed == 1, .(n_rows = .N), by = jrnid])
obs[, halt_drst_grp := cumsum(drst_changed) + halt_grp, by = .(jrnid, subjrn)]
obs[!is.na(halt_drst_grp),
    `:=`(first_tst = min(tst), last_tst = max(tst), n_points = .N),
    by = .(halt_drst_grp)]

obs[!is.na(halt_drst_grp) & tst != first_tst & tst != last_tst,
    drop_row := TRUE]
logger('N of redundant points deleted',
       obs[drop_row == TRUE,
           .(n_rows = .N),
           by = jrnid])
obs <- obs[drop_row == FALSE, ]

#' By default, each point represents itself (1 point).
#' If redundant ones have been removed, then the point before them is
#' representative of them. - 1 is because the last point of such a group
#' is preserved and represents itself.
obs[, represents_n_points := 1]
obs[!is.na(halt_drst_grp) & tst == first_tst & tst != last_tst,
    represents_n_points := n_points - 1]

#' Finally, calculate also the time that each point represents,
#' before the next point. Of course this information is contained by and can be
#' derived from the ordered data, but pre-calculating this can make various
#' metrics calculations easier, as window functions need not be called
#' again and again.
obs[,
    represents_time_s := as.numeric(shift(tst, type = 'lead') - tst, units = 'secs'),
    by = .(jrnid, subjrn)]

# Select cols & write out ----
#' jrn table may still have journeys that do not have corresponding obs records
#' anymore due to filtering.
jrn <- jrn[jrnid %in% obs$jrnid, ]
jrn <- jrn[, .(jrnid, route, dir, start_tst, oper, veh)]
fwrite(jrn, file = OUTPUT_JRN_PATH)

obs <- obs[, .(jrnid, tst, odo, drst, represents_n_points, represents_time_s, X, Y)]
fwrite(obs, file = OUTPUT_OBS_PATH)
logger('End of script', data.table(jrnid = NA_character_, n_rows = NA_integer_))

fwrite(logdt, file = OUTPUT_LOG_PATH)
