test_that('44 col names as character vector', {
  x <- raw_hfp_col_names()
  expect_length(x, 44)
  expect_type(x, 'character')
})

test_that('44 default col types as col_spec', {
  x <- raw_hfp_col_spec()
  expect_s3_class(x, 'col_spec')
  expect_length(x$cols, 44)
})

test_that('Can read vp with default col names and spec', {
  f <- function() {
    infile <- file.path('testdata', 'vp_test.csv.gz')
    colnames <- raw_hfp_col_names()
    colspec <- raw_hfp_col_spec()
    df <- vroom::vroom(infile, col_names = colnames, col_types = colspec)
  }
  expect_silent(f())
})

test_that('Milliseconds are correctly cast to a timestamp', {
  x <- milliseconds_to_timestamp(1600657201019)
  expect_s3_class(x, 'POSIXct')
  expect_equal(format(x, '%Y-%m-%d %H:%M:%OS3'),
               '2020-09-21 03:00:01.019')
})

test_that('Milliseconds are correctly cast to a date', {
  x <- milliseconds_to_date(1600646400000)
  expect_s3_class(x, 'Date')
  expect_equal(format(x, '%Y-%m-%d'),
               '2020-09-21')
})

test_that('Cannot cast object that is already a date or timestamp', {
  x <- milliseconds_to_timestamp(1600646400000)
  expect_error(milliseconds_to_timestamp(x))
  x <- milliseconds_to_date(1600646400000)
  expect_error(milliseconds_to_date(x))
})

test_that('Cannot cast non-numeric/int object to date or timestamp', {
  x <- '1600646400000'
  expect_error(milliseconds_to_timestamp(x))
  expect_error(milliseconds_to_date(x))
})

infile <- file.path('testdata', 'vp_test.csv.gz')
colnames <- raw_hfp_col_names()
colspec <- raw_hfp_col_spec()
df <- vroom::vroom(infile, col_names = colnames, col_types = colspec)

test_that('Timestamp and date columns are cast correctly', {
  df <- cast_datetime_cols(df)
  expect_s3_class(df$received_at, 'POSIXct')
  expect_s3_class(df$tst, 'POSIXct')
  expect_s3_class(df$oday, 'Date')
})

test_that('Data frame silently unchanged if no datetime columns present', {
  df1 <- df %>%
    dplyr::select(-received_at, -tst, -oday)
  df2 <- cast_datetime_cols(df1)
  expect_equal(df1, df2)
})
