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
