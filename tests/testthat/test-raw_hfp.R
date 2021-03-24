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
