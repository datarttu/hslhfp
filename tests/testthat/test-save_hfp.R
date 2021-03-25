infile <- file.path('testdata', 'vp_test.csv.gz')
colnames <- raw_hfp_col_names()
colspec <- raw_hfp_col_spec()
df <- vroom::vroom(infile, col_names = colnames, col_types = colspec) %>%
  cast_datetime_cols()

test_that("Datetime columns are made into character columns", {
  df_chr <- datetimes_as_character(df)
  expect_type(df_chr$oday, 'character')
  expect_type(df_chr$received_at, 'character')
  expect_type(df_chr$tst, 'character')
})

test_that("Can write HFP data to a .csv.gz file", {
  out <- tempfile()
  on.exit(unlink(out))
  df_out <- df %>%
    dplyr::select(tst, oday, route) %>%
    dplyr::slice_head(n = 5)
  hfp_vroom_write(x = df_out, path = out)
  # Headers cause n of lines == 6
  expect_length(readLines(out), 6)
})
