test_that("D must be scalar", {
  expect_error(download_blobs_by_date(d = c('2020-01-01', '2020-01-02')))
})

test_that("Type / format of d is checked", {
  expect_error(download_blobs_by_date(d = 12))
  expect_error(download_blobs_by_date(d = '2020-01'))
})

test_that("Existence of target directory is checked", {
  expect_error(download_blobs_by_date(d = '2020-01-01', target_dir = '/foobarbaz'))
})
