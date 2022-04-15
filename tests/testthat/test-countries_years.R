test_that("filter_countries_and_years() works as expected", {
  psut_data <- tibble::tribble(~Country, ~Year,
                               "ZAF", 1971,
                               "ZAF", 1972,
                               "ZAF", 1973,
                               "USA", 1971,
                               "USA", 1972,
                               "USA", 1973)
  expect_equal(filter_countries_and_years(psut_data, "ZAF", 1971),
               tibble::tribble(~Country, ~Year,
                               "ZAF", 1971))
})
