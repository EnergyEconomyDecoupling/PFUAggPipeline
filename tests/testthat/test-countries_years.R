test_that("filter_countries_and_years() works as expected", {
  psut_data <- tibble::tribble(~Country, ~Year,
                               "ZAF", 1971,
                               "ZAF", 1972,
                               "ZAF", 1973,
                               "USA", 1971,
                               "USA", 1972,
                               "USA", 1973)
  expect_equal(filter_countries_and_years(psut_data, countries = "all", years = "all"),
               psut_data)

  expect_equal(filter_countries_and_years(psut_data, countries = "all", years = 1972),
               psut_data %>% dplyr::filter(Year == 1972))
  expect_equal(filter_countries_and_years(psut_data, countries = "all", years = c(1971:1972)),
               psut_data %>% dplyr::filter(Year %in% 1971:1972))


  expect_equal(filter_countries_and_years(psut_data, "ZAF", 1971),
               tibble::tribble(~Country, ~Year,
                               "ZAF", 1971))

  expect_equal(filter_countries_and_years(psut_data, c("ZAF", "USA"), 1971:1972),
               psut_data %>% dplyr::filter(Year %in% 1971:1972))
  expect_equal(filter_countries_and_years(psut_data, c("USA"), 1971:1972),
               psut_data %>% dplyr::filter(Country == "USA", Year %in% 1971:1972))
})
