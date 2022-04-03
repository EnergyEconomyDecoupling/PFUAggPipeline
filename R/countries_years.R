#' Filter countries
#'
#' Chooses countries to use in the rest of the pipeline.
#'
#' @param .psut_data The entire PSUT data frame, read from the pinboard.
#' @param countries A vector of 3-letter country codes for the countries to be used in this analysis.
#'                  Default value is "all", meaning that all countries in `.psut_data` will be included.
#' @param years A numeric vector of years to be used in this analysis.
#'              Default value is "all", meaning that ll years in `.psut_dat` will be included.
#' @param country,year See `IEATools::iea_cols`.
#'
#'
#' @return A filtered version of `.psut_data`.
#'
#' @export
filter_countries_and_years <- function(.psut_data,
                             countries = "all",
                             years = "all",
                             country = IEATools::iea_cols$country,
                             year = IEATools::iea_cols$year) {
  if (length(countries) == 1 & countries == "all") {
    out <- .psut_data
  } else {
    out <- .psut_data %>%
      dplyr::filter(.data[[country]] %in% countries)
  }
  if (length(years) == 1 & years == "all") {
    out <- out
  } else {
    out <- out %>%
      dplyr::filter(.data[[year]] %in% years)
  }
  out
}
