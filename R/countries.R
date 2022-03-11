#' Filter countries
#'
#' Chooses countries to use in the rest of the pipeline.
#'
#' @param .psut_data The entire PSUT data frame, read from the pinboard.
#' @param countries A vector of 3-letter country codes for the countries to be used in this analysis.
#'                  Default value is "all", meaning that all countries in `.psut_data` will be included.
#' @param country The name of the country column in `.psut_data`.
#'                Default is `IEATools::iea_cols$country`.
#'
#' @return A filtered version of `.psut_data`..
#'
#' @export
filter_countries <- function(.psut_data,
                             countries = "all",
                             country = IEATools::iea_cols$country) {
  if (length(countries) == 1) {
    if (countries == "all" ) {
      return(.psut_data)
    }
  }
  .psut_data %>%
    dplyr::filter(.data[[country]] %in% countries)
}
