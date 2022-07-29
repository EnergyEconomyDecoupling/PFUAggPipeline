#' Calculate footprint aggregates
#'
#' This function uses `Recca::footprint_aggregates()` internally.
#'
#' @param .psut_data
#' @param countries
#' @param years
#' @param p_industries
#' @param fd_sectors
#'
#' @return
#'
#' @export
calculate_footprint_aggregations <- function(.psut_data,
                                             countries,
                                             years,
                                             p_industries,
                                             fd_sectors) {
  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::footprint_aggregates(p_industries = p_industries,
                                fd_sectors = fd_sectors,
                                pattern_type = "leading",
                                unnest = TRUE)
}
