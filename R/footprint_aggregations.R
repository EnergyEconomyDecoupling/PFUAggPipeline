#' Calculate footprint aggregates
#'
#' This function uses `Recca::footprint_aggregates()` internally
#' to calculate footprint aggregates.
#'
#' @param .psut_data A data frame of PSUT matrices. It should be wide by matrices.
#' @param countries The countries to analyze.
#' @param years The years to analyze.
#' @param p_industries Industries that count for primary energy aggregates.
#' @param fd_sectors Final demand sectors that count for final demand aggregates.
#'
#' @return A data frame of footprint aggregates.
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
