#' Calculate footprint aggregates
#'
#' This function uses `Recca::footprint_aggregates()` internally
#' to calculate footprint aggregates.
#'
#' Footprint aggregations involve a matrix inversion step.
#' The `method` argument specifies which method should be used for
#' calculating the inverse.
#' See `matsbyname::invert_byname()`.
#'
#' Both `tol` and `method` should be a single values and apply to all matrices being inverted.
#'
#' @param .psut_data A data frame of PSUT matrices. It should be wide by matrices.
#' @param countries The countries to analyze.
#' @param years The years to analyze.
#' @param p_industries Industries that count for primary energy aggregates.
#' @param fd_sectors Final demand sectors that count for final demand aggregates.
#' @param method Tells how to invert matrices. Default is "SVD". See details.
#' @param tol The tolerance for detecting linear dependencies in the columns of matrices to be inverted.
#'            Default is `.Machine$double.eps`.
#'
#' @return A data frame of footprint aggregates.
#'
#' @export
calculate_footprint_aggregations <- function(.psut_data,
                                             countries,
                                             years,
                                             p_industries,
                                             fd_sectors,
                                             method = "SVD",
                                             tol = .Machine$double.eps) {
  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::footprint_aggregates(p_industries = p_industries,
                                fd_sectors = fd_sectors,
                                pattern_type = "leading",
                                unnest = TRUE,
                                method = method,
                                tol_invert = tol)
}


#' Calculate effects aggregates
#'
#' This function uses `Recca::effects_aggregates()` internally
#' to calculate effects (downstream) aggregates of resource matrix energy carriers.
#'
#' Effects aggregations involve a matrix inversion step.
#' The `method` argument specifies which method should be used for
#' calculating the inverse.
#' See `matsbyname::invert_byname()`.
#'
#' Both `tol` and `method` should be a single values and apply to all matrices being inverted.
#'
#' @param .psut_data A data frame of PSUT matrices. It should be wide by matrices.
#' @param countries The countries to analyze.
#' @param years The years to analyze.
#' @param p_industries Industries that count for primary energy aggregates.
#' @param fd_sectors Final demand sectors that count for final demand aggregates.
#' @param method Tells how to invert matrices. Default is "SVD". See details.
#' @param tol The tolerance for detecting linear dependencies in the columns of matrices to be inverted.
#'            Default is `.Machine$double.eps`.
#'
#' @return A data frame of effects aggregates.
#'
#' @export
calculate_effects_aggregations <- function(.psut_data,
                                           countries,
                                           years,
                                           p_industries,
                                           fd_sectors,
                                           method = "SVD",
                                           tol_invert = .Machine$double.eps) {
  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::effects_aggregates(p_industries = p_industries,
                              fd_sectors = fd_sectors,
                              pattern_type = "leading",
                              unnest = TRUE,
                              method = method,
                              tol_invert = tol)
}
