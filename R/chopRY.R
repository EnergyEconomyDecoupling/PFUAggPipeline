#' Chop the ECC for **Y**
#'
#' This function uses `Recca::chop_Y()` internally
#' to calculate new ECCs for each row or column in the **Y** matrix.
#'
#' Chopping in the **Y** matrix and calculating a new ECC
#' involves a matrix inversion step.
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
#' @return A data frame of chopped final demand matrix ECCs.
#'
#' @export
chop_Y_eccs <- function(.psut_data,
                        countries,
                        years,
                        method = "SVD",
                        tol = .Machine$double.eps) {
  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)
  # Check for the case where we have no data for that country and year.
  # In that event, we simply want to return the data frame.
  if (nrow(filtered_data) == 0) {
    return(filtered_data)
  }
  filtered_data %>%
    Recca::chop_Y(calc_pfd_aggs = FALSE,
                  pattern_type = "leading",
                  unnest = TRUE,
                  method = method,
                  tol_invert = tol)
}


#' Chop the ECC for **R**
#'
#' This function uses `Recca::chop_R()` internally
#' to calculate new ECCs for each column in the **R** matrix.
#'
#' Chopping in the **R** matrix and calculating a new ECC
#' involves a matrix inversion step.
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
#' @return A data frame of chopped resource matrix ECCs.
#'
#' @export
chop_R_eccs <- function(.psut_data,
                        countries,
                        years,
                        method = "SVD",
                        tol_invert = .Machine$double.eps) {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(.psut_data)
  }
  filtered_data %>%
    Recca::chop_R(calc_pfd_aggs = FALSE,
                  pattern_type = "leading",
                  unnest = TRUE,
                  method = method,
                  tol_invert = tol)
}
