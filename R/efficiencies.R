#' Pivot data frame to calculate PFU efficiencies
#'
#' This function pivots the data frame produced by `add_grossnet_column()`
#' to obtain primary-final, final-useful, and primary-useful efficiencies.
#'
#' @param .eta_pfu_data A data frame produced by `add_grossnet_column()`.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param ex_p,ex_f,ex_u Names of primary, final, and useful energy and exergy aggregate columns (respectively).
#'                       Defaults are from `IEATools::aggregate_cols`.
#' @param eta_pf,eta_fu,eta_pu The names of efficiency columns: primary-to-final, final-to-useful, and primary-to-useful, respectively.
#'                             Defaults from `Recca::efficiency_cols`.
#'
#' @return A data frame with ECC stage efficiencies.
#'
#' @export
calculate_pfu_efficiencies <- function(.eta_pfu_data,
                                       countries,
                                       years,
                                       ex_p = IEATools::aggregate_cols$aggregate_primary,
                                       ex_f = IEATools::aggregate_cols$aggregate_final,
                                       ex_u = IEATools::aggregate_cols$aggregate_useful,
                                       eta_pf = Recca::efficiency_cols$eta_pf,
                                       eta_fu = Recca::efficiency_cols$eta_fu,
                                       eta_pu = Recca::efficiency_cols$eta_pu) {

  filtered_data <- .eta_pfu_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  filtered_data %>%
    dplyr::mutate(
      "{eta_pf}" := .data[[ex_f]] / .data[[ex_p]],
      "{eta_fu}" := .data[[ex_u]] / .data[[ex_f]],
      "{eta_pu}" := .data[[ex_u]] / .data[[ex_p]]
    )
}
