#' Calculate final demand sector sums
#'
#' This function calculates final demand aggregates by sector.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param fd_sectors The sectors that count for final demand.
#' @param pattern_type Where to look for sectors.
#'                     Default is "leading".
#' @param U The name of the use matrix column in `.psut_mats`.
#'          Default is `Recca::psut_cols$U`.
#' @param U_feed The name of the feed matrix column in `.psut_mats`.
#'               Default is `Recca::psut_cols$U_feed`.
#' @param Y The name of the final demand matrix column in `.psut_mats`.
#'          Default is `Recca::psut_cols$Y`.
#' @param net_aggregate_demand The name of the net aggregate demand column on output.
#'                             Default is `Recca::aggregate_cols$net_aggregate_demand`.
#' @param gross_aggregate_demand The name of the gross aggregate demand column on output.
#'                               Default is `Recca::aggregate_cols$gross_aggregate_demand`.
#'
#' @return A data frame of sector energy and exergy sums.
#'
#' @export
calculate_sector_aggregates <- function(.psut_data,
                                        countries,
                                        years,
                                        fd_sectors,
                                        pattern_type = "leading",
                                        U = Recca::psut_cols$U,
                                        U_feed = Recca::psut_cols$U_feed,
                                        Y = Recca::psut_cols$Y,
                                        net_aggregate_demand = Recca::aggregate_cols$net_aggregate_demand,
                                        gross_aggregate_demand = Recca::aggregate_cols$gross_aggregate_demand) {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    # return(filtered_data)
    return(NULL)
  }

  filtered_data %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors, pattern_type = pattern_type,
                                  U = U, U_feed = U_feed, Y = Y,
                                  net_aggregate_demand = net_aggregate_demand,
                                  gross_aggregate_demand = gross_aggregate_demand)
}
