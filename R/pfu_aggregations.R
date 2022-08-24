

#' Calculate primary aggregates for PSUT data
#'
#' This function routes to `Recca::primary_aggregates()`.
#'
#' @param .psut_data The data for which primary aggregates are to be calculated.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param p_industries A string vector of industries that count as "primary".
#' @param pattern_type The type of matching to be used for primary industry names.
#'                     Default is "leading".
#'
#' @return A version of `.psut_data` with additional column for primary aggregate data.
#'
#' @export
calculate_primary_aggregates <- function(.psut_data,
                                         countries,
                                         years,
                                         p_industries,
                                         pattern_type = "leading") {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(filtered_data)
  }
  filtered_data %>%
    Recca::primary_aggregates(p_industries = p_industries,
                              pattern_type = pattern_type)
}


#' Calculate final demand aggregates for PSUT data
#'
#' This function routes to `Recca::finaldemand_aggregates()`.
#'
#' @param .psut_data The data for which final demand aggregates are to be calculated.
#' @param countries The countries for which final demand aggregates are to be calculated.
#' @param years The years for which final demand aggregates are to be calculated.
#' @param fd_sectors A string vector of sectors that count as "final demand".
#' @param pattern_type The type of matching to be used for final demand sectors names.
#'                     Default is "leading".
#'
#' @return A version of `.psut_data` with additional column for final demand aggregate data.
#'
#' @export
calculate_finaldemand_aggregates <- function(.psut_data,
                                             countries,
                                             years,
                                             fd_sectors,
                                             pattern_type = "leading") {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(filtered_data)
  }
  filtered_data %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors,
                                  pattern_type = pattern_type)
}


#' Add a Gross.Net column and delete matrices
#'
#' Between calculating the gross and net final demand aggregates
#' and calculating efficiencies,
#' it is helpful to add a step wherein
#' delete the matrices (lots of data!) and add a `GrossNet` column.
#' This function performs those tasks.
#'
#' @param .agg_data Input data containing gross and net final demand aggregate columns.
#' @param countries The countries to work on.
#' @param years The years to work on.
#' @param gross_net The name of the column that identifies gross or net final demand aggregates.
#'                  Default is `Recca::efficiency_cols$gross_net`.
#' @param R,U,U_feed,U_eiou,r_eiou,V,Y,S_units Columns of matrices to be deleted.
#' @param gross,net Strings inserted into the `grossnet` column. See `Recca::efficiency_cols`.
#' @param ex_fd_gross,ex_fd_net,ex_fd Names for columns in `.agg_data` for gross and net final demand aggregations.
#'
#' @return A data frame with matrices removed and a new `GrossNet` column.
#'
#' @export
add_grossnet_column <- function(.agg_data,
                                countries,
                                years,
                                gross_net = Recca::efficiency_cols$gross_net,
                                R = "R",
                                U = "U",
                                U_feed = "U_feed",
                                U_eiou = "U_EIOU",
                                r_eiou = "r_EIOU",
                                V = "V",
                                Y = "Y",
                                S_units = "S_units",
                                gross = Recca::efficiency_cols$gross,
                                net = Recca::efficiency_cols$net,
                                ex_fd_gross = Recca::aggregate_cols$gross_aggregate_demand,
                                ex_fd_net = Recca::aggregate_cols$net_aggregate_demand,
                                ex_fd = Recca::aggregate_cols$aggregate_demand) {
  filtered_data <- .agg_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    dplyr::mutate(
      "{R}" := NULL,
      "{U}" := NULL,
      "{U_feed}" := NULL,
      "{U_eiou}" := NULL,
      "{r_eiou}" := NULL,
      "{V}" := NULL,
      "{Y}" := NULL,
      "{S_units}" := NULL
    )

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  filtered_data %>%
    # Pivot to gross and net final demand energy stage
    dplyr::rename(
      "{gross}" := ex_fd_gross,
      "{net}" := ex_fd_net
    ) %>%
    tidyr::pivot_longer(cols = c(gross, net), names_to = gross_net, values_to = ex_fd)
}

