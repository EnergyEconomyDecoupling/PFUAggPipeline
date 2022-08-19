

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
    return(.psut_data)
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
#' @param fd_industries A string vector of sectors that count as "final demand".
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
    return(.psut_data)
  }
  filtered_data %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors,
                                  pattern_type = pattern_type)
}




