

#' Calculate primary aggregates for PSUT data
#'
#' This function routes to `Recca::primary_aggregates()`.
#'
#' @param .psut_data The data for which primary aggregates are to be calculated.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param p_industries A string vector of industries that count as "primary".
#' @param piece The piece to be aggregated. Default is "noun".
#' @param notation The assumed notation for the labels.
#'                 Default is `list(RCLabels::bracket_notation, RCLabels::arrow_notation)`.
#' @param pattern_type The type of matching to be used for primary industry names.
#'                     Default is "leading".
#' @param prepositions The expected propositions in row and column labels.
#'                     Default is `RCLabels::prepositions_list`.
#'
#' @return A version of `.psut_data` with additional column for primary aggregate data.
#'
#' @export
calculate_primary_aggregates <- function(.psut_data,
                                         countries,
                                         years,
                                         p_industries,
                                         piece = "noun",
                                         notation = list(RCLabels::bracket_notation,
                                                         RCLabels::arrow_notation),
                                         pattern_type = "exact",
                                         prepositions = RCLabels::prepositions_list) {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  rm(.psut_data)
  gc()

  if (nrow(filtered_data) == 0) {
    # return(filtered_data)
    return(NULL)
  }
  filtered_data %>%
    Recca::primary_aggregates(p_industries = p_industries,
                              piece = piece,
                              notation = notation,
                              pattern_type = pattern_type,
                              prepositions = prepositions)
}


#' Calculate final demand aggregates for PSUT data
#'
#' This function routes to `Recca::finaldemand_aggregates()`.
#'
#' @param .psut_data The data for which final demand aggregates are to be calculated.
#' @param countries The countries for which final demand aggregates are to be calculated.
#' @param years The years for which final demand aggregates are to be calculated.
#' @param fd_sectors A string vector of sectors that count as "final demand".
#' @param piece The piece to be aggregated. Default is "noun".
#' @param notation The assumed notation for the labels.
#'                 Default is `list(RCLabels::bracket_notation, RCLabels::arrow_notation)`.
#' @param pattern_type The type of matching to be used for final demand sectors names.
#'                     Default is "leading".
#' @param prepositions The expected propositions in row and column labels.
#'                     Default is `RCLabels::prepositions_list`.
#'
#' @return A version of `.psut_data` with additional column for final demand aggregate data.
#'
#' @export
calculate_finaldemand_aggregates <- function(.psut_data,
                                             countries,
                                             years,
                                             fd_sectors,
                                             piece = "noun",
                                             notation = list(RCLabels::bracket_notation,
                                                             RCLabels::arrow_notation),
                                             pattern_type = "exact",
                                             prepositions = RCLabels::prepositions_list) {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    # return(filtered_data)
    return(NULL)
  }

  rm(.psut_data)
  gc()

  filtered_data %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors,
                                  piece = piece,
                                  notation = notation,
                                  pattern_type = pattern_type,
                                  prepositions = prepositions)
}


#' Calculate primary, final, and useful aggregates
#'
#' In addition to calculating primary, final, and useful aggregates,
#' this function removes matrix columns and adds a `GrossNet` column to the data frame.
#'
#' @param .agg_data Input data containing gross and net final demand aggregate columns.
#' @param countries The countries to work on.
#' @param years The years to work on.
#' @param gross_net The name of the column that identifies gross or net final demand aggregates.
#'                  Default is `Recca::efficiency_cols$gross_net`.
#' @param R,U,U_feed,U_eiou,r_eiou,V,Y,S_units Columns of matrices to be deleted.
#' @param gross,net Strings inserted into the `grossnet` column. See `Recca::efficiency_cols`.
#' @param last_stage The string name of a column containing last stages of energy conversion chains.
#'                   Default is `Recca::psut_cols$last_stage`.
#' @param final,useful String names of last stages. Defaults from `IEATools::all_stages`.
#' @param ex_p,ex_f,ex_u Names for columns in `.agg_data` for primary, final, and useful aggregations.
#' @param ex_fd,ex_fd_gross,ex_fd_net Names for columns of total, gross, and net final demand aggregates.
#'                                    Defaults from `Recca::aggregate_cols`.
#'
#' @return A data frame with matrices removed and a new `GrossNet` column.
#'
#' @export
calculate_pfu_aggregates <- function(.agg_data,
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
                                     last_stage = Recca::psut_cols$last_stage,
                                     final = IEATools::all_stages$final,
                                     useful = IEATools::all_stages$useful,
                                     ex_p = Recca::aggregate_cols$aggregate_primary,
                                     ex_f = IEATools::aggregate_cols$aggregate_final,
                                     ex_u = IEATools::aggregate_cols$aggregate_useful,
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

  rm(.agg_data)
  gc()

  filtered_data %>%
    # Pivot to gross and net final demand energy stage
    dplyr::rename(
      "{gross}" := ex_fd_gross,
      "{net}" := ex_fd_net
    ) %>%
    tidyr::pivot_longer(cols = c(gross, net), names_to = gross_net, values_to = ex_fd) %>%
    dplyr::mutate(
      "{ex_p}" := as.numeric(.data[[ex_p]]),
      "{ex_fd}" := as.numeric(.data[[ex_fd]])
    ) %>%
    tidyr::pivot_wider(names_from = last_stage, values_from = ex_fd) %>%
    dplyr::rename(
      "{ex_f}" := .data[[final]],
      "{ex_u}" := .data[[useful]]
    )
}

