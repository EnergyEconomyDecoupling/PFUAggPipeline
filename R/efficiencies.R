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

  rm(.eta_pfu_data)
  gc()

  filtered_data %>%
    dplyr::mutate(
      "{eta_pf}" := .data[[ex_f]] / .data[[ex_p]],
      "{eta_fu}" := .data[[ex_u]] / .data[[ex_f]],
      "{eta_pu}" := .data[[ex_u]] / .data[[ex_p]]
    )
}


#' Calculate PFU aggregations and efficiencies
#'
#' @param .psut_data A data frame of PSUT data or a slice (row) of the data frame.
#' @param p_industries A string vector of primary industries.
#' @param fd_sectors A string vector of final demand sectors.
#' @param piece The piece of the labels used for matching.
#'              Default is "noun".
#' @param notation The notation used for row and column labels.
#'                 Default is `list(RCLabels::bracket_notation, RCLabels::arrow_notation)`.
#' @param pattern_type The pattern type to be used for row and column matching.
#'                     Default is "exact".
#' @param prepositions A list of prepositions for row and column labels.
#'                     Default is `RCLabels::prepositions_list`.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units,country,year,last_stage See `Recca::psut_cols`.
#' @param gross,net,gross_net See `Recca::efficiency_cols`.
#' @param final,useful See `IEATools::all_stages`.
#' @param ex_p,ex_fd_gross,ex_fd_net,ex_fd See `Recca::aggregate_cols`.
#' @param ex_f,ex_u See `IEATools::aggregate_cols`.
#' @param eta_pf,eta_fu,eta_pu See `Recca::efficiency_cols`.
#'
#' @return A data frame of metadata columns and efficiencies
#'
#' @export
efficiency_pipeline <- function(.psut_data,
                                p_industries,
                                fd_sectors,
                                piece = "noun",
                                notation = list(RCLabels::bracket_notation,
                                                RCLabels::arrow_notation),
                                pattern_type = "exact",
                                prepositions = RCLabels::prepositions_list,
                                # Names of original matrices in .psut_data
                                R = Recca::psut_cols$R,
                                U = Recca::psut_cols$U,
                                U_feed = Recca::psut_cols$U_feed,
                                U_eiou = Recca::psut_cols$U_eiou,
                                r_eiou = Recca::psut_cols$r_eiou,
                                V = Recca::psut_cols$V,
                                Y = Recca::psut_cols$Y,
                                S_units = Recca::psut_cols$S_units,
                                # Country and year columns
                                country = Recca::psut_cols$country,
                                year = Recca::psut_cols$year,
                                # Key names
                                gross = Recca::efficiency_cols$gross,
                                net = Recca::efficiency_cols$net,
                                gross_net = Recca::efficiency_cols$gross_net,
                                last_stage = Recca::psut_cols$last_stage,
                                final = IEATools::all_stages$final,
                                useful = IEATools::all_stages$useful,
                                ex_p = Recca::aggregate_cols$aggregate_primary,
                                ex_f = IEATools::aggregate_cols$aggregate_final,
                                ex_u = IEATools::aggregate_cols$aggregate_useful,
                                ex_fd_gross = Recca::aggregate_cols$gross_aggregate_demand,
                                ex_fd_net = Recca::aggregate_cols$net_aggregate_demand,
                                ex_fd = Recca::aggregate_cols$aggregate_demand,
                                eta_pf = Recca::efficiency_cols$eta_pf,
                                eta_fu = Recca::efficiency_cols$eta_fu,
                                eta_pu = Recca::efficiency_cols$eta_pu) {

  # filtered_data <- .psut_data |>
  #   dplyr::filter(.data[[country]] %in% countries, .data[[year]] %in% years)
  # rm(.psut_data)
  # gc()

  if (nrow(.psut_data) == 0) {
    return(NULL)
  }

  # Calculate primary aggregates
  PSUT_Chop_all_Ds_all_Gr_all_St_p <- .psut_data |>
    Recca::primary_aggregates(p_industries = p_industries,
                              piece = piece,
                              notation = notation,
                              pattern_type = pattern_type,
                              prepositions = prepositions)

  # Calculate final demand aggregates

  PSUT_Chop_all_Ds_all_Gr_all_St_fd <- PSUT_Chop_all_Ds_all_Gr_all_St_p |>
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors,
                                  piece = piece,
                                  notation = notation,
                                  pattern_type = pattern_type,
                                  prepositions = prepositions)

    # Stack the data frames
    PFU_aggregates <- PSUT_Chop_all_Ds_all_Gr_all_St_fd |>
      dplyr::mutate(
        "{R}" := NULL,
        "{U}" := NULL,
        "{U_feed}" := NULL,
        "{U_eiou}" := NULL,
        "{r_eiou}" := NULL,
        "{V}" := NULL,
        "{Y}" := NULL,
        "{S_units}" := NULL
      ) |>
      # Pivot to gross and net final demand energy stage
      dplyr::rename(
        "{gross}" := ex_fd_gross,
        "{net}" := ex_fd_net
      ) |>
      tidyr::pivot_longer(cols = c(gross, net), names_to = gross_net, values_to = ex_fd) %>%
      dplyr::mutate(
        "{ex_p}" := as.numeric(.data[[ex_p]]),
        "{ex_fd}" := as.numeric(.data[[ex_fd]])
      ) |>
      tidyr::pivot_wider(names_from = last_stage, values_from = ex_fd) %>%
      dplyr::rename(
        "{ex_f}" := .data[[final]],
        "{ex_u}" := .data[[useful]]
      )

    # Calculate efficiencies and return
    PFU_aggregates |>
      dplyr::mutate(
        "{eta_pf}" := .data[[ex_f]] / .data[[ex_p]],
        "{eta_fu}" := .data[[ex_u]] / .data[[ex_f]],
        "{eta_pu}" := .data[[ex_u]] / .data[[ex_p]]
      )
}
