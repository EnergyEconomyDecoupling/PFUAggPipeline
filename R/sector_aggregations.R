#' Compile the list of final demand sectors
#'
#' The `IEATools` package contains a list of final demand sectors
#' in `IEATools::fd_sectors`.
#' However, we aggregate and rename sectors according to an aggregation map.
#' The names of the aggregation map items should also count as
#' final demand sectors.
#' This function adds the aggregated final demand sector names
#' to the `IEATools` final demand sectors.
#'
#' @param iea_fd_sectors The list of final demand sectors, according to the IEA.
#' @param sector_aggregation_map The aggregation map used to aggregate final demand sectors.
#'
#' @return A larger list of final demand sectors comprised of
#'         the items in `iea_fd_sectors` and the names of `sector_aggregation_map`.
#'
#' @export
create_fd_sectors_list <- function(iea_fd_sectors, sector_aggregation_map) {
  append(iea_fd_sectors, names(sector_aggregation_map))
}


#' Calculate final and useful sector sums and efficiencies
#'
#' This function calculates final demand aggregates by sector
#' for both final and useful stages and resulting efficiencies.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param fd_sectors The sectors that count for final demand.
#' @param piece The piece to be aggregated. Default is "noun".
#' @param pattern_type Where to look for sectors.
#'                     Default is "leading".
#' @param notation The assumed notation for the labels.
#'                 Default is `list(RCLabels::bracket_notation, RCLabels::arrow_notation)`.
#' @param prepositions The expected propositions in row and column labels.
#'                     Default is `RCLabels::prepositions_list`.
#' @param R,U,U_feed,U_eiou,r_eiou,V,Y,S_units The names of the matrix columns in `.psut_mats`.
#'                                             Defaults from `Recca::psut_cols`.
#' @param year The string name of the year column.
#'             Default is `Recca::psut_cols$year`.
#' @param last_stage The string name of the last stage column.
#'                   Default is `Recca::psut_cols$last_stage`.
#' @param aggregate_demand,gross_aggregate_demand,net_aggregate_demand The names of columns of aggregate demand.
#'                                                                     Defaults from `Recca::aggregate_cols`.
#' @param gross_net The name of the column that tells whether aggregates are gross or net.
#'                  Default is `Recca::efficiency_cols$gross_net`.
#' @param gross,net Strings identifying gross or net quantities.
#'                  Defaults from `Recca::efficiency_cols`.
#' @param sector The name of the sector column.
#'               Default is "Sector".
#' @param colnames,rowtypes,coltypes Names of columns deleted after expanding matrices.
#'                                   Defaults are "colnames", "rowtypes", and "coltypes", respectively.
#' @param final,useful The string names for final and useful aggregates.
#'                     Defaults from `IEATools::all_stages`.
#' @param eta_fu The string name of the final-to-useful efficiency.
#'               Default is `Recca::efficiency_cols$eta_fu`.
#'
#' @return A data frame of sector energy and exergy sums.
#'
#' @export
calculate_sector_agg_eta_fu <- function(.psut_data,
                                        countries,
                                        years,
                                        fd_sectors,
                                        piece = "all",
                                        pattern_type = "exact",
                                        notation = RCLabels::notations_list,
                                        prepositions = RCLabels::prepositions_list,
                                        R = Recca::psut_cols$R,
                                        U = Recca::psut_cols$U,
                                        U_feed = Recca::psut_cols$U_feed,
                                        U_eiou = Recca::psut_cols$U_eiou,
                                        r_eiou = Recca::psut_cols$r_eiou,
                                        V = Recca::psut_cols$V,
                                        Y = Recca::psut_cols$Y,
                                        S_units = Recca::psut_cols$S_units,
                                        year = Recca::psut_cols$year,
                                        last_stage = Recca::psut_cols$last_stage,
                                        gross_aggregate_demand = Recca::aggregate_cols$gross_aggregate_demand,
                                        net_aggregate_demand = Recca::aggregate_cols$net_aggregate_demand,
                                        aggregate_demand = Recca::aggregate_cols$aggregate_demand,
                                        gross_net = Recca::efficiency_cols$gross_net,
                                        gross = Recca::efficiency_cols$gross,
                                        net = Recca::efficiency_cols$net,
                                        sector = "Sector",
                                        colnames = "colnames",
                                        rowtypes = "rowtypes",
                                        coltypes = "coltypes",
                                        final = IEATools::all_stages$final,
                                        useful = IEATools::all_stages$useful,
                                        eta_fu = Recca::efficiency_cols$eta_fu) {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  rm(.psut_data)
  gc()

  filtered_data %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors,
                                  piece = piece,
                                  notation = notation,
                                  pattern_type = pattern_type,
                                  prepositions = prepositions,
                                  U = U, U_feed = U_feed, Y = Y,
                                  by = "Sector",
                                  net_aggregate_demand = net_aggregate_demand,
                                  gross_aggregate_demand = gross_aggregate_demand) %>%
    # Eliminate matrix columns
    dplyr::mutate(
      "{R}" := NULL,
      "{U}" := NULL,
      "{U_feed}" := NULL,
      "{U_eiou}" := NULL,
      "{r_eiou}" := NULL,
      "{V}" := NULL,
      "{Y}" := NULL,
      "{S_units}" := NULL
    ) %>%
    tidyr::pivot_longer(cols = c(net_aggregate_demand, gross_aggregate_demand), names_to = gross_net, values_to = aggregate_demand) %>%
    dplyr::mutate(
      "{gross_net}" := dplyr::case_when(
        .data[[gross_net]] == gross_aggregate_demand ~ gross,
        .data[[gross_net]] == net_aggregate_demand ~ net,
        TRUE ~ NA_character_
      )
    ) %>%
    # Expand matrices
    matsindf::expand_to_tidy(matnames = net_aggregate_demand, matvals = aggregate_demand, rownames = sector) %>%
    dplyr::mutate(
      "{colnames}" := NULL,
      "{rowtypes}" := NULL,
      "{coltypes}" := NULL
    ) %>%
    tidyr::pivot_wider(names_from = last_stage, values_from = aggregate_demand) %>%
    dplyr::mutate(
      "{eta_fu}" := .data[[useful]] / .data[[final]]
    ) # %>%
    # tidyr::pivot_longer(cols = c(final, useful, eta_fu), names_to = "var", values_to = "val") %>%
    # tidyr::pivot_wider(names_from = year, values_from = "val")
}
