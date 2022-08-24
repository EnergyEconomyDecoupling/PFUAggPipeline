#' Calculate primary to final demand efficiency
#'
#' This function calculates primary to final demand efficiencies in a data frame of
#' primary and final demand aggregates (`.pfd_data`).
#' Columns of energy conversion chain matrices are removed.
#'
#' @param .pfd_data A data frame of primary and final demand aggregates.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param eta_pfd_net The name of the primary to net final demand efficiency column on output.
#' @param eta_pfd_gross The name of the primary to gross final demand efficiency column on output.
#' @param R,U,U_feed,U_eiou,r_eioul,V,Y,S_units Columns of matrices to be removed from .pdf_data.
#'
#' @return A data frame of primary-to-final-demand efficiencies, both net and gross.
#'
#' @export
calculate_pfd_efficiencies <- function(.agg_pfd_data,
                                       countries,
                                       years,
                                       EX_p = Recca::aggregate_cols$aggregate_primary,
                                       EX_fd = Recca::aggregate_cols$aggregate_demand,
                                       eta_pfd = "eta_pfd") {

  filtered_data <- .agg_pfd_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  filtered_data %>%
    dplyr::mutate(
      "{eta_pfd}" := as.numeric(.data[[EX_fd]]) / as.numeric(.data[[EX_p]])
    )
}


#' Pivot data frame to calculate PFU efficiencies
#'
#' This function pivots the data frame produced by `calculate_pfd_efficiencies()`
#' to obtain primary-final, final-useful, and primary-useful efficiencies.
#'
#' @param .eta_pfd_data A data frame produced by `calculate_pfd_efficiencies()`.
#'
#' @return A data frame with ECC stage efficiencies.
#'
#' @export
calculate_pfu_efficiencies <- function(.eta_pfd_data,
                                       countries,
                                       years,
                                       ex_p = Recca::aggregate_cols$aggregate_primary,
                                       ex_fd = Recca::aggregate_cols$aggregate_demand,
                                       last_stage = Recca::psut_cols$last_stage,
                                       eta_pf = Recca::efficiency_cols$eta_pf,
                                       eta_pu = Recca::efficiency_cols$eta_pu,
                                       eta_fu = Recca::efficiency_cols$eta_fu,
                                       eta_pfd = Recca::efficiency_cols$eta_pfd,
                                       final = "Final",
                                       useful = "Useful"

                                       ) {

  filtered_data <- .eta_pfd_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  filtered_data %>%
    # Eliminate the aggregate values, because they aren't unique.
    dplyr::mutate(
      "{ex_p}" := NULL,
      "{ex_fd}" := NULL
    ) %>%
    # Spread across stages
    tidyr::pivot_wider(names_from = last_stage, values_from = eta_pfd) %>%
    # Rename efficiency types
    dplyr::rename(
      "{eta_pf}" := final,
      "{eta_pu}" := useful
    ) %>%
    # Calculate eta_fu where possible
    dplyr::mutate(
      "{eta_fu}" := .data[[eta_pu]] / .data[[eta_pf]]
    ) %>%
    dplyr::relocate(.data[[eta_fu]], .before = .data[[eta_pu]])
}















#
# The functions below this point can probably be deleted after the efficiencies are working
# They are old, and I'm doing the efficiencies differently now.
# ---MKH, 21 Aug 2022
#



#' Calculate efficiencies from an aggregates data frame
#'
#' This function calculates efficiencies (etas) from the aggregates data frame.
#' Aggregate energy and exergy are retained.
#'
#' This function removes the `tar_group` column.
#'
#' @param .aggregates The data frame from which efficiencies are to be calculated.
#'                    This data frame should be the output of the
#'                    `PSUT_Re_all_St_pfu` or `PSUT_Re_all_St_pfu_by_country`
#'                    targets.
#' @param countries The countries for which primary energy and exergy data are to be calculated.
#' @param years The years for which primary energy and exergy data are to be calculated.
#' @param stage_colname,ex_colname,agg_by_colname,e_product_colname,gross_net_colname,sector_colname See `PFUAggDatabase::sea_cols`.
#' @param eta_pf_colname,eta_fu_colname,eta_pu_colname See `PFUAggDatabase::efficiency_cols`.
#' @param total_value See `PFUAggDatabase::agg_metadata`.
#' @param gross,net See `PFUAggDatabase::gross_net_metadata`
#' @param primary,final,useful,country,year,method,energy_type,flow See `IEATools::iea_cols`.
#'
#' @return A data frame of aggregates and aggregate efficiencies.
#'
#' @export
calc_agg_etas <- function(.aggregates,
                          countries,
                          years,
                          stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                          ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                          agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                          e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                          gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                          sector_colname = PFUAggDatabase::sea_cols$sector_colname,

                          eta_pf_colname = PFUAggDatabase::efficiency_cols$eta_pf,
                          eta_fu_colname = PFUAggDatabase::efficiency_cols$eta_fu,
                          eta_pu_colname = PFUAggDatabase::efficiency_cols$eta_pu,

                          gross = PFUAggDatabase::gross_net_metadata$gross,
                          net = PFUAggDatabase::gross_net_metadata$net,

                          total_value = PFUAggDatabase::agg_metadata$total_value,

                          primary = IEATools::all_stages$primary,
                          final = IEATools::all_stages$final,
                          useful = IEATools::all_stages$useful,
                          country = IEATools::iea_cols$country,
                          year = IEATools::iea_cols$year,
                          method = IEATools::iea_cols$method,
                          energy_type = IEATools::iea_cols$energy_type,
                          flow = IEATools::iea_cols$flow) {

  filtered_aggregates <- .aggregates %>%
    # Filter for desired countries and years.
    PFUDatabase::filter_countries_years(countries = countries, years = years,
                                        country = country, year = year)
  prim <- filtered_aggregates %>%
    # Filter to only Aggregation.by == "Total", because that's the only
    # way it makes sense to do aggregate efficiencies.
    # Duplicate the Primary stage information for both net and gross
    dplyr::filter(.data[[agg_by_colname]] == total_value,
                  .data[[stage_colname]] == primary)
  prim_net <- prim %>%
    dplyr::mutate(
      "{gross_net_colname}" := net
    )
  prim_gross <- prim %>%
    dplyr::mutate(
      "{gross_net_colname}" := gross
    )
  wide_primary <- dplyr::bind_rows(prim_gross, prim_net) %>%
    dplyr::mutate(
      # Eliminate unneeded columns
      "{e_product_colname}" := NULL,
      "{flow}" := NULL,
      "{agg_by_colname}" := NULL,
      "{sector_colname}" := NULL
    ) %>%
    # Put the primary energy into a column.
    tidyr::pivot_wider(names_from = stage_colname, values_from = ex_colname)

  # Build the final and useful data frame.
  wide_finaluseful <- filtered_aggregates %>%
    dplyr::filter(.data[[agg_by_colname]] == total_value,
                  .data[[stage_colname]] %in% c(final, useful)) %>%
    dplyr::mutate(
      # Eliminate unneeded columns
      "{e_product_colname}" := NULL,
      "{flow}" := NULL,
      "{agg_by_colname}" := NULL,
      "{sector_colname}" := NULL
    ) %>%
    # Put the final and useful energy into a column.
    tidyr::pivot_wider(names_from = stage_colname, values_from = ex_colname)

  # Join the data frames
  dplyr::full_join(wide_primary, wide_finaluseful,
                   by = c(country, year, method, energy_type, gross_net_colname)) %>%
    dplyr::mutate(
      "{eta_pf_colname}" := .data[[final]] / .data[[primary]],
      "{eta_fu_colname}" := .data[[useful]] / .data[[final]],
      "{eta_pu_colname}" := .data[[useful]] / .data[[primary]]
    )
}


#' Pivot and write aggregate efficiencies to an Excel file
#'
#' The incoming data frame is expected to contain
#' a `year` column as well as
#' `primary`, `final`, `useful`, `eta_pf`, `eta_fu`, and `eta_pu` columns.
#'
#' @param .agg_etas A data frame created by the `eta_Re_all_St_pfu` target.
#' @param path The path where the Excel file will be saved.
#' @param aggs_tabname,etas_tabname See `PFUAggDatabase::output_file_info`.
#' @param wide_by_year If `TRUE` (the default), the incoming data frame will be pivoted to be wide by years.
#'                     If `FALSE`, data will be unchanged.
#' @param primary,final,useful See `IEATools::all_stages`.
#' @param eta_pf_colname,eta_fu_colname,eta_pu_colname See `PFUAggDatabase::efficiency_cols`.
#' @param year,country See `IEATools::iea_cols`.
#' @param quantity,.values See `IEATools::template_cols`.
#'
#' @return `TRUE` if the file was written successfully.
#'
#' @export
write_agg_etas_xlsx <- function(.agg_etas,
                                path,
                                aggs_tabname = PFUAggDatabase::output_file_info$agg_tabname,
                                etas_tabname = PFUAggDatabase::output_file_info$eta_tabname,
                                wide_by_year = TRUE,
                                primary = IEATools::all_stages$primary,
                                final = IEATools::all_stages$final,
                                useful = IEATools::all_stages$useful,
                                eta_pf_colname = PFUAggDatabase::efficiency_cols$eta_pf,
                                eta_fu_colname = PFUAggDatabase::efficiency_cols$eta_fu,
                                eta_pu_colname = PFUAggDatabase::efficiency_cols$eta_pu,
                                year = IEATools::iea_cols$year,
                                country = IEATools::iea_cols$country,
                                quantity = IEATools::template_cols$quantity,
                                .values = IEATools::template_cols$.values) {
  agg_df <- .agg_etas %>%
    dplyr::mutate(
      "{eta_pf_colname}" := NULL,
      "{eta_fu_colname}" := NULL,
      "{eta_pu_colname}" := NULL
    )
  eta_df <- .agg_etas %>%
    dplyr::mutate(
      "{primary}" := NULL,
      "{final}" := NULL,
      "{useful}" := NULL
    )
  if (wide_by_year) {
    agg_df <- agg_df %>%
      tidyr::pivot_longer(cols = c(primary, final, useful),
                          names_to = quantity,
                          values_to = .values) %>%
      dplyr::arrange(.data[[year]]) %>%
      tidyr::pivot_wider(names_from = year, values_from = .values) %>%
      dplyr::arrange(.data[[country]])
    eta_df <- eta_df %>%
      tidyr::pivot_longer(cols = c(eta_pf_colname, eta_fu_colname, eta_pu_colname),
                          names_to = quantity,
                          values_to = .values) %>%
      dplyr::arrange(.data[[year]]) %>%
      tidyr::pivot_wider(names_from = year, values_from = .values) %>%
      dplyr::arrange(.data[[country]])
  }

  writexl::write_xlsx(list(agg_df, eta_df) %>%
                        magrittr::set_names(c(aggs_tabname, etas_tabname)),
                      path = path)
  return(TRUE)
}
