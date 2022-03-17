#' Calculate efficiencies from an aggregates data frame
#'
#' @param .aggregates The data frame from which efficiencies are to be calculated.
#'                    This data frame should be the output of the
#'
#' @return
#'
#' @export
#'
#' @examples
calc_agg_etas <- function(.aggregates,
                          stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                          ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                          eta_pf_colname = PFUAggDatabase::efficiency_cols$eta_pf,
                          eta_fu_colname = PFUAggDatabase::efficiency_cols$eta_fu,
                          eta_pu_colname = PFUAggDatabase::efficiency_cols$eta_pu,
                          agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                          e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                          gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                          sector_colname = PFUAggDatabase::sea_cols$sector_colname,
                          total_value = PFUAggDatabase::agg_metadata$total_value,
                          primary = IEATools::all_stages$primary,
                          final = IEATools::all_stages$final,
                          useful = IEATools::all_stages$useful,
                          country = IEATools::iea_cols$country,
                          year = IEATools::iea_cols$year,
                          method = IEATools::iea_cols$method,
                          energy_type = IEATools::iea_cols$energy_type,
                          flow = IEATools::iea_cols$flow,
                          gross = PFUAggDatabase::gross_net_metadata$gross,
                          net = PFUAggDatabase::gross_net_metadata$net) {

  # Filter .aggregates to only Aggregation.by == "Total", because that's the only
  # way it makes sense to do aggregate efficiencies.


  # Duplicate the Primary stage information for both net and gross
  prim <- .aggregates %>%
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
  wide_finaluseful <- .aggregates %>%
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
#' @param tab The name of the tab in the Excel file.
#' @param pivot_wide If `TRUE`, the incoming data frame will be pivoted wider so that years are in columns.
#' @param primary,final,useful See `IEATools::all_stages`.
#' @param eta_pf_colname,eta_fu_colname,eta_pu_colname See `PFUAggDatabase::efficiency_cols`.
#' @param year A column of years. Default is `IEATools::iea_cols$year`.
#' @param quantity,.values See `IEATools::template_cols`.
#'
#' @return `TRUE` if the file was written successfully.
#'
#' @export
write_agg_etas_xlsx <- function(.agg_etas,
                                path,
                                tab,
                                pivot_wide = TRUE,
                                primary = IEATools::all_stages$primary,
                                final = IEATools::all_stages$final,
                                useful = IEATools::all_stages$useful,
                                eta_pf_colname = PFUAggDatabase::efficiency_cols$eta_pf,
                                eta_fu_colname = PFUAggDatabase::efficiency_cols$eta_fu,
                                eta_pu_colname = PFUAggDatabase::efficiency_cols$eta_pu,
                                year = IEATools::iea_cols$year,
                                quantity = IEATools::template_cols$quantity,
                                .values = IEATools::template_cols$.values) {
  if (pivot_wide) {
    .agg_etas <- .agg_etas %>%
      tidyr::pivot_longer(cols = c(primary, final, useful, eta_pf_colname, eta_fu_colname, eta_pu_colname),
                          names_to = quantity,
                          values_to = .values) %>%
      tidyr::pivot_wider(names_from = year, values_from = .values)
  }
  .agg_etas %>%
    writexl::write_xlsx(path = path)
  return(TRUE)
}
