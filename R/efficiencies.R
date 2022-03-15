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
                              primary = IEATools::all_stages$primary,
                              final = IEATools::all_stages$final,
                              useful = IEATools::all_stages$useful,
                              gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                              gross = PFUAggDatabase::gross_net_metadata$gross,
                              net = PFUAggDatabase::gross_net_metadata$net) {

  # Filter .aggregates to only Aggregation.by == "Total", because that's the only
  # way it makes sense to do aggregate efficiencies.


  # Duplicate the Primary stage information for both net and gross
  prim <- .aggregates %>%
    dplyr::filter(.data[[stage_colname]] == primary)
  prim_net <- prim %>%
    dplyr::mutate(
      "{gross_net_colname}" := net
    )
  prim_gross <- prim %>%
    dplyr::mutate(
      "{gross_net_colname}" := gross
    )
  .aggregates_with_gross_net <- .aggregates %>%
    # Get rid of existing primary data
    dplyr::filter(.data[[stage_colname]] != primary) %>%
    # Add gross and net primary data
    dplyr::bind_rows(prim_gross, prim_net)
  wide <- .aggregates_with_gross_net %>%
    tidyr::pivot_wider(names_from = stage_colname, values_from = ex_colname)
}
