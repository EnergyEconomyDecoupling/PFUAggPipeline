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


#' Bundle several aggregation calculations together
#'
#' The efficiency pipeline calculates efficiencies
#' for various despecifications, row aggregations, and column aggregations.
#'
#' This function is an attempt to streamline the calculation pipeline
#' by eliminating the need to repeatedly re-load intermediate targets from disk.
#' It bundles the work of previous targets to
#'
#' - despecify and aggregate both product and industry dimensions of PSUT matrices,
#' - group and aggregate products,
#' - group and aggregate industries,
#' - group and aggregate both products and industries,
#' - calculate primary-to-final efficiencies,
#' - calculate primary-to-useful efficiencies, and
#' - calculate final-to-useful efficiencies.
#'
#'
#' @param .psut_data PSUT matrices in wide-by-matrix format.
#'                   This could be an entire data frame,
#'                   a slice (row) of the data frame, or
#'                   a group of the data frame.
#' @param notation The notations from which notation for row and column names can be inferred.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#'                                             Default values are from `Recca::psut_cols`.
#' @param R_aggregated_colname,U_aggregated_colname,V_aggregated_colname,Y_aggregated_colname,r_eiou_aggregated_colname,U_eiou_aggregated_colname,U_feed_aggregated_colname,S_units_aggregated_colname The names of output aggregated columns.
#'                          Defaults are the matrix names with `aggregated_suffix` appended.
#' @param aggregated_suffix The suffix for columns of aggregated matrices.
#'                          Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A data frame of efficiencies for the original, despecified, and grouped versions
#'         of `.psut_data`.
#'
#' @export
efficiency_pipeline <- function(.psut_data,
                                product_agg_map,
                                p_industries,
                                pattern_type = "exact",
                                piece = "noun",
                                notation = list(RCLabels::bracket_notation,
                                                RCLabels::arrow_notation),
                                prepositions = RCLabels::prepositions_list,
                                # Row and column types
                                product_type = Recca::row_col_types$product_type,
                                industry_type = Recca::row_col_types$industry_type,
                                # Names of original matrices in .psut_data
                                R = Recca::psut_cols$R,
                                U = Recca::psut_cols$U,
                                V = Recca::psut_cols$V,
                                Y = Recca::psut_cols$Y,
                                r_eiou = Recca::psut_cols$r_eiou,
                                U_eiou = Recca::psut_cols$U_eiou,
                                U_feed = Recca::psut_cols$U_feed,
                                S_units = Recca::psut_cols$S_units,
                                # Names for the aggregated matrices after aggregation
                                R_aggregated_colname = paste0(R, aggregated_suffix),
                                U_aggregated_colname = paste0(U, aggregated_suffix),
                                V_aggregated_colname = paste0(V, aggregated_suffix),
                                Y_aggregated_colname = paste0(Y, aggregated_suffix),
                                r_eiou_aggregated_colname = paste0(r_eiou, aggregated_suffix),
                                U_eiou_aggregated_colname = paste0(U_eiou, aggregated_suffix),
                                U_feed_aggregated_colname = paste0(U_feed, aggregated_suffix),
                                S_units_aggregated_colname = paste0(S_units, aggregated_suffix),
                                # The suffix added to the name of the columns of aggregated matrices
                                aggregated_suffix = Recca::aggregate_cols$aggregated_suffix,
                                # Some identifying strings
                                product_aggregation = PFUAggDatabase::aggregation_df_cols$product_aggregation,
                                industry_aggregation = PFUAggDatabase::aggregation_df_cols$industry_aggregation,
                                specified = PFUAggDatabase::aggregation_df_cols$specified,
                                despecified = PFUAggDatabase::aggregation_df_cols$despecified,
                                grouped = PFUAggDatabase::aggregation_df_cols$grouped) {

  # Despecify and aggregate both Product and Industry dimensions
  PSUT_Ds_InPr <- .psut_data |>
    Recca::despecified_aggregates(notation = notation,
                                  R = R, U = U, V = V, Y = Y,
                                  r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed,
                                  S_units = S_units,
                                  R_aggregated_colname = R_aggregated_colname,
                                  U_aggregated_colname = U_aggregated_colname,
                                  V_aggregated_colname = V_aggregated_colname,
                                  Y_aggregated_colname = Y_aggregated_colname,
                                  r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                                  U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                                  U_feed_aggregated_colname = U_feed_aggregated_colname,
                                  S_units_aggregated_colname = S_units_aggregated_colname,
                                  aggregated_suffix = aggregated_suffix) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units)

  # Group on Product dimension
  PSUT_Ds_InPr_Gr_Pr <- PSUT_Ds_InPr |>
    Recca::grouped_aggregates(aggregation_map = product_agg_map, margin = product_type, pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units)

  # Group on Industry dimension
  PSUT_Ds_InPr_Gr_In <- PSUT_Ds_InPr |>
    Recca::grouped_aggregates(aggregation_map = product_agg_map, margin = industry_type, pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units)

  # Group on Product and Industry dimensions
  PSUT_Ds_InPr_Gr_PrIn <- PSUT_Ds_InPr |>
    Recca::grouped_aggregates(aggregation_map = product_agg_map, margin = c(product_type, industry_type), pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) |>
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units)

  # Stack all the despecifications and groupings together
  PSUT_Ds_all_Gr_all <- dplyr::bind_rows(.psut_data |>
                                           dplyr::mutate(
                                             "{product_aggregation}" := specified,
                                             "{industry_aggregation}" := specified
                                           ),
                                         PSUT_Ds_InPr |>
                                           dplyr::mutate(
                                             "{product_aggregation}" := despecified,
                                             "{industry_aggregation}" := despecified
                                           ),
                                         PSUT_Ds_InPr_Gr_Pr |>
                                           dplyr::mutate(
                                             "{product_aggregation}" := grouped,
                                             "{industry_aggregation}" := despecified
                                           ),
                                         PSUT_Ds_InPr_Gr_In |>
                                           dplyr::mutate(
                                             "{product_aggregation}" := despecified,
                                             "{industry_aggregation}" := grouped
                                           ),
                                         PSUT_Ds_InPr_Gr_PrIn |>
                                           dplyr::mutate(
                                             "{product_aggregation}" := grouped,
                                             "{industry_aggregation}" := grouped
                                           ))

  # Calculate primary aggregates
  PSUT_Ds_all_Gr_all_St_p <- PSUT_Ds_all_Gr_all |>
    Recca::primary_aggregates(p_industries = p_industries,
                              piece = piece,
                              notation = notation,
                              pattern_type = pattern_type,
                              prepositions = prepositions)

  # Calculate final demand aggregates



  # Calculate efficiencies


}
