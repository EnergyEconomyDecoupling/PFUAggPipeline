#' Despecify and aggregate all PSUT matrices
#'
#' This function uses `Recca::despecified_aggregates()` internally.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries to be analyzed.
#' @param years The years to be analyzed.
#' @param notation The notations from which notation for row and column names can be inferred.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#'                                             Default values are from `Recca::psut_cols`.
#' @param R_aggregated_colname,U_aggregated_colname,V_aggregated_colname,Y_aggregated_colname,r_eiou_aggregated_colname,U_eiou_aggregated_colname,U_feed_aggregated_colname,S_units_aggregated_colname The names of output aggregated columns.
#' @param aggregated_suffix The suffix for columns of aggregated matrices.
#'                          Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A data frame with despecified and aggregated matrices.
#'
#' @export
despecified_aggregations <- function(.psut_data,
                                     countries,
                                     years,
                                     notation,
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
                                     R_aggregated_colname = paste0(Recca::psut_cols$R, aggregated_suffix),
                                     U_aggregated_colname = paste0(Recca::psut_cols$U, aggregated_suffix),
                                     V_aggregated_colname = paste0(Recca::psut_cols$V, aggregated_suffix),
                                     Y_aggregated_colname = paste0(Recca::psut_cols$Y, aggregated_suffix),
                                     r_eiou_aggregated_colname = paste0(Recca::psut_cols$r_eiou, aggregated_suffix),
                                     U_eiou_aggregated_colname = paste0(Recca::psut_cols$U_eiou, aggregated_suffix),
                                     U_feed_aggregated_colname = paste0(Recca::psut_cols$U_feed, aggregated_suffix),
                                     S_units_aggregated_colname = paste0(Recca::psut_cols$S_units, aggregated_suffix),
                                     # The suffix added to the name of the columns of aggregated matrices.
                                     aggregated_suffix = Recca::aggregate_cols$aggregated_suffix) {

  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
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
                                  aggregated_suffix = aggregated_suffix) %>%
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units)
}


#' Aggregate PSUT matrices by row and column groups
#'
#' Aggregating to groups of rows or columns can be accomplished by an `aggregation_map`.
#' Internally, this function calls `Recca::grou_aggregates()`, so
#' arguments `aggregation_map`, `margin`, and `pattern_type` control its behavior.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries to be analyzed.
#' @param years The years to be analyzed.
#' @param aggregation_map A named list of rows or columns to be aggregated (or `NULL`).
#' @param margin `1`, `2`, or `c(1, 2)` for row aggregation, column aggregation, or both.
#'               As a string, `margin` can be a row or column type.
#'               Default is `c(1, 2)`.
#' @param pattern_type See `RCLabels::make_or_pattern()`.
#'                     Default is "exact".
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#'                                             Default values are taken from `Recca::psut_cols`.
#' @param R_aggregated_colname,U_aggregated_colname,V_aggregated_colname,Y_aggregated_colname,r_eiou_aggregated_colname,U_eiou_aggregated_colname,U_feed_aggregated_colname,S_units_aggregated_colname The names of output aggregated columns.
#' @param aggregated_suffix The suffix for columns of aggregated matrices.
#'                          Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A version of `.psut_data` with aggregated rows or columns.
#'
#' @export
grouped_aggregations <- function(.psut_data,
                                 countries,
                                 years,
                                 aggregation_map,
                                 margin = list(c(1, 2)),
                                 pattern_type = "exact",
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
                                 R_aggregated_colname = paste0(Recca::psut_cols$R, aggregated_suffix),
                                 U_aggregated_colname = paste0(Recca::psut_cols$U, aggregated_suffix),
                                 V_aggregated_colname = paste0(Recca::psut_cols$V, aggregated_suffix),
                                 Y_aggregated_colname = paste0(Recca::psut_cols$Y, aggregated_suffix),
                                 r_eiou_aggregated_colname = paste0(Recca::psut_cols$r_eiou, aggregated_suffix),
                                 U_eiou_aggregated_colname = paste0(Recca::psut_cols$U_eiou, aggregated_suffix),
                                 U_feed_aggregated_colname = paste0(Recca::psut_cols$U_feed, aggregated_suffix),
                                 S_units_aggregated_colname = paste0(Recca::psut_cols$S_units, aggregated_suffix),
                                 # The suffix added to the name of the columns of aggregated matrices.
                                 aggregated_suffix = Recca::aggregate_cols$aggregated_suffix) {
  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::grouped_aggregates(aggregation_map = aggregation_map, margin = margin, pattern_type = pattern_type,
                              R = R, U = U, V = V, Y = Y, r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                              R_aggregated_colname = R_aggregated_colname,
                              U_aggregated_colname = U_aggregated_colname,
                              V_aggregated_colname = V_aggregated_colname,
                              Y_aggregated_colname = Y_aggregated_colname,
                              r_eiou_aggregated_colname = r_eiou_aggregated_colname,
                              U_eiou_aggregated_colname = U_eiou_aggregated_colname,
                              U_feed_aggregated_colname = U_feed_aggregated_colname,
                              S_units_aggregated_colname = S_units_aggregated_colname) %>%
    rename_suffixed_psut_columns(suffix = aggregated_suffix,
                                 R = R, U = U, V = V, Y = Y,
                                 r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units)
}






#' Delete original and rename aggregated columns
#'
#' This is a helpful function that deletes original PSUT columns and
#' renames the aggregated columns to the original names.
#'
#' Columns with names given by `R`, `U`, `V`, `Y`, `r_eiou`, `U_eiou`, `U_feed`, and `S_units` arguments.
#' The suffix is given by the `suffix` argument.
#'
#' This function is a helper function and, thus, is not public.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#' @param suffix The suffix for columns of aggregated matrices.
#'               Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A version of `.psut_data` with original PSUT columns deleted and suffixed columns renamed to original names.
rename_suffixed_psut_columns <- function(.psut_data,
                                         suffix = Recca::aggregate_cols$aggregated_suffix,
                                         R = Recca::psut_cols$R,
                                         U = Recca::psut_cols$U,
                                         V = Recca::psut_cols$V,
                                         Y = Recca::psut_cols$Y,
                                         r_eiou = Recca::psut_cols$r_eiou,
                                         U_eiou = Recca::psut_cols$U_eiou,
                                         U_feed = Recca::psut_cols$U_feed,
                                         S_units = Recca::psut_cols$S_units) {
  .psut_data %>%
    # Delete the original columns
    dplyr::mutate(
      "{R}" := NULL,
      "{U}" := NULL,
      "{V}" := NULL,
      "{Y}" := NULL,
      "{r_eiou}" := NULL,
      "{U_eiou}" := NULL,
      "{U_feed}" := NULL,
      "{S_units}" := NULL
    ) %>%
    # Rename the new columns to the original column names
    dplyr::rename(
      "{R}" := paste0(R, suffix),
      "{U}" := paste0(U, suffix),
      "{V}" := paste0(V, suffix),
      "{Y}" := paste0(Y, suffix),
      "{r_eiou}" := paste0(r_eiou, suffix),
      "{U_eiou}" := paste0(U_eiou, suffix),
      "{U_feed}" := paste0(U_feed, suffix),
      "{S_units}" := paste0(S_units, suffix)
    )
}
