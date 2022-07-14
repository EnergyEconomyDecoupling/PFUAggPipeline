#' Despecify and aggregate all PSUT matrices
#'
#' This function uses `Recca::despecified_aggregates()` internally.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries to be analyzed.
#' @param years The years to be analyzed.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#' @param R_aggregated_colname,U_aggregated_colname,V_aggregated_colname,Y_aggregated_colname,r_eiou_aggregated_colname,U_eiou_aggregated_colname,U_feed_aggregated_colname,S_units_aggregated_colname The names of output aggregated columns.
#' @param aggregated_suffix The suffix for columns of aggregated matrices.
#'                          Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A data frame with despecified and aggregated matrices.
#'
#' @export
despecify_aggregations <- function(.psut_data,
                                   countries,
                                   years,
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
    Recca::despecified_aggregates(R = R, U = U, V = V, Y = Y,
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
    rename_aggregated_psut_columns(R = R, U = U, V = V, Y = Y,
                                   r_eiou = r_eiou, U_eiou = U_eiou, U_feed = U_feed, S_units = S_units,
                                   aggregated_suffix = aggregated_suffix)
}


product_group_aggregations <- function(.psut_data,
                                       countries,
                                       years,
                                       aggregation_map) {
  # .psut_data %>%
  #   PFUDatabase::filter_countries_years(countries = countries, years = years) %>%

}






#' Delete original and rename aggregated columns
#'
#' This is a helpful function that deletes original PSUT columns and
#' renames the aggregated columns to the original names.
#'
#' Columns with names given by `R`, `U`, `V`, `Y`, `r_EIOU`, `U_EIOU`,
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries to be analyzed.
#' @param years The years to be analyzed.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed,S_units The names of input columns in `.psut_data`.
#' @param aggregated_suffix The suffix for columns of aggregated matrices.
#'                          Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return
#' @export
#'
#' @examples
rename_aggregated_psut_columns <- function(.psut_data,
                                           R = Recca::psut_cols$R,
                                           U = Recca::psut_cols$U,
                                           V = Recca::psut_cols$V,
                                           Y = Recca::psut_cols$Y,
                                           r_eiou = Recca::psut_cols$r_eiou,
                                           U_eiou = Recca::psut_cols$U_eiou,
                                           U_feed = Recca::psut_cols$U_feed,
                                           S_units = Recca::psut_cols$S_units,
                                           aggregated_suffix = Recca::aggregate_cols$aggregated_suffix) {

  .psut_data %>%
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
    dplyr::rename(
      "{R}" := paste0(R, aggregated_suffix),
      "{U}" := paste0(U, aggregated_suffix),
      "{V}" := paste0(V, aggregated_suffix),
      "{Y}" := paste0(Y, aggregated_suffix),
      "{r_eiou}" := paste0(r_eiou, aggregated_suffix),
      "{U_eiou}" := paste0(U_eiou, aggregated_suffix),
      "{U_feed}" := paste0(U_feed, aggregated_suffix),
      "{S_units}" := paste0(S_units, aggregated_suffix)
    )

}
