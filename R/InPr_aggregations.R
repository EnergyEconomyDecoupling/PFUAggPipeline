#' Despecify and aggregate all PSUT matrices
#'
#' This function uses `Recca::despecified_aggregates()` internally.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param countries The countries to be analyzed.
#' @param years The years to be analyzed.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed The names of columns in `.psut_data` containing matrices.
#' @param aggregated_suffix The suffix for columns of aggregated matrices.
#'                          Default is `Recca::aggregate_cols$aggregated_suffix`.
#'
#' @return A data frame with despecified and aggregated matrices.
#'
#' @export
despecify_aggregations <- function(.psut_data,
                                   countries = Countries,
                                   years = Years,
                                   # Names of original matrices in .psut_data
                                   R = Recca::psut_cols$R,
                                   U = Recca::psut_cols$U,
                                   V = Recca::psut_cols$V,
                                   Y = Recca::psut_cols$Y,
                                   r_EIOU = Recca::psut_cols$r_eiou,
                                   U_EIOU = Recca::psut_cols$U_eiou,
                                   U_feed = Recca::psut_cols$U_feed,
                                   S_units = Recca::psut_cols$S_units,
                                   # Names for the aggregated matrices after aggregation
                                   R_aggregated_colname = paste0(Recca::psut_cols$R, aggregated_suffix),
                                   U_aggregated_colname = paste0(Recca::psut_cols$U, aggregated_suffix),
                                   V_aggregated_colname = paste0(Recca::psut_cols$V, aggregated_suffix),
                                   Y_aggregated_colname = paste0(Recca::psut_cols$Y, aggregated_suffix),
                                   r_EIOU_aggregated_colname = paste0(Recca::psut_cols$r_eiou, aggregated_suffix),
                                   U_EIOU_aggregated_colname = paste0(Recca::psut_cols$U_eiou, aggregated_suffix),
                                   U_feed_aggregated_colname = paste0(Recca::psut_cols$U_feed, aggregated_suffix),
                                   S_units_aggregated_colname = paste0(Recca::psut_cols$S_units, aggregated_suffix),
                                   # The suffix added to the name of the columns of aggregated matrices.
                                   aggregated_suffix = Recca::aggregate_cols$aggregated_suffix) {

  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::despecified_aggregates(R = R, U = U, V = V, Y = Y,
                                  r_EIOU = r_EIOU, U_EIOU = U_EIOU, U_feed = U_feed,
                                  S_units = S_units,
                                  R_aggregated_colname = R_aggregated_colname,
                                  U_aggregated_colname = U_aggregated_colname,
                                  V_aggregated_colname = V_aggregated_colname,
                                  Y_aggregated_colname = Y_aggregated_colname,
                                  r_EIOU_aggregated_colname = r_EIOU_aggregated_colname,
                                  U_EIOU_aggregated_colname = U_EIOU_aggregated_colname,
                                  U_feed_aggregated_colname = U_feed_aggregated_colname,
                                  S_units_aggregated_colname = S_units_aggregated_colname,
                                  aggregated_suffix = aggregated_suffix) %>%
    dplyr::mutate(
      "{R}" := NULL,
      "{U}" := NULL,
      "{V}" := NULL,
      "{Y}" := NULL,
      "{r_EIOU}" := NULL,
      "{U_EIOU}" := NULL,
      "{U_feed}" := NULL,
      "{S_units}" := NULL
    ) %>%
    dplyr::rename(
      "{R}" := R_aggregated_colname,
      "{U}" := U_aggregated_colname,
      "{V}" := V_aggregated_colname,
      "{Y}" := Y_aggregated_colname,
      "{r_EIOU}" := r_EIOU_aggregated_colname,
      "{U_EIOU}" := U_EIOU_aggregated_colname,
      "{U_feed}" := U_feed_aggregated_colname,
      "{S_units}" := S_units_aggregated_colname
    )
}


product_group_aggregations <- function(.psut_data,
                                       countries = Countries,
                                       years = Years,
                                       aggregation_map) {
  # .psut_data %>%
  #   PFUDatabase::filter_countries_years(countries = countries, years = years) %>%

}


