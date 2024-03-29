#' Chop the **R** and **Y** matrices
#'
#' These functions use `Recca::chop_R()` and `Recca::chop_Y()` internally
#' to calculate new ECCs for each column in the **R** matrix
#' and for each row and column for the **Y** matrix.
#'
#' Chopping in the **R** and **Y** matrices and calculating a new ECC
#' involves a matrix inversion step.
#' The `method` argument specifies which method should be used for
#' calculating the inverse.
#' See `matsbyname::invert_byname()` for additional details.
#'
#' Both `tol_invert` and `method` should be a single values and apply to all matrices being inverted.
#'
#' When `do_chops` is `FALSE`, a data frame is returned
#' with no rows and the same columns as `.psut_data`.
#'
#' @param .psut_data A data frame of PSUT matrices. It should be wide by matrices.
#' @param countries The countries to analyze.
#' @param years The years to analyze.
#' @param do_chops A boolean that tells whether to do the **R** and **Y** chops.
#' @param method Tells how to invert matrices. Default is "SVD". See details.
#' @param tol_invert The tolerance for detecting linear dependencies in the columns of matrices to be inverted.
#'                   Default is `.Machine$double.eps`.
#'
#' @return A data frame of chopped **R** or **Y** matrix ECCs.
#'
#' @name chopRY
NULL

#' @export
#' @rdname chopRY
chop_R_eccs <- function(.psut_data,
                        countries,
                        years,
                        do_chops,
                        method = "SVD",
                        tol_invert = .Machine$double.eps) {

  if (!do_chops) {
    return(NULL)
  }

  filtered_data <- .psut_data %>%
    PFUPipelineTools::filter_countries_years(countries = countries, years = years)

  rm(.psut_data)
  gc()

  if (nrow(filtered_data) == 0) {
    return(filtered_data)
  }
  filtered_data %>%
    Recca::chop_R(calc_pfd_aggs = FALSE,
                  piece = "noun",
                  notation = RCLabels::bracket_notation,
                  pattern_type = "literal",
                  unnest = TRUE,
                  method = method,
                  tol_invert = tol_invert)
}


#' @export
#' @rdname chopRY
chop_Y_eccs <- function(.psut_data,
                        countries,
                        years,
                        do_chops,
                        method = "SVD",
                        tol_invert = .Machine$double.eps) {
  if (!do_chops) {
    return(NULL)
  }

  filtered_data <- .psut_data %>%
    PFUPipelineTools::filter_countries_years(countries = countries, years = years)

  rm(.psut_data)
  gc()

  # Check for the case where we have no data for that country and year.
  # In that event, we simply want to return the data frame.
  if (nrow(filtered_data) == 0) {
    return(filtered_data)
  }
  filtered_data %>%
    Recca::chop_Y(calc_pfd_aggs = FALSE,
                  piece = "noun",
                  notation = RCLabels::bracket_notation,
                  pattern_type = "literal",
                  unnest = TRUE,
                  method = method,
                  tol_invert = tol_invert)
}


#' Stack chopped ECC data frames
#'
#' This function stacks chopped data frames with `dplyr::bind_rows()`.
#'
#' @param psut,chop_Y,chop_R Data frames to be stacked.
#' @param chopped_mat,chopped_var Names of columns that tell the matrix that has been chopped (`chop_mat`) and
#'                          the column that contains the the row or column name used for this chop.
#' @param R_matname,Y_matname The names of **R** and **Y** matrices to be added to the `chopped_var` column of the data frame.
#'                            Defaults are taken from `Recca::psut_cols`.
#' @param product_sector The name of the data frame column that contains the variable that has been chopped.
#' @param none The string for no chopping. Used with `PSUT_Re_all_Gr_all`.
#'             Default is `PFUAggPipeline::agg_metadata$none`.
#'
#' @return A row-bound version of `PSUT_Re_all_Gr_all`, `PSUT_Re_all_Gr_all_Chop_Y`, and `PSUT_Re_all_Gr_all_Chop_R`.
#'
#' @export
stack_chopped_ECCs <- function(psut,
                               chop_R = NULL,
                               chop_Y = NULL,
                               chopped_mat = PFUAggPipeline::aggregation_df_cols$chopped_mat,
                               chopped_var = PFUAggPipeline::aggregation_df_cols$chopped_var,
                               Y_matname = Recca::psut_cols$Y,
                               R_matname = Recca::psut_cols$R,
                               product_sector = PFUAggPipeline::aggregation_df_cols$product_sector,
                               none = PFUAggPipeline::agg_metadata$none) {

  # Build a combined data frame.
  out <- psut %>%
    dplyr::mutate(
      "{chopped_mat}" := none,
      "{chopped_var}" := none
    )
  if (!is.null(chop_R)) {
    out <- dplyr::bind_rows(out,
                            chop_R %>%
                              rename_prime_psut_columns() %>%
                              dplyr::mutate(
                                "{chopped_mat}" := R_matname
                              ) %>%
                              dplyr::rename(
                                "{chopped_var}" := .data[[product_sector]]
                              ))
  }
  if (!is.null(chop_Y)) {
    out <- dplyr::bind_rows(out,
                            chop_Y %>%
                              rename_prime_psut_columns() %>%
                              dplyr::mutate(
                                "{chopped_mat}" := Y_matname
                              ) %>%
                              dplyr::rename(
                                "{chopped_var}" := .data[[product_sector]]
                              ))
  }
  return(out)
}


#' Rename "_prime" columns
#'
#' Deletes the original (un-prime) columns and renames the prime columns to those names.
#'
#' This is an internal helper function and not intended for broader use.
#'
#' @param .psut_data A data frame of PSUT matrices.
#' @param R,U,V,Y,r_eiou,U_eiou,U_feed The names of input columns in `.psut_data`.
#' @param suffix The suffix for columns of aggregated matrices.
#'               Default is "_prime".
#'
#' @return A version of `.psut_data` with original PSUT columns deleted and suffixed columns renamed to original names.
rename_prime_psut_columns <- function(.psut_data,
                                      suffix = "_prime",
                                      R = Recca::psut_cols$R,
                                      U = Recca::psut_cols$U,
                                      V = Recca::psut_cols$V,
                                      Y = Recca::psut_cols$Y,
                                      r_eiou = Recca::psut_cols$r_eiou,
                                      U_eiou = Recca::psut_cols$U_eiou,
                                      U_feed = Recca::psut_cols$U_feed) {
  .psut_data %>%
    # Delete the original columns
    dplyr::mutate(
      "{R}" := NULL,
      "{U}" := NULL,
      "{V}" := NULL,
      "{Y}" := NULL,
      "{r_eiou}" := NULL,
      "{U_eiou}" := NULL,
      "{U_feed}" := NULL
    ) %>%
    # Rename the new columns to the original column names
    dplyr::rename(
      "{R}" := paste0(R, suffix),
      "{U}" := paste0(U, suffix),
      "{V}" := paste0(V, suffix),
      "{Y}" := paste0(Y, suffix),
      "{r_eiou}" := paste0(r_eiou, suffix),
      "{U_eiou}" := paste0(U_eiou, suffix),
      "{U_feed}" := paste0(U_feed, suffix))
}

