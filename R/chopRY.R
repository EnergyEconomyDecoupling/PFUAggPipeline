#' Chop the ECC for **Y**
#'
#' This function uses `Recca::chop_Y()` internally
#' to calculate new ECCs for each row or column in the **Y** matrix.
#'
#' Chopping in the **Y** matrix and calculating a new ECC
#' involves a matrix inversion step.
#' The `method` argument specifies which method should be used for
#' calculating the inverse.
#' See `matsbyname::invert_byname()`.
#'
#' Both `tol` and `method` should be a single values and apply to all matrices being inverted.
#'
#' @param .psut_data A data frame of PSUT matrices. It should be wide by matrices.
#' @param countries The countries to analyze.
#' @param years The years to analyze.
#' @param p_industries Industries that count for primary energy aggregates.
#' @param fd_sectors Final demand sectors that count for final demand aggregates.
#' @param method Tells how to invert matrices. Default is "SVD". See details.
#' @param tol The tolerance for detecting linear dependencies in the columns of matrices to be inverted.
#'            Default is `.Machine$double.eps`.
#'
#' @return A data frame of chopped final demand matrix ECCs.
#'
#' @export
chop_Y_eccs <- function(.psut_data,
                        countries,
                        years,
                        method = "SVD",
                        tol = .Machine$double.eps) {
  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)
  # Check for the case where we have no data for that country and year.
  # In that event, we simply want to return the data frame.
  if (nrow(filtered_data) == 0) {
    return(filtered_data)
  }
  filtered_data %>%
    Recca::chop_Y(calc_pfd_aggs = FALSE,
                  pattern_type = "leading",
                  unnest = TRUE,
                  method = method,
                  tol_invert = tol)
}


#' Chop the ECC for **R**
#'
#' This function uses `Recca::chop_R()` internally
#' to calculate new ECCs for each column in the **R** matrix.
#'
#' Chopping in the **R** matrix and calculating a new ECC
#' involves a matrix inversion step.
#' The `method` argument specifies which method should be used for
#' calculating the inverse.
#' See `matsbyname::invert_byname()`.
#'
#' Both `tol` and `method` should be a single values and apply to all matrices being inverted.
#'
#' @param .psut_data A data frame of PSUT matrices. It should be wide by matrices.
#' @param countries The countries to analyze.
#' @param years The years to analyze.
#' @param p_industries Industries that count for primary energy aggregates.
#' @param fd_sectors Final demand sectors that count for final demand aggregates.
#' @param method Tells how to invert matrices. Default is "SVD". See details.
#' @param tol The tolerance for detecting linear dependencies in the columns of matrices to be inverted.
#'            Default is `.Machine$double.eps`.
#'
#' @return A data frame of chopped resource matrix ECCs.
#'
#' @export
chop_R_eccs <- function(.psut_data,
                        countries,
                        years,
                        method = "SVD",
                        tol_invert = .Machine$double.eps) {

  filtered_data <- .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(.psut_data)
  }
  filtered_data %>%
    Recca::chop_R(calc_pfd_aggs = FALSE,
                  pattern_type = "leading",
                  unnest = TRUE,
                  method = method,
                  tol_invert = tol)
}


#' Stack chopped ECC data frames
#'
#' @param PSUT_Re_all_Gr_all
#' @param PSUT_Re_all_Gr_all_Chop_Y
#' @param PSUT_Re_all_Gr_all_Chop_R
#' @param chop_mat
#' @param Y
#' @param R
#' @param chop_var
#' @param product_sector
#' @param none
#'
#' @return
#' @export
#'
#' @examples
stack_chop_ECCs <- function(PSUT_Re_all_Gr_all,
                            PSUT_Re_all_Gr_all_Chop_Y,
                            PSUT_Re_all_Gr_all_Chop_R,
                            chop_mat = PFUAggDatabase::aggregation_df_cols$chopped_mat,
                            Y_matname = "Y",
                            R_matname = "R",
                            chop_var = PFUAggDatabase::aggregation_df_cols$chop_var,
                            product_sector = PFUAggDatabase::aggregation_df_cols$product_sector,
                            none = "None") {

  # Build a combined data frame.
  dplyr::bind_rows(PSUT_Re_all_Gr_all %>%
                     dplyr::mutate(
                       "{chop_mat}" := none,
                       "{chop_var}" := none
                     ),
                   PSUT_Re_all_Gr_all_Chop_Y %>%
                     rename_prime_psut_columns() %>%
                     dplyr::mutate(
                       "{chop_mat}" := Y_matname
                     ) %>%
                     dplyr::rename(
                       "{chop_var}" := .data[[product_sector]]
                     ),
                   PSUT_Re_all_Gr_all_Chop_R %>%
                     rename_prime_psut_columns() %>%
                     dplyr::mutate(
                       "{chop_mat}" := R_matname
                     ) %>%
                     dplyr::rename(
                       "{chop_var}" := .data[[product_sector]]
                     )
  )
}


#' Rename "_prime" columns
#'
#' Deletes the original (un-prime) columns and renames the prime columns to those names
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

