#' Pivot a data frame for saving as a .csv file
#'
#' Data frames in tidy format should be pivoted
#' to wide-by-year data frames for saving as .csv files.
#' This function does that pivoting.
#'
#' @param .df The data frame to be pivoted.
#' @param val_cols Data columns to be swapped to the `var_col`.
#'                 These should be variable names as a string vector.
#' @param year_col The string name of the year column.
#'                 Default is `IEATools::iea_cols$year`.
#' @param var_col The name of the resulting variable name column.
#'                Default is "var".
#'
#' @return The pivoted data frame.
#'
#' @export
pivot_for_csv <- function(.df,
                          val_cols,
                          year_col = IEATools::iea_cols$year,
                          var_col = "var") {
  .df |>
    tidyr::pivot_longer(cols = val_cols, names_to = var_col, values_to = "val") |>
    tidyr::pivot_wider(names_from = year_col, values_from = "val")
}
