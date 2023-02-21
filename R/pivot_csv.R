#' Title
#'
#' @param .df
#' @param year_col
#' @param val_cols
#' @param var_col
#'
#' @return
#' @export
#'
#' @examples
pivot_for_csv <- function(.df, year_col = "Year", val_cols, var_col = ".var") {
  # year_cols <- sae |>
  #   IEATools::year_cols()
  # sae |>
  #   tidyr::pivot_longer(cols = year_cols, names_to = "Year") |>
  #   tidyr::pivot_wider(names_from = "var", values_from = "value") |>
  #   View()
  # .df |>
  #   tidyr::pivot_longer(cols = c("EX.p", "EX.f", "EX.u", "eta_pf", "eta_fu", "eta_pu"), names_to = "var", values_to = "val") |>
  #   tidyr::pivot_wider(names_from = "Year", values_from = "val")
  .df |>
    tidyr::pivot_longer(cols = val_cols, names_to = var_col, values_to = "val") |>
    tidyr::pivot_wider(names_from = year_col, values_from = "val")
}
