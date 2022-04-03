#' Pivot an aggregation or efficiency data frame wide by years
#'
#' Converts a aggregation of efficiencies data frame (in tidy format)
#' to one that is wide by years.
#'
#' @param .df The data frame to pivot.
#' @param pivot_cols The columns to pivot.
#' @param country,year See `IEATools::iea_cols`.
#' @param quantity,.values See `IEATools::template_cols`.
#'
#' @return A version of `.df` that is pivoted wide by years.
#'
#' @export
pivot_agg_eta_wide_by_year <- function(.df,
                                       pivot_cols,
                                       country = IEATools::iea_cols$country,
                                       year = IEATools::iea_cols$year,
                                       quantity = IEATools::template_cols$quantity,
                                       .values = IEATools::template_cols$.values) {
  .df %>%
    tidyr::pivot_longer(cols = pivot_cols,
                        names_to = quantity,
                        values_to = .values) %>%
    dplyr::arrange(.data[[year]]) %>%
    tidyr::pivot_wider(names_from = year, values_from = .values) %>%
    dplyr::arrange(.data[[country]])
}
