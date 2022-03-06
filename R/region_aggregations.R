#' Create an aggregation map for continent aggregation
#'
#' An aggregation map is a named list.
#' Names are the continents.
#' List items are 3-letter country codes.
#'
#' The exemplar table is assumed to be an Excel file with the following columns:
#' `region_code` and years (as numbers).
#' The body of the table should contain 3-letter codes
#' of countries.
#' The exemplar table is assumed to be on the `exemplar_table_tab` tab of the Excel file.
#'
#' @param exemplar_table_path The path to the exemplar table
#' @param region_code The name of a column containing region codes.
#'                    Default is "Region.code".
#' @param exemplar_table_tab The name of the tab in the file at `exemplar_table_path`
#'                           where the region information exists.
#'                           Default is "exemplar_table".
#' @param country The name of the country column in the outgoing data frame.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column in the outgoing data frame.
#'             Default is `IEATools::iea_cols$year`.
#'
#' @return A continent aggregation map.
#'
#' @export
continent_aggregation_map <- function(exemplar_table_path,
                                      exemplar_table_tab = "exemplar_table",
                                      region_code = "Region.code",
                                      continent = "Continent",
                                      country = IEATools::iea_cols$country,
                                      year = IEATools::iea_cols$year) {

  # Load the file
  region_table <- readxl::read_excel(path = exemplar_table_path,
                                     sheet = exemplar_table_tab)
  # Find names for the region_code column and year columns
  region_code_col <- which((names(region_table) == region_code), arr.ind = TRUE)
  year_col_nums <- IEATools::year_cols(region_table)
  year_col_names <- IEATools::year_cols(region_table, return_names = TRUE)
  keep <- c(region_code_col, year_col_nums)
  # pivot the table and return
  df <- region_table |>
    dplyr::select(keep) |>
    tidyr::pivot_longer(cols = year_col_names, names_to = year, values_to = country) |>
    # Remove an blank entries that appear as NA
    dplyr::filter(!is.na(.data[[country]])) |>
    dplyr::rename(
      "{continent}" := .data[[region_code]]
    ) |>
    dplyr::select(-.data[[year]]) |>
    unique() |>
    # Create a nested tibble
    dplyr::nest_by(.data[[continent]], .key = country) |>
    dplyr::mutate(
      "{country}" := .data[[country]] |>
        as.list() |>
        unname()
    )
  # Return a list with countries as list items and continents as names.
  df[[country]] |>
    setNames(df[[continent]])
}
