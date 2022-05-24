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
#' The exemplar table is assumed to be on the `exemplar_table_tab` tab
#' of the Excel file.
#'
#' @param exemplar_table_path The path to the exemplar table
#' @param exemplar_table_tab The name of the tab in the file at `exemplar_table_path`
#'                           where the region information exists.
#'                           Default is "exemplar_table".
#' @param region_code The name of a column containing region codes.
#'                    Default is "Region.code".
#' @param continent The name of the continent column. Default is "Continent".
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
  # pivot the table and return an aggregation map
  region_table %>%
    dplyr::select(keep) %>%
    tidyr::pivot_longer(cols = year_col_names, names_to = year, values_to = country) %>%
    dplyr::rename(
      "{continent}" := .data[[region_code]]
    ) %>%
    matsbyname::agg_table_to_agg_map(few_colname = continent, many_colname = country)
}


#' Join the continent aggregation map to the PSUT data frame
#'
#' The join is accomplished in a way that supports aggregating into continents.
#'
#' @param PSUT The `PSUT` data frame.
#' @param continent_aggregation_map A list with names for continents and vectors of countries as items.
#' @param country The name of the country column. Default is `IEATools::iea_cols$country`.
#' @param continent The name of the continent column. Default is "Continent".
#'
#' @return `PSUT` with an additional "Continent" column.
#'
#' @export
join_psut_continents <- function(PSUT,
                                 continent_aggregation_map,
                                 country = IEATools::iea_cols$country,
                                 continent = "Continent") {
  agg_df <- matsbyname::agg_map_to_agg_table(continent_aggregation_map,
                                             few_colname = continent,
                                             many_colname = IEATools::iea_cols$country)
  dplyr::left_join(PSUT, agg_df, by = country)
}


#' Calculate continent aggregation
#'
#' This function is a wrapper for `Recca::region_aggregates()`
#' that also filters on `continents`,
#' thereby enabling parallel processing across continents.
#'
#' @param PSUT A data frame of PSUT matrices
#' @param continents The continents for which aggregation is desired.
#' @param continent The name of the continent column in the `PSUT` data frame.
#'                  Default is "Continent".
#' @param many_colname The name of the column of many things that will be aggregated into continents.
#'                     Default is `IEATools::iea_cols$country`.
#' @param few_colname The name of the column of few things that will remain on output.
#'                    Default is the same as the value of the `continent` argument.
#'
#' @return A data frame in which countries are aggregated to continents according to
#'         the aggregatio map.
#'
#' @export
continent_aggregation <- function(PSUT,
                                  continents,
                                  continent = "Continent",
                                  many_colname = IEATools::iea_cols$country,
                                  few_colname = continent) {
  filtered_psut <- PSUT %>%
    dplyr::filter(.data[[few_colname]] %in% continents)
  Recca::region_aggregates(filtered_psut,
                           many_colname = many_colname,
                           few_colname = few_colname)
}
