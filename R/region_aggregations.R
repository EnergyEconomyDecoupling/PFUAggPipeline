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
                                             many_colname = country)
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
#' @param years The years for which aggregation is desired.
#' @param continent The name of the continent column in the `PSUT` data frame.
#'                  Default is "Continent".
#' @param year The name of the year column in the `PSUT` data frame.
#'             Default is `IEATools::iea_cols$year`.
#' @param many_colname The name of the column of many things that will be aggregated into continents.
#'                     Default is `IEATools::iea_cols$country`.
#' @param few_colname The name of the column of few things that will remain on output.
#'                    Default is the same as the value of the `continent` argument.
#'
#' @return A data frame in which countries are aggregated to continents according to
#'         the aggregation map.
#'
#' @export
continent_aggregation <- function(PSUT,
                                  continents,
                                  years,
                                  continent = "Continent",
                                  year = IEATools::iea_cols$year,
                                  many_colname = IEATools::iea_cols$country,
                                  few_colname = continent) {
  filtered_psut <- PSUT %>%
    dplyr::filter(.data[[few_colname]] %in% continents, .data[[year]] %in% years)
  Recca::region_aggregates(filtered_psut,
                           many_colname = many_colname,
                           few_colname = few_colname)
}
