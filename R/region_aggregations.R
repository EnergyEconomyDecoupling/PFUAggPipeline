#' Join the continent aggregation map to the PSUT data frame
#'
#' The join is accomplished in a way that supports aggregating into continents.
#'
#' @param PSUT The `PSUT` data frame.
#' @param years The years for which this join should occur.
#' @param continent_aggregation_map A list with names for continents and vectors of countries as items.
#' @param country The name of the country column. Default is `Recca::psut_cols$country`.
#' @param year The name of the year column. Default is `Recca::psut_cols$year`.
#' @param continent The name of the continent column. Default is "Continent".
#'
#' @return `PSUT` with an additional "Continent" column.
#'
#' @export
join_psut_continents <- function(PSUT,
                                 years,
                                 continent_aggregation_map,
                                 country = Recca::psut_cols$country,
                                 year = Recca::psut_cols$year,
                                 continent = "Continent") {
  agg_df <- matsbyname::agg_map_to_agg_table(continent_aggregation_map,
                                             few_colname = continent,
                                             many_colname = country)
  filtered_data <- PSUT %>%
    dplyr::filter(.data[[year]] %in% years)

  rm(PSUT)
  gc()

  dplyr::left_join(filtered_data, agg_df, by = country)
}


#' Calculate continent aggregation
#'
#' This function is a wrapper for `Recca::region_aggregates()`
#' that also filters on `continents`,
#' thereby enabling parallel processing across continents.
#'
#' @param PSUT_Chop_all A data frame of PSUT matrices that were previously chopped.
#' @param continents The continents for which aggregation is desired.
#' @param years The years for which aggregation is desired.
#' @param continent The name of the continent column in the `PSUT` data frame.
#'                  Default is "Continent".
#' @param year The name of the year column in the `PSUT` data frame.
#'             Default is `Recca::psut_cols$year`.
#' @param many_colname The name of the column of many things that will be aggregated into continents.
#'                     Default is `Recca::psut_cols$country`.
#' @param few_colname The name of the column of few things that will remain on output.
#'                    Default is the same as the value of the `continent` argument.
#'
#' @return A data frame in which countries are aggregated to continents
#'         according to the aggregation map
#'         for the desired `continents` and `years`.
#'
#' @export
continent_aggregation <- function(PSUT_Chop_all,
                                  continents,
                                  years,
                                  continent = "Continent",
                                  year = Recca::psut_cols$year,
                                  many_colname = Recca::psut_cols$country,
                                  few_colname = continent) {
  filtered_psut <- PSUT_Chop_all %>%
    dplyr::filter(.data[[few_colname]] %in% continents, .data[[year]] %in% years)

  rm(PSUT_Chop_all)
  gc()

  Recca::region_aggregates(filtered_psut,
                           many_colname = many_colname,
                           few_colname = few_colname,
                           drop_na_few = TRUE)
}


#' Aggregate continents to world
#'
#' This function is a wrapper for `Recca::region_aggregates()`
#' that also filters on `years`,
#' thereby enabling parallel processing across years.
#'
#' @param PSUT_Chop_all_Re_continents A data frame of PSUT matrices for continents.
#' @param years The years for which aggregation is desired.
#' @param world_aggregation_map The aggregation map to sum continents into the world.
#' @param country The name of the country column in the `PSUT_Chop_all_Re_continents` data frame.
#'                Default is `Recca::psut_cols$country`.
#' @param world The name of the column of few things that will remain on output.
#'              Default is "World".
#' @param year The name of the year column in the `PSUT` data frame.
#'             Default is `Recca::psut_cols$year`.
#' @param many_colname The name of the column of many things that will be aggregated into continents.
#'                     Default is `Recca::psut_cols$country`.
#' @param few_colname The name of the column of few things that will remain on output.
#'                    Default is the same as the value of the `world` argument.
#' @return A data frame in which continents are aggregated to world
#'         according to the aggregation map
#'         for the desired `years`.
#'
#' @export
world_aggregation <- function(PSUT_Chop_all_Re_continents,
                              years,
                              world_aggregation_map,
                              country = Recca::psut_cols$country,
                              world = "World",
                              year = Recca::psut_cols$year,
                              many_colname = Recca::psut_cols$country,
                              few_colname = world) {
  filtered_data <- PSUT_Chop_all_Re_continents %>%
    dplyr::filter(.data[[year]] %in% years)

  rm(PSUT_Chop_all_Re_continents)
  gc()

  filtered_data %>%
    dplyr::left_join(world_aggregation_map %>%
                       matsbyname::agg_map_to_agg_table(many_colname = country,
                                                        few_colname = few_colname),
                     by = country) %>%
    Recca::region_aggregates(many_colname = country,
                             few_colname = few_colname,
                             drop_na_few = TRUE)
}


#' Aggregation regions
#'
#' The database can benefit from continent and World aggregations.
#' This function bundles those aggregations into a single function.
#'
#' All regional aggregations have names that are 5 characters or longer.
#'
#' @param .psut_data
#'
#' @return A data frame that includes new "Country"s for
#'         continents and the World.
#' @export
#'
#' @examples
region_pipeline <- function(.psut_data,
                            years,
                            continent_aggregation_map,
                            world_aggregation_map,
                            country = Recca::psut_cols$country,
                            year = Recca::psut_cols$year,
                            continent = "Continent",
                            world = "World") {

  filtered_data <- .psut_data |>
    dplyr::filter(.data[[year]] %in% years)

  rm(.psut_data)
  gc()

  # Create a data frame of continents,
  # by aggregating countries to continents according to continent_aggregation_map.
  PSUT_Re_continents <- filtered_data |>
    dplyr::left_join(continent_aggregation_map |>
                       matsbyname::agg_map_to_agg_table(many_colname = country,
                                                        few_colname = continent),
                     by = country) |>
    Recca::region_aggregates(many_colname = country,
                             few_colname = continent,
                             drop_na_few = TRUE)

  # Create a data frame of the World,
  # by aggregating continents to World according to world_aggregation_map.
  PSUT_Re_world <- PSUT_Re_continents |>
    dplyr::left_join(world_aggregation_map |>
                       matsbyname::agg_map_to_agg_table(many_colname = country,
                                                        few_colname = world),
                     by = country) %>%
    Recca::region_aggregates(many_colname = country,
                             few_colname = world,
                             drop_na_few = TRUE)

  # Stach the data frames and return
  dplyr::bind_rows(filtered_data,
                   PSUT_Re_continents,
                   PSUT_Re_world)
}
