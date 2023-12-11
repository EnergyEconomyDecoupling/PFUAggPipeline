#' Aggregation regions
#'
#' The database can benefit from continent and World aggregations.
#' This function bundles those aggregations into a single function.
#'
#' All regional aggregations have names that are 5 characters or longer.
#'
#' @param .psut_data A data frame of PSUT information.
#' @param region_aggregation_map An aggregation map that shows how to
#'                               aggregate countries to regions,
#'                               like FoSUN and FoYUG.
#'                               Entries in the each sublist are not
#'                               assumed to be unique across sublists.
#'                               I.e., a country could be in more than one region.
#' @param continent_aggregation_map An aggregation map that shows how to
#'                                  aggregate countries to continents.
#'                                  Entries in the each sublist are
#'                                  assumed to be unique across sublists.
#'                                  I.e., each country is in one and only one continent.
#' @param world_aggregation_map An aggregation map that shows how to
#'                              aggregate continents to the world.
#' @param country The name of the country column.
#'                Default is `Recca::psut_cols$country`.
#' @param year The name of the year column.
#'             Default is `Recca::psut_cols$year`.
#' @param region The name of the region column.
#'               Default is "Region".
#' @param continent The name of the continent column.
#'                  Default is "Continent".
#' @param world The name of the world column.
#'              Default is "World".
#'
#' @return A data frame that includes new "Country"s for
#'         regions, continents, and the World.
#' @export
region_pipeline <- function(.psut_data,
                            region_aggregation_map,
                            continent_aggregation_map,
                            world_aggregation_map,
                            country = Recca::psut_cols$country,
                            year = Recca::psut_cols$year,
                            region = "Region",
                            continent = "Continent",
                            world = "World") {
  # The region_aggregation_map is not assumed to have unique
  # entries in the Many column for each Few entry.
  # So cycle through each Few entry.
  regions <- names(region_aggregation_map)
  PSUT_Re_regions <- lapply(regions, function(this_region){
    .psut_data |>
      dplyr::left_join(region_aggregation_map[this_region] |>
                         matsbyname::agg_map_to_agg_table(many_colname = country,
                                                          few_colname = region),
                       by = country) |>
      Recca::region_aggregates(many_colname = country,
                               few_colname = region,
                               drop_na_few = TRUE)
  }) |>
    dplyr::bind_rows()

  # Create a data frame of continents,
  # by aggregating countries to continents according to continent_aggregation_map.
  PSUT_Re_continents <- .psut_data |>
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

  # Stack the data frames and return
  dplyr::bind_rows(.psut_data,
                   PSUT_Re_regions,
                   PSUT_Re_continents,
                   PSUT_Re_world)
}
