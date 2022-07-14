#' Create a targets workflow
#'
#' This is a target factory whose arguments
#' specify the details of a targets workflow to be constructed
#'
#' @param countries A string vector of 3-letter country codes.
#'                  Default is "all", meaning all available countries should be analyzed.
#' @param years A numeric vector of years to be analyzed.
#'              Default is "all", meaning all available years should be analyzed.
#' @param psut_release The release we'll use from `pipeline_releases_folder`.
#'                     See details.
#' @param aggregation_maps_path The path to the Excel file of aggregation maps.
#' @param pipeline_releases_folder The path to a folder where releases of output targets are pinned.
#' @param release Boolean that tells whether to do a release of the results.
#'                Default is `FALSE`.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
get_pipeline <- function(countries = "all",
                         years = "all",
                         psut_release,
                         aggregation_maps_path,
                         pipeline_releases_folder,
                         release = FALSE) {

  list(
    #####################
    # Preliminary setup #
    #####################

    # Store some incoming data as targets.
    # These targets are invariant across incoming psut_releases.
    targets::tar_target_raw("Countries", list(countries)),
    targets::tar_target_raw("Years", list(years)),
    targets::tar_target_raw("AggregationMapsPath", aggregation_maps_path),
    targets::tar_target_raw("PinboardFolder", pipeline_releases_folder),
    targets::tar_target_raw("Release", release),

    # Establish prefixes for primary industries
    targets::tar_target_raw(
      "PIndustryPrefixes",
      quote(IEATools::tpes_flows %>% unname() %>% unlist() %>% list())),

    # Establish final demand sectors
    targets::tar_target_raw(
      "FinalDemandSectors",
      quote(IEATools::fd_sectors)),

    # Gather the aggregation maps.
    targets::tar_target_raw(
      "AggregationMaps",
      quote(load_aggregation_maps(path = AggregationMapsPath))),

    # Identify the continents to which we'll aggregate
    targets::tar_target_raw(
      "Continents",
      substitute(AggregationMaps$world_aggregation$World)
    ),

    # Set the pin and release as targets
    targets::tar_target_raw(
      "PSUTRelease",
      unname(psut_release)),

    # Pull in the PSUT data frame
    targets::tar_target_raw(
      "PSUT",
      substitute(pins::board_folder(PinboardFolder, versioned = TRUE) %>%
                   pins::pin_read("psut", version = PSUTRelease) %>%
                   filter_countries_and_years(countries = Countries, years = Years))),


    #########################
    # Regional aggregations #
    #########################

    # Create a continents data frame, grouped by continent,
    # so subsequent operations (region aggregation)
    # will be performed in parallel, if desired.
    targets::tar_target_raw(
      "PSUT_with_continent_col",
      substitute(join_psut_continents(PSUT = PSUT,
                                      continent_aggregation_map = AggregationMaps$continent_aggregation,
                                      continent = "Continent"))),

    # Aggregate by continent
    targets::tar_target_raw(
      "PSUT_Re_continents",
      substitute(continent_aggregation(PSUT_with_continent_col,
                                       continents = Continents,
                                       many_colname = IEATools::iea_cols$country,
                                       few_colname = "Continent")),
      pattern = quote(map(Continents))),

    # Aggregate to world
    targets::tar_target_raw(
      "PSUT_Re_world",
      substitute(Recca::region_aggregates(PSUT_Re_continents %>%
                                            dplyr::left_join(AggregationMaps$world_aggregation %>%
                                                               matsbyname::agg_map_to_agg_table(many_colname = IEATools::iea_cols$country,
                                                                                                few_colname = "World"),
                                                             by = IEATools::iea_cols$country),
                                          many_colname = IEATools::iea_cols$country, # Which actually holds continents
                                          few_colname = "World"))),

    # Bind all region aggregations together
    targets::tar_target_raw(
      "PSUT_Re_all",
      substitute(dplyr::bind_rows(PSUT, PSUT_Re_continents, PSUT_Re_world))),


    ############################
    # Despecified aggregations #
    ############################

    targets::tar_target_raw(
      "PSUT_Re_all_Pr_despec_In_despec",
      substitute(PSUT_Re_all %>%
                   despecified_aggregations(countries = Countries, years = Years,
                                            # We use arrow, from, and of notations.
                                            # Restricting to only these notations makes the code faster.
                                            # Also, need to wrap in a list to ensure the notations_list is
                                            # correctly propagated to all rows in the PSUT_Re_all data frame.
                                            notation = list(RCLabels::notations_list[c("of_notation", "arrow_notation", "from_notation")]))),
      pattern = quote(map(Countries))
    ),


    ################################
    # Grouped product aggregations #
    ################################

    targets::tar_target_raw(
      "ProductAggMap",
      substitute(c(AggregationMaps[["ef_product_aggregation"]],
                   AggregationMaps[["eu_product_aggregation"]]))
    ),

    targets::tar_target_raw(
      "PSUT_Re_all_Pr_group",
      substitute(PSUT_Re_all_Pr_despec_In_despec %>%
                   grouped_aggregations(countries = Countries,
                                        years = Years,
                                        aggregation_map = ProductAggMap,
                                        margin = "Product")),
      pattern = quote(map(Countries))
    ),


    #################################
    # Grouped industry aggregations #
    #################################

    targets::tar_target_raw(
      "IndustryAggMap",
      substitute(AggregationMaps[["ef_sector_aggregation"]])
    ),

    targets::tar_target_raw(
      "PSUT_Re_all_In_group",
      substitute(PSUT_Re_all_Pr_despec_In_despec %>%
                   grouped_aggregations(countries = Countries,
                                        years = Years,
                                        aggregation_map = IndustryAggMap,
                                        margin = "Industry")),
      pattern = quote(map(Countries))
    )


    #############################################
    # Grouped product and industry aggregations #
    #############################################



    # PSUT_Re_all_Pr_group


    # PSUT_Re_all_In_group_Pr_group


    # PSUT_Re_all_InPr_all











  )



  # # Names for targets common to many parts of the pipeline.
  # aggregation_maps_tar_str <- "AggregationMaps"
  # continents_tar_str <- "Continents"
  #
  # # Create the initial targets
  # initial_targets <- init_targets(countries = countries,
  #                                 years = years,
  #                                 aggregation_maps_path = aggregation_maps_path,
  #                                 pipeline_releases_folder = pipeline_releases_folder,
  #                                 release = release,
  #                                 aggregation_maps_tar_str = aggregation_maps_tar_str,
  #                                 continents_tar_str = continents_tar_str)
  #
  # # get_one_middle_pipeline() returns both a list of targets and
  # # a list of cache dependencies.
  # # Create empty lists to gather those items.
  # middle_targets <- list()
  # # Loop over all the items in psut_releases.
  # for (i_pr in 1:length(psut_releases)) {
  #   # Preserve name of i_pr'th psut_release.
  #   pr <- psut_releases[i_pr]
  #   these_mid_targs_and_deps <- get_one_middle_pipeline(pr = pr,
  #                                                       aggregation_maps_tar_str = aggregation_maps_tar_str,
  #                                                       continents_tar_str = continents_tar_str)
  #   middle_targets <- c(middle_targets, these_mid_targs_and_deps)
  # }
  #
  # # Return all targets as a single list
  # c(initial_targets, middle_targets)
}


