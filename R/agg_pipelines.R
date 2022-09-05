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
#' @param pipeline_caches_folder The path to a folder where releases of pipeline caches are stored.
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
                         pipeline_caches_folder,
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
    targets::tar_target_raw("PipelineCachesFolder", pipeline_caches_folder),
    targets::tar_target_raw("ExcelOutputFolder", file.path(pipeline_releases_folder, "eta_pfu_excel")),
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

    targets::tar_target_raw(
      "CountriesContinentsWorld",
      substitute(c(Countries, Continents, "World"))
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
                   PFUDatabase::filter_countries_years(countries = Countries, years = Years))),


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
                                       years = Years,
                                       many_colname = IEATools::iea_cols$country,
                                       few_colname = "Continent")),
      pattern = quote(cross(Continents))
    ),

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
      "PSUT_Re_all_Ds_PrIn",
      substitute(PSUT_Re_all %>%
                   despecified_aggregations(countries = CountriesContinentsWorld,
                                            years = Years,
                                            # We use arrow, from, and of notations.
                                            # Restricting to only these notations makes the code faster.
                                            # Also, need to wrap in a list to ensure the notations_list is
                                            # correctly propagated to all rows in the PSUT_Re_all data frame.
                                            notation = list(RCLabels::notations_list[c("of_notation", "arrow_notation", "from_notation")]))),
      pattern = quote(cross(CountriesContinentsWorld))
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
      "PSUT_Re_all_Gr_Pr",
      substitute(PSUT_Re_all_Ds_PrIn %>%
                   grouped_aggregations(countries = CountriesContinentsWorld,
                                        years = Years,
                                        aggregation_map = ProductAggMap,
                                        margin = "Product")),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    #################################
    # Grouped industry aggregations #
    #################################

    targets::tar_target_raw(
      "IndustryAggMap",
      substitute(AggregationMaps[["ef_sector_aggregation"]])
    ),

    targets::tar_target_raw(
      "PSUT_Re_all_Gr_In",
      substitute(PSUT_Re_all_Ds_PrIn %>%
                   grouped_aggregations(countries = CountriesContinentsWorld,
                                        years = Years,
                                        aggregation_map = IndustryAggMap,
                                        margin = "Industry")),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    #############################################
    # Grouped product and industry aggregations #
    #############################################

    targets::tar_target_raw(
      "PSUT_Re_all_Gr_PrIn",
      substitute(PSUT_Re_all_Ds_PrIn %>%
                   grouped_aggregations(countries = CountriesContinentsWorld,
                                        years = Years,
                                        aggregation_map = c(ProductAggMap, IndustryAggMap),
                                        margin = c("Product", "Industry"))),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    ########################################
    # Stack product and industry groupings #
    ########################################

    targets::tar_target_raw(
      "PSUT_Re_all_Gr_all",
      substitute(stack_PrIn_aggregations(PSUT_Re_all = PSUT_Re_all,
                                         PSUT_Re_all_Ds_PrIn = PSUT_Re_all_Ds_PrIn,
                                         PSUT_Re_all_Gr_Pr = PSUT_Re_all_Gr_Pr,
                                         PSUT_Re_all_Gr_In = PSUT_Re_all_Gr_In,
                                         PSUT_Re_all_Gr_PrIn = PSUT_Re_all_Gr_PrIn))
    ),


    ################
    # Chop R and Y #
    ################

    # # Chop R
    # targets::tar_target_raw(
    #   "PSUT_Re_all_Gr_all_Chop_R",
    #   substitute(PSUT_Re_all_Gr_all %>%
    #                chop_R_eccs(countries = CountriesContinentsWorld,
    #                            years = Years,
    #                            method = "SVD")),
    #   pattern = quote(cross(CountriesContinentsWorld))
    # ),
    #
    # # Chop Y
    # targets::tar_target_raw(
    #   "PSUT_Re_all_Gr_all_Chop_Y",
    #   substitute(PSUT_Re_all_Gr_all %>%
    #                chop_Y_eccs(countries = CountriesContinentsWorld,
    #                            years = Years,
    #                            method = "SVD")),
    #   pattern = quote(cross(CountriesContinentsWorld))
    # ),


    ######################
    # Stack chopped ECCs #
    ######################

    # targets::tar_target_raw(
    #   "PSUT_Re_all_Gr_all_Chop_all",
    #   substitute(stack_choped_ECCs(PSUT_Re_all_Gr_all,
    #                                PSUT_Re_all_Gr_all_Chop_Y,
    #                                PSUT_Re_all_Gr_all_Chop_R))
    # ),
    targets::tar_target_raw(
      "PSUT_Re_all_Gr_all_Chop_all",
      substitute(stack_choped_ECCs(PSUT_Re_all_Gr_all))
    ),


    ##############################
    # Calculate PFU aggregations #
    ##############################

    # Primary aggregates
    targets::tar_target_raw(
      "PSUT_Re_all_Gr_all_Chop_all_St_p",
      substitute(PSUT_Re_all_Gr_all_Chop_all %>%
                   calculate_primary_aggregates(countries = CountriesContinentsWorld,
                                                years = Years,
                                                p_industries = unlist(PIndustryPrefixes))),
      pattern = quote(cross(CountriesContinentsWorld))
    ),

    # Net and gross final demand aggregates
    targets::tar_target_raw(
      "PSUT_Re_all_Gr_all_Chop_all_St_pfd",
      substitute(PSUT_Re_all_Gr_all_Chop_all_St_p %>%
                   calculate_finaldemand_aggregates(countries = CountriesContinentsWorld,
                                                    years = Years,
                                                    fd_sectors = unlist(FinalDemandSectors))),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    ##############################################
    # Calculate final demand sector aggregations #
    ##############################################

    targets::tar_target_raw(
      "SectorAggregations",
      substitute(PSUT_Re_all_Gr_all_Chop_all %>%
                   calculate_sector_aggregates(countries = CountriesContinentsWorld,
                                               years = Years,
                                               fd_sectors = unlist(FinalDemandSectors))),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    ####################
    # PFD efficiencies #
    ####################

    targets::tar_target_raw(
      "ETA_prep",
      substitute(PSUT_Re_all_Gr_all_Chop_all_St_pfd %>%
                   add_grossnet_column(countries = CountriesContinentsWorld,
                                       years = Years)),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    targets::tar_target_raw(
      "ETA_pfd",
      substitute(ETA_prep %>%
                   calculate_pfd_efficiencies(countries = CountriesContinentsWorld,
                                              years = Years)),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    ####################
    # PFU efficiencies #
    ####################

    targets::tar_target_raw(
      "ETA_pfu",
      substitute(ETA_pfd %>%
                   calculate_pfu_efficiencies(countries = CountriesContinentsWorld,
                                              years = Years)),
      pattern = quote(cross(CountriesContinentsWorld))
    ),


    ################
    # Save results #
    ################

    # Pin the ETA_pfu data frame
    targets::tar_target_raw(
      "ReleaseETApfu",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = ETA_pfu,
                                        targ_name = "eta_pfu",
                                        release = Release))),

    # Zip the targets cache and store it in the pipeline_caches_folder
    targets::tar_target_raw(
      "StoreCache",
      quote(PFUDatabase::stash_cache(pipeline_caches_folder = PipelineCachesFolder,
                                     cache_folder = "_targets",
                                     file_prefix = "pfu_agg_pipeline_cache_",
                                     dependency = ETA_pfu,
                                     release = Release))),

    # Write a csv file of efficiencies
    targets::tar_target_raw(
      "ReleaseETApfuCSV",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = ETA_pfu,
                                        targ_name = "eta_pfu_csv",
                                        type = "csv",
                                        release = Release)))
  )
}


