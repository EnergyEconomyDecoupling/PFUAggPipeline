#' Create a targets workflow
#'
#' This is a target factory whose arguments
#' specify the details of a targets workflow to be constructed
#'
#' @param countries A string vector of 3-letter country codes.
#'                  Default is "all", meaning all available countries should be analyzed.
#' @param years A numeric vector of years to be analyzed.
#'              Default is "all", meaning all available years should be analyzed.
#' @param do_chops A boolean that tells whether to perform the R and Y chops.
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
                         do_chops,
                         psut_release,
                         aggregation_maps_path,
                         pipeline_releases_folder,
                         pipeline_caches_folder,
                         release = FALSE) {

  list(

    # Preliminary setup --------------------------------------------------------

    # Store some incoming data as targets.
    # These targets are invariant across incoming psut_releases.
    targets::tar_target_raw("Countries", list(countries)),
    targets::tar_target_raw("Years", list(years)),
    targets::tar_target_raw("AggregationMapsPath", aggregation_maps_path),
    targets::tar_target_raw("PinboardFolder", pipeline_releases_folder),
    targets::tar_target_raw("PipelineCachesFolder", pipeline_caches_folder),
    targets::tar_target_raw("ExcelOutputFolder", file.path(pipeline_releases_folder, "eta_pfu_excel")),
    targets::tar_target_raw("Release", release),

    # Gather the aggregation maps.
    targets::tar_target_raw(
      "AggregationMaps",
      quote(load_aggregation_maps(path = AggregationMapsPath))
    ),

    # Establish prefixes for primary industries
    targets::tar_target_raw(
      "PIndustryPrefixes",
      quote(IEATools::tpes_flows %>% unname() %>% unlist() %>% list())
    ),

    # Establish final demand sectors
    targets::tar_target_raw(
      "FinalDemandSectors",
      quote(create_fd_sectors_list(IEATools::fd_sectors, AggregationMaps$ef_sector_aggregation))
    ),

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
      unname(psut_release)
    ),


    # PSUT ---------------------------------------------------------------------

    # Pull in the PSUT data frame
    targets::tar_target_raw(
      "PSUT",
      substitute(pins::board_folder(PinboardFolder, versioned = TRUE) %>%
                   pins::pin_read("psut", version = PSUTRelease) %>%
                   PFUDatabase::filter_countries_years(countries = Countries, years = Years))
    ),

    # Filter to the US for Carey King
    targets::tar_target_raw(
      "PSUT_USA",
      substitute(PSUT %>%
                   dplyr::filter(Country == "USA"))
    ),

    # --------------------------------------------------------------------------
    # Product A ----------------------------------------------------------------
    # --------------------------------------------------------------------------
    # Pin the PSUT_USA data frame ----------------------------------------------

    targets::tar_target_raw(
      "ReleasePSUT_USA",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = PSUT_USA,
                                        pin_name = "psut_usa",
                                        release = Release))
    ),


    # Regional aggregations ----------------------------------------------------

    # Create a continents data frame, grouped by continent,
    # so subsequent operations (region aggregation)
    # will be performed in parallel, if desired.

    targets::tar_target_raw(
      "PSUT_Re_all",
      substitute(region_pipeline(PSUT,
                                 continent_aggregation_map = AggregationMaps$continent_aggregation,
                                 world_aggregation_map = AggregationMaps$world_aggregation,
                                 continent = "Continent"))
    ),


    # Chopping, despecifying, and grouping -------------------------------------

    targets::tar_target_raw(
      "ProductAggMap",
      substitute(c(AggregationMaps[["ef_product_aggregation"]],
                   AggregationMaps[["eu_product_aggregation"]]))
    ),


    targets::tar_target_raw(
      "IndustryAggMap",
      substitute(AggregationMaps[["ef_sector_aggregation"]])
    ),


    targets::tar_target_raw(
      "PSUT_Re_all_Chop_all_Ds_all_Gr_all",
      substitute(PSUT_Re_all |>
                   row_col_agg_pipeline(countries = CountriesContinentsWorld,
                                        years = Years,
                                        do_chops = do_chops,
                                        method = "SVD"))
    ),

    # PFU aggregates and efficiencies ------------------------------------------

    targets::tar_target_raw(
      "AggEtaPFU",
      substitute(PSUT_Re_all_Chop_all_Ds_all_Gr_all |>
                   efficiency_pipeline(countries = CountriesContinentsWorld,
                                       years = Years,
                                       do_chops = do_chops,
                                       method = "SVD"))
    ),


    # --------------------------------------------------------------------------
    # Product D ----------------------------------------------------------------
    # --------------------------------------------------------------------------
    # Pin the EtaPFU data frame ------------------------------------------------

    targets::tar_target_raw(
      "ReleaseAggEtaPFU",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = AggEtaPFU,
                                        pin_name = "agg_eta_pfu",
                                        release = Release))
    ),


    # Pivot AggEtaPFU in preparation for writing .csv file ---------------------

    targets::tar_target_raw(
      "PivotedAggEtaPFU",
      substitute(AggEtaPFU %>%
                   pivot_for_csv(val_cols = c("EX.p", "EX.f", "EX.u", "eta_pf", "eta_fu", "eta_pu")))
    ),


    # --------------------------------------------------------------------------
    # Product E ----------------------------------------------------------------
    # --------------------------------------------------------------------------
    # Write a CSV file of PFU efficiencies -------------------------------------

    targets::tar_target_raw(
      "ReleaseAggEtaPFUCSV",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = PivotedAggEtaPFU,
                                        pin_name = "agg_eta_pfu_csv",
                                        type = "csv",
                                        release = Release))),


    # Zip the cache and store in the pipeline_caches_folder --------------------

    targets::tar_target_raw(
      "StoreCache",
      quote(PFUDatabase::stash_cache(pipeline_caches_folder = PipelineCachesFolder,
                                     cache_folder = "_targets",
                                     file_prefix = "pfu_agg_pipeline_cache_",
                                     dependency = EtaPFU,
                                     release = Release))
    )



















    # # Regional aggregations ----------------------------------------------------
    #
    # # Create a continents data frame, grouped by continent,
    # # so subsequent operations (region aggregation)
    # # will be performed in parallel, if desired.
    #
    # targets::tar_target_raw(
    #   "PSUT_with_continent_col",
    #   substitute(join_psut_continents(PSUT = PSUT,
    #                                   years = Years,
    #                                   continent_aggregation_map = AggregationMaps$continent_aggregation,
    #                                   continent = "Continent")),
    #   pattern = quote(cross(Years))
    # ),
    #
    # # Aggregate by continent
    # targets::tar_target_raw(
    #   "PSUT_Re_continents",
    #   substitute(continent_aggregation(PSUT_with_continent_col,
    #                                    continents = Continents,
    #                                    years = Years,
    #                                    many_colname = IEATools::iea_cols$country,
    #                                    few_colname = "Continent")),
    #   pattern = quote(cross(Continents))
    # ),
    #
    # # Aggregate continents to world
    # targets::tar_target_raw(
    #   "PSUT_Re_world",
    #   substitute(world_aggregation(PSUT_Chop_all_Re_continents,
    #                                years = Years,
    #                                world_aggregation_map = AggregationMaps$world_aggregation)),
    #   pattern = quote(cross(Years))
    # ),
    #
    # # Stack all region aggregations together
    # targets::tar_target_raw(
    #   "PSUT_Re_all",
    #   substitute(dplyr::bind_rows(PSUT,
    #                               PSUT_Re_continents,
    #                               PSUT_Re_world))
    # ),




    # # Despecified aggregations -------------------------------------------------
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_PrIn",
    #   substitute(PSUT_Chop_all_Re_all %>%
    #                despecified_aggregations(countries = CountriesContinentsWorld,
    #                                         years = Years,
    #                                         notation = list(RCLabels::bracket_notation,
    #                                                         RCLabels::arrow_notation))),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all",
    #   substitute(stack_despecification_aggregations(specified_df = PSUT_Chop_all_Re_all,
    #                                                 # Ds_Pr = PSUT_Chop_all_Re_all_Ds_Pr,
    #                                                 # Ds_In = PSUT_Chop_all_Re_all_Ds_In,
    #                                                 Ds_PrIn = PSUT_Chop_all_Re_all_Ds_PrIn))
    # ),
    #
    #
    # # Grouped product aggregations ---------------------------------------------
    #
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all_Gr_Pr",
    #   substitute(PSUT_Chop_all_Re_all_Ds_PrIn %>%
    #                grouped_aggregations(countries = CountriesContinentsWorld,
    #                                     years = Years,
    #                                     aggregation_map = ProductAggMap,
    #                                     margin = "Product")),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    #
    # # Grouped industry aggregations --------------------------------------------
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all_Gr_In",
    #   substitute(PSUT_Chop_all_Re_all_Ds_PrIn %>%
    #                grouped_aggregations(countries = CountriesContinentsWorld,
    #                                     years = Years,
    #                                     aggregation_map = IndustryAggMap,
    #                                     margin = "Industry")),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    #
    # # Grouped product and industry aggregations --------------------------------
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all_Gr_PrIn",
    #   substitute(PSUT_Chop_all_Re_all_Ds_PrIn %>%
    #                grouped_aggregations(countries = CountriesContinentsWorld,
    #                                     years = Years,
    #                                     aggregation_map = c(ProductAggMap, IndustryAggMap),
    #                                     margin = c("Product", "Industry"))),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    #
    # # Stack product and industry groupings -------------------------------------
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all_Gr_all",
    #   substitute(stack_group_aggregations(despecified_df = PSUT_Chop_all_Re_all_Ds_all,
    #                                       Gr_Pr = PSUT_Chop_all_Re_all_Ds_all_Gr_Pr,
    #                                       Gr_In = PSUT_Chop_all_Re_all_Ds_all_Gr_In,
    #                                       Gr_PrIn = PSUT_Chop_all_Re_all_Ds_all_Gr_PrIn))
    # ),
    #
    #
    # # Primary aggregations -----------------------------------------------------
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all_Gr_all_St_p",
    #   substitute(PSUT_Chop_all_Re_all_Ds_all_Gr_all %>%
    #                calculate_primary_aggregates(countries = CountriesContinentsWorld,
    #                                             years = Years,
    #                                             p_industries = unlist(PIndustryPrefixes))),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    #
    # # Net and gross final demand aggregations ----------------------------------
    #
    # targets::tar_target_raw(
    #   "PSUT_Chop_all_Re_all_Ds_all_Gr_all_St_pfd",
    #   substitute(PSUT_Chop_all_Re_all_Ds_all_Gr_all_St_p %>%
    #                calculate_finaldemand_aggregates(countries = CountriesContinentsWorld,
    #                                                 years = Years,
    #                                                 fd_sectors = unlist(FinalDemandSectors))),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    #
    # # Final demand sector aggregates and efficiencies --------------------------
    #
    # targets::tar_target_raw(
    #   "SectorAggEtaFU",
    #   substitute(PSUT_Chop_all_Re_all_Ds_all_Gr_all %>%
    #                calculate_sector_agg_eta_fu(countries = CountriesContinentsWorld,
    #                                            years = Years,
    #                                            fd_sectors = unlist(FinalDemandSectors))),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    # # --------------------------------------------------------------------------
    # # Product B ----------------------------------------------------------------
    # # --------------------------------------------------------------------------
    # # Write a data frame of final demand sector efficiencies -------------------
    #
    # targets::tar_target_raw(
    #   "ReleaseSectorAggEtaFU",
    #   quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
    #                                     targ = SectorAggEtaFU,
    #                                     pin_name = "sector_agg_eta_fu",
    #                                     release = Release))
    # ),
    #
    #
    # # Pivot SectorAggEtaFU in preparation for writing .csv file ----------------
    #
    # targets::tar_target_raw(
    #   "PivotedSectorAggEtaFU",
    #   substitute(pivot_for_csv(SectorAggEtaFU,
    #                            val_cols = c("Final", "Useful", "eta_fu")))
    # ),
    #
    #
    # # --------------------------------------------------------------------------
    # # Product C ----------------------------------------------------------------
    # # --------------------------------------------------------------------------
    # # Write a CSV file of final demand sector efficiencies ---------------------
    #
    # targets::tar_target_raw(
    #   "ReleaseSectorAggEtaFUCSV",
    #   substitute(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
    #                                          targ = PivotedSectorAggEtaFU,
    #                                          pin_name = "sector_agg_eta_fu_csv",
    #                                          type = "csv",
    #                                          release = Release))
    # ),
    #
    #
    # # PFU aggregations ---------------------------------------------------------
    #
    # targets::tar_target_raw(
    #   "AggPFU",
    #   substitute(PSUT_Chop_all_Re_all_Ds_all_Gr_all_St_pfd %>%
    #                calculate_pfu_aggregates(countries = CountriesContinentsWorld,
    #                                         years = Years)),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),
    #
    #
    # # PFU efficiencies ---------------------------------------------------------
    #
    # targets::tar_target_raw(
    #   "AggEtaPFU",
    #   substitute(AggPFU %>%
    #                calculate_pfu_efficiencies(countries = CountriesContinentsWorld,
    #                                           years = Years)),
    #   pattern = quote(cross(CountriesContinentsWorld, Years))
    # ),


  )
}


