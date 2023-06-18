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

  # Avoid warnings for some target names
  Country <- NULL
  Year <- NULL
  PSUT <- NULL
  PSUT_Re_all <- NULL
  PSUT_Re_all_Chop_all_Ds_all_Gr_all <- NULL

  list(

    # Preliminary setup --------------------------------------------------------

    # Store some incoming data as targets
    # These targets are invariant across incoming psut_releases
    targets::tar_target_raw("Countries", list(countries)),
    targets::tar_target_raw("Years", list(years)),
    targets::tar_target_raw("AggregationMapsPath", aggregation_maps_path),
    targets::tar_target_raw("PinboardFolder", pipeline_releases_folder),
    targets::tar_target_raw("PipelineCachesFolder", pipeline_caches_folder),
    targets::tar_target_raw("ExcelOutputFolder", file.path(pipeline_releases_folder, "eta_pfu_excel")),
    targets::tar_target_raw("Release", release),

    # Gather the aggregation maps
    targets::tar_target_raw(
      "AggregationMaps",
      quote(load_aggregation_maps(path = AggregationMapsPath))
    ),

    # Separate the product aggregation map
    targets::tar_target_raw(
      "ProductAggMap",
      substitute(c(AggregationMaps[["ef_product_aggregation"]],
                   AggregationMaps[["eu_product_aggregation"]]))
    ),

    # Separate the industry aggregation map
    targets::tar_target_raw(
      "IndustryAggMap",
      substitute(AggregationMaps[["ef_sector_aggregation"]])
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
      quote(pins::board_folder(PinboardFolder, versioned = TRUE) |>
              pins::pin_read("psut", version = PSUTRelease) |>
              PFUPipelineTools::filter_countries_years(countries = Countries, years = Years))
    ),
    tarchetypes::tar_group_by(
      name = "PSUTbyYear",
      command = PSUT,
      Year
    ),


    # Regional aggregations ----------------------------------------------------

    targets::tar_target_raw(
      "PSUT_Re_all",
      quote(PSUTbyYear |>
              region_pipeline(continent_aggregation_map = AggregationMaps$continent_aggregation,
                              world_aggregation_map = AggregationMaps$world_aggregation,
                              continent = "Continent")),
      pattern = quote(map(PSUTbyYear))
    ),
    tarchetypes::tar_group_by(
      "PSUT_Re_all_grouped",
      PSUT_Re_all,
      Country,
    ),


    # Chopping, despecifying, and grouping -------------------------------------

    targets::tar_target_raw(
      "PSUT_Re_all_Chop_all_Ds_all_Gr_all",
      quote(PSUT_Re_all_grouped |>
              pr_in_agg_pipeline(product_agg_map = ProductAggMap,
                                 industry_agg_map = IndustryAggMap,
                                 p_industries = unlist(PIndustryPrefixes),
                                 do_chops = do_chops,
                                 method = "SVD",
                                 country = Recca::psut_cols$country,
                                 year = Recca::psut_cols$year)) ,
      pattern = quote(map(PSUT_Re_all_grouped))
    ),
    tarchetypes::tar_group_by(
      "PSUT_Re_all_Chop_all_Ds_all_Gr_all_grouped",
      PSUT_Re_all_Chop_all_Ds_all_Gr_all,
      Country
    ),


    # Product C ----------------------------------------------------------------
    # A data frame of final demand sector efficiencies

    # Final demand sector aggregates and efficiencies
    targets::tar_target_raw(
      "SectorAggEtaFU",
      quote(PSUT_Re_all_Chop_all_Ds_all_Gr_all_grouped |>
              calculate_sector_agg_eta_fu(fd_sectors = unlist(FinalDemandSectors)) |>
              PFUPipelineTools::tar_ungroup()),
      pattern = quote(map(PSUT_Re_all_Chop_all_Ds_all_Gr_all_grouped))
    ),


    targets::tar_target_raw(
      "ReleaseSectorAggEtaFU",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = SectorAggEtaFU,
                                             pin_name = "sector_agg_eta_fu",
                                             release = Release))
    ),


    # Product D ----------------------------------------------------------------
    # CSV file of final demand sector efficiencies -----------------------------

    # Pivot SectorAggEtaFU in preparation for writing .csv file ----------------
    targets::tar_target_raw(
      "PivotedSectorAggEtaFU",
      quote(pivot_for_csv(SectorAggEtaFU,
                          val_cols = c("Final", "Useful", "eta_fu")))
    ),

    targets::tar_target_raw(
      "ReleaseSectorAggEtaFUCSV",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PivotedSectorAggEtaFU,
                                             pin_name = "sector_agg_eta_fu_csv",
                                             type = "csv",
                                             release = Release))
    ),


    # Product E ----------------------------------------------------------------
    # Pin the EtaPFU data frame

    # PFU aggregates and efficiencies
    targets::tar_target_raw(
      "AggEtaPFU",
      quote(PSUT_Re_all_Chop_all_Ds_all_Gr_all_grouped |>
              efficiency_pipeline(p_industries = unlist(PIndustryPrefixes),
                                  fd_sectors = unlist(FinalDemandSectors)) |>
              PFUPipelineTools::tar_ungroup()),
      pattern = quote(map(PSUT_Re_all_Chop_all_Ds_all_Gr_all_grouped))
    ),

    targets::tar_target_raw(
      "ReleaseAggEtaPFU",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = AggEtaPFU,
                                             pin_name = "agg_eta_pfu",
                                             release = Release))
    ),


    # Product F ----------------------------------------------------------------
    # Write a CSV file of PFU efficiencies

    # Pivot AggEtaPFU in preparation for writing .csv file
    targets::tar_target_raw(
      "PivotedAggEtaPFU",
      quote(AggEtaPFU %>%
              pivot_for_csv(val_cols = c("EX.p", "EX.f", "EX.u", "eta_pf", "eta_fu", "eta_pu")))
    ),


    targets::tar_target_raw(
      "ReleaseAggEtaPFUCSV",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PivotedAggEtaPFU,
                                             pin_name = "agg_eta_pfu_csv",
                                             type = "csv",
                                             release = Release))),










    # Code below here does chops for the World only.
    # This is temporary code that can be deleted when
    # chops are working for all countries.

    # PSUT_Re_World ------------------------------------------------------------

    targets::tar_target_raw(
      "PSUT_Re_World",
      quote(PSUT_Re_all |>
              dplyr::filter(Country == "World"))
    ),


    # PSUT_Re_World_Chop_all_Ds_all_Gr_all -------------------------------------

    targets::tar_target_raw(
      "PSUT_Re_World_Chop_all_Ds_all_Gr_all",
      quote(PSUT_Re_World |>
              pr_in_agg_pipeline(product_agg_map = ProductAggMap,
                                 industry_agg_map = IndustryAggMap,
                                 p_industries = unlist(PIndustryPrefixes),
                                 do_chops = TRUE,
                                 method = "SVD",
                                 country = Recca::psut_cols$country,
                                 year = Recca::psut_cols$year))
    ),


    # Product G ----------------------------------------------------------------
    # World sector agg eta with chops
    targets::tar_target_raw(
      "SectorAggEtaFUWorld",
      quote(PSUT_Re_World_Chop_all_Ds_all_Gr_all |>
              calculate_sector_agg_eta_fu(fd_sectors = unlist(FinalDemandSectors)) |>
              PFUPipelineTools::tar_ungroup())
    ),

    targets::tar_target_raw(
      "ReleaseSectorAggEtaFUWorld",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = SectorAggEtaFUWorld,
                                             pin_name = "sector_agg_eta_fu_world",
                                             release = Release))),


    # Product H ----------------------------------------------------------------
    # World PFU aggregates and efficiencies with chops -------------------------

    targets::tar_target_raw(
      "AggEtaPFUWorld",
      quote(PSUT_Re_World_Chop_all_Ds_all_Gr_all |>
              efficiency_pipeline(p_industries = unlist(PIndustryPrefixes),
                                  fd_sectors = unlist(FinalDemandSectors)) |>
              PFUPipelineTools::tar_ungroup())
    ),

    targets::tar_target_raw(
      "ReleaseAggEtaPFUWorld",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = AggEtaPFUWorld,
                                             pin_name = "agg_eta_pfu_world",
                                             release = Release))),


    # Zip the cache and store in the pipeline_caches_folder --------------------

    targets::tar_target_raw(
      "StoreCache",
      quote(PFUPipelineTools::stash_cache(pipeline_caches_folder = PipelineCachesFolder,
                                          cache_folder = "_targets",
                                          file_prefix = "pfu_agg_pipeline_cache_",
                                          dependency = c(ReleaseSectorAggEtaFU,      # Product C
                                                         ReleaseSectorAggEtaFUCSV,   # Product D
                                                         ReleaseAggEtaPFU,           # Product E
                                                         ReleaseAggEtaPFUCSV,        # Product F
                                                         ReleaseSectorAggEtaFUWorld, # Product G
                                                         ReleaseAggEtaPFUWorld),     # Product H
                                          release = Release))
    )
  )
}


