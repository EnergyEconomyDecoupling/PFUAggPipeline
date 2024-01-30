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
#' @param psut_without_neu_release The release we'll use from `pipeline_releases_folder`.
#'                                 See details.
#' @param phi_vecs_release The release we'll use from `pipeline_releases_folder`.
#'                         See details.
#' @param Y_fu_U_EIOU_details_release The release we'll use from `pipeline_releases_folder`.
#'                                    See details.
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
                         psut_without_neu_release,
                         phi_vecs_release,
                         Y_fu_U_EIOU_fu_details_release,
                         aggregation_maps_path,
                         pipeline_releases_folder,
                         pipeline_caches_folder,
                         release = FALSE) {

  # Avoid warnings for some target names
  Country <- NULL
  Year <- NULL
  PSUT <- NULL
  PSUTWithoutNEU <- NULL
  PSUT_Re_all <- NULL
  PSUTWithoutNEU_Re_all <- NULL
  PSUT_Re_all_Chop_all_Ds_all_Gr_all <- NULL
  PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all <- NULL
  PSUT_Re_World <- NULL

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

    # Identify the regions to which we'll aggregate
    targets::tar_target_raw(
      "Regions",
      substitute(names(AggregationMaps$region_aggregation))
    ),

    # Identify the continents to which we'll aggregate
    targets::tar_target_raw(
      "Continents",
      substitute(AggregationMaps$world_aggregation$World)
    ),

    targets::tar_target_raw(
      "CountriesRegionsContinentsWorld",
      substitute(c(Countries, Regions, Continents, "World"))
    ),

    # Set the pin and release as targets
    targets::tar_target_raw(
      "PSUTRelease",
      unname(psut_release)
    ),
    targets::tar_target_raw(
      "PhivecsRelease",
      unname(phi_vecs_release)
    ),
    targets::tar_target_raw(
      "PSUTWithoutNEURelease",
      unname(psut_without_neu_release)
    ),
    targets::tar_target_raw(
      "YfuUEIOUfuDetailsRelease",
      unname(Y_fu_U_EIOU_fu_details_release)
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

    # Phivecs ------------------------------------------------------------------

    # Pull in the phi_vecs data frame
    targets::tar_target_raw(
      "Phivecs",
      quote(pins::board_folder(PinboardFolder, versioned = TRUE) |>
              pins::pin_read("phi_vecs", version = PhivecsRelease) |>
              PFUPipelineTools::filter_countries_years(countries = Countries, years = Years))
    ),

    # PSUTWithoutNEU -----------------------------------------------------------

    # Pull in the PSUTWithoutNEU data frame
    targets::tar_target_raw(
      "PSUTWithoutNEU",
      quote(pins::board_folder(PinboardFolder, versioned = TRUE) |>
              pins::pin_read("psut_without_neu", version = PSUTWithoutNEURelease) |>
              PFUPipelineTools::filter_countries_years(countries = Countries, years = Years))
    ),
    tarchetypes::tar_group_by(
      name = "PSUTWithoutNEUbyYear",
      command = PSUTWithoutNEU,
      Year
    ),

    # Y_fu_U_EIOU_fu_details ---------------------------------------------------------------------

    # Pull in the PSUT data frame
    targets::tar_target_raw(
      "YfuUEIOUfuDetails",
      quote(pins::board_folder(PinboardFolder, versioned = TRUE) |>
              pins::pin_read("Y_fu_U_EIOU_fu_details", version = YfuUEIOUfuDetailsRelease) |>
              PFUPipelineTools::filter_countries_years(countries = Countries, years = Years))
    ),
    tarchetypes::tar_group_by(
      name = "YfuUEIOUfuDetailsbyYear",
      command = YfuUEIOUfuDetails,
      Year
    ),


    # Regional aggregations ----------------------------------------------------

    targets::tar_target_raw(
      "PSUT_Re_all",
      quote(PSUTbyYear |>
              region_pipeline(region_aggregation_map = AggregationMaps$region_aggregation,
                              continent_aggregation_map = AggregationMaps$continent_aggregation,
                              world_aggregation_map = AggregationMaps$world_aggregation)),
      pattern = quote(map(PSUTbyYear))
    ),
    tarchetypes::tar_group_by(
      "PSUT_Re_all_grouped",
      PSUT_Re_all,
      Country,
    ),

    targets::tar_target_raw(
      "PSUTWithoutNEU_Re_all",
      quote(PSUTWithoutNEUbyYear |>
              region_pipeline(region_aggregation_map = AggregationMaps$region_aggregation,
                              continent_aggregation_map = AggregationMaps$continent_aggregation,
                              world_aggregation_map = AggregationMaps$world_aggregation,
                              continent = "Continent")),
      pattern = quote(map(PSUTWithoutNEUbyYear))
    ),
    tarchetypes::tar_group_by(
      "PSUTWithoutNEU_Re_all_grouped",
      PSUTWithoutNEU_Re_all,
      Country,
    ),


    # Product Agg-E ------------------------------------------------------------
    # A data frame of PSUT matrices (including NEU) with all aggregations

    # Chopping, despecifying, and grouping
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
    targets::tar_target_raw(
      "ReleasePSUT_Re_all_Chop_all_Ds_all_Gr_all",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PSUT_Re_all_Chop_all_Ds_all_Gr_all,
                                             pin_name = "psut_re_all_chop_all_ds_all_gr_all",
                                             release = Release))),

    # Product Agg-F ------------------------------------------------------------
    # A data frame of PSUT matrices (excluding NEU) with all aggregations

    # Chopping, despecifying, and grouping
    targets::tar_target_raw(
      "PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all",
      quote(PSUTWithoutNEU_Re_all_grouped |>
              pr_in_agg_pipeline(product_agg_map = ProductAggMap,
                                 industry_agg_map = IndustryAggMap,
                                 p_industries = unlist(PIndustryPrefixes),
                                 do_chops = do_chops,
                                 method = "SVD",
                                 country = Recca::psut_cols$country,
                                 year = Recca::psut_cols$year)) ,
      pattern = quote(map(PSUTWithoutNEU_Re_all_grouped))
    ),
    tarchetypes::tar_group_by(
      "PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all_grouped",
      PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all,
      Country
    ),
    targets::tar_target_raw(
      "ReleasePSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all,
                                             pin_name = "psut_without_neu_re_all_chop_all_ds_all_gr_all",
                                             release = Release))),


    # Product Agg-A ------------------------------------------------------------
    # A data frame of final demand sector efficiencies -------------------------

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


    # Product Agg-B ------------------------------------------------------------
    # CSV file of final demand sector efficiencies -----------------------------

    # Pivot SectorAggEtaFU in preparation for writing .csv file ----------------
    targets::tar_target_raw(
      "PivotedSectorAggEtaFU",
      quote(SectorAggEtaFU |>
              # Release only exergy data for people who do not have access to IEA WEEB
              dplyr::filter(Energy.type == "X") |>
              pivot_for_csv(val_cols = c("Final", "Useful", "eta_fu")))
    ),
    targets::tar_target_raw(
      "ReleaseSectorAggEtaFUCSV",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PivotedSectorAggEtaFU,
                                             pin_name = "sector_agg_eta_fu_csv",
                                             type = "csv",
                                             release = Release))
    ),


    # Product Agg-C ------------------------------------------------------------
    # Pin the EtaPFU data frame

    # PFU aggregates and efficiencies
    targets::tar_target_raw(
      "AggEtaPFU",
      quote(PSUT_Re_all_Chop_all_Ds_all_Gr_all_grouped |>
              Recca::calc_agg_eta_pfus(p_industries = unlist(PIndustryPrefixes),
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


    # Product Agg-D ------------------------------------------------------------
    # Write a CSV file of PFU efficiencies

    # Pivot AggEtaPFU in preparation for writing .csv file
    targets::tar_target_raw(
      "PivotedAggEtaPFU",
      quote(AggEtaPFU |>
              # Release only exergy data for people who do not have access to IEA WEEB
              dplyr::filter(Energy.type == "X") |>
              pivot_for_csv(val_cols = c("EX.p", "EX.f", "EX.u", "eta_pf", "eta_fu", "eta_pu")))
    ),
    targets::tar_target_raw(
      "ReleaseAggEtaPFUCSV",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PivotedAggEtaPFU,
                                             pin_name = "agg_eta_pfu_csv",
                                             type = "csv",
                                             release = Release))),

    # Product Agg-G ------------------------------------------------------------
    # Pin the EtaPFUWithoutNEU data frame

    targets::tar_target_raw(
      "AggEtaPFUWithoutNEU",
      quote(PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all_grouped |>
              Recca::calc_agg_eta_pfus(p_industries = unlist(PIndustryPrefixes),
                                       fd_sectors = unlist(FinalDemandSectors)) |>
              PFUPipelineTools::tar_ungroup()),
      pattern = quote(map(PSUTWithoutNEU_Re_all_Chop_all_Ds_all_Gr_all_grouped))
    ),
    targets::tar_target_raw(
      "ReleaseAggEtaPFUWithoutNEU",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = AggEtaPFUWithoutNEU,
                                             pin_name = "agg_eta_pfu_without_neu",
                                             release = Release))
    ),


    # Product Agg-H ------------------------------------------------------------
    # Write a CSV file of PFU efficiencies

    # Pivot AggEtaPFUWithoutNEU in preparation for writing .csv file
    targets::tar_target_raw(
      "PivotedAggEtaPFUWithoutNEU",
      quote(AggEtaPFUWithoutNEU |>
              # Release only exergy data for people who do not have access to IEA WEEB
              dplyr::filter(Energy.type == "X") |>
              pivot_for_csv(val_cols = c("EX.p", "EX.f", "EX.u", "eta_pf", "eta_fu", "eta_pu")))
    ),
    targets::tar_target_raw(
      "ReleaseAggEtaPFUWithoutNEUCSV",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = PivotedAggEtaPFUWithoutNEU,
                                             pin_name = "agg_eta_pfu_without_neu_csv",
                                             type = "csv",
                                             release = Release))),


    targets::tar_target_raw(
      "YfuUEIOUfudetails_Re_all",
      quote(YfuUEIOUfuDetailsbyYear |>
              region_pipeline(region_aggregation_map = AggregationMaps$region_aggregation,
                              continent_aggregation_map = AggregationMaps$continent_aggregation,
                              world_aggregation_map = AggregationMaps$world_aggregation) |>
              PFUPipelineTools::tar_ungroup()),
      pattern = quote(map(YfuUEIOUfuDetailsbyYear))
    ),
    targets::tar_target_raw(
      "ReleaseYfuUEIOUfudetails_Re_all",
      quote(PFUPipelineTools::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = YfuUEIOUfudetails_Re_all,
                                             pin_name = "Y_fu_U_EIOU_fu_details_Re_all",
                                             release = Release))
    )


    # Zip the cache and store in the pipeline_caches_folder --------------------

    # targets::tar_target_raw(
    #   "StoreCache",
    #   quote(PFUPipelineTools::stash_cache(pipeline_caches_folder = PipelineCachesFolder,
    #                                       cache_folder = "_targets",
    #                                       file_prefix = "pfu_agg_pipeline_cache_",
    #                                       dependency = c(ReleasePSUT_Re_all_Chop_all_Ds_all_Gr_all, # The RUVY matrices for ECCs
    #                                                      ReleaseSectorAggEtaFU,                     # Product C
    #                                                      ReleaseSectorAggEtaFUCSV,                  # Product D
    #                                                      ReleaseAggEtaPFU,                          # Product E
    #                                                      ReleaseAggEtaPFUCSV,                       # Product F
    #                                                      ReleaseSectorAggEtaFUWorld,                # Product G
    #                                                      ReleaseAggEtaPFUWorld),                    # Product H
    #                                       release = Release))
    # )
  )
}


