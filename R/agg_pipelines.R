#' Create a targets workflow
#'
#' Arguments to this function specify the details of a targets workflow to be executed.
#'
#' The exemplar table is assumed to be an Excel file with the following columns:
#' "Region.code" and years (as numbers).
#' The body of the table should contain 3-letter codes
#' of countries.
#' The exemplar table is assumed to be on the "exemplar_table" tab of the Excel file.
#'
#' @param countries A string vector of 3-letter country codes.
#'                  Default is "all", meaning all available countries should be analyzed.
#' @param years A numeric vector of years to be analyzed.
#'              Default is "all", meaning all available years should be analyzed.
#' @param psut_release The release we'll use from `psut_releases_folder`.
#' @param aggregation_maps_path The path to the Excel file of aggregation maps.
#' @param pipeline_caches_folder The path to a folder where .zip files of the targets pipeline are saved.
#' @param pipeline_releases_folder The path to a folder where releases of output targets are pinned.
#' @param release Boolean that tells whether to do a release of the results.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
get_pipeline <- function(countries = "all",
                         years = "all",
                         psut_release,
                         aggregation_maps_path,
                         pipeline_caches_folder,
                         pipeline_releases_folder,
                         release = FALSE) {

  # Avoid notes when checking the package.
  PSUT_with_continent_col <- NULL
  PSUT <- NULL
  AggregationMaps <- NULL
  # Continent <- NULL
  .continent_col <- "Continent"
  PSUT_Re_continents <- NULL
  PSUT_Re_world <- NULL
  PSUT_Re_all_by_country <- NULL
  PSUT_Re_all <- NULL
  Country <- NULL
  PSUT_Re_all_St_p <- NULL
  p_industry_prefixes <- NULL
  PSUT_Re_all_St_fu <- NULL
  final_demand_sectors <- NULL
  PSUT_Re_all_St_pfu_by_country <- NULL
  PSUT_Re_all_St_pfu <- NULL
  agg_eta_Re_all_St_pfu <- NULL
  agg_Re_all_St_pfu <- NULL
  pin_agg_csv <- NULL
  PinboardFolder <- NULL
  pin_eta_csv <- NULL
  store_cache <- NULL
  PipelineCachesOutputFolder <- NULL
  eta_Re_all_St_pfu <- NULL
  map <- NULL


  # Create the pipeline
  list(

    #####################
    # Preliminary setup #
    #####################

    # Store some incoming data as targets
    targets::tar_target_raw("Countries", rlang::enexpr(countries)),
    targets::tar_target_raw("Years", rlang::enexpr(years)),
    targets::tar_target_raw("PSUTRelease", psut_release),
    targets::tar_target_raw("AggregationMapsPath", aggregation_maps_path),
    targets::tar_target_raw("PipelineCachesOutputFolder", pipeline_caches_folder),
    targets::tar_target_raw("PinboardFolder", pipeline_releases_folder),

    # Pull in the PSUT data frame
    targets::tar_target_raw("PSUT", quote(pins::board_folder(PinboardFolder, versioned = TRUE) %>%
                                            pins::pin_read("psut", version = PSUTRelease) %>%
                                            filter_countries_and_years(countries = Countries, years = Years)),
                            # Very important to assign storage and retrieval tasks to workers,
                            # else the pipeline seemingly never finishes.
                            storage = "worker",
                            retrieval = "worker"),

    # Gather the aggregation maps.
    targets::tar_target_raw("AggregationMaps", quote(load_aggregation_maps(path = AggregationMapsPath))),


    #########################
    # Regional aggregations #
    #########################

    # Create a continents data frame, grouped by continent,
    # so subsequent operations (region aggregation)
    # will be performed in parallel, if desired.
    tarchetypes::tar_group_by(
      name = PSUT_with_continent_col,
      command = join_psut_continents(PSUT = PSUT,
                                     continent_aggregation_map = AggregationMaps$continent_aggregation,
                                     continent = "Continent"),
      # The columns to group by, as symbols.
      Continent,
      storage = "worker",
      retrieval = "worker"
    ),

    # Aggregate by continent
    targets::tar_target_raw(
      "PSUT_Re_continents",
      quote(Recca::region_aggregates(PSUT_with_continent_col,
                                     many_colname = IEATools::iea_cols$country,
                                     few_colname = "Continent") %>%
              # Eliminate the targets grouping on Continent
              # so we can group by country later.
              dplyr::mutate(
                tar_group = NULL
              )),
      pattern = quote(map(PSUT_with_continent_col)),
      iteration = "group",
      storage = "worker",
      retrieval = "worker"
    ),


    # Aggregate to world (WLD)
    targets::tar_target(
      PSUT_Re_world,
      Recca::region_aggregates(PSUT_Re_continents %>%
                                 dplyr::left_join(AggregationMaps$world_aggregation %>%
                                                    matsbyname::agg_map_to_agg_table(many_colname = IEATools::iea_cols$country,
                                                                                     few_colname = "World"),
                                                  by = IEATools::iea_cols$country),
                               many_colname = IEATools::iea_cols$country, # Which actually holds continents
                               few_colname = "World")
    ),

    # Bind all region aggregations together
    targets::tar_target_raw("PSUT_Re_all", quote(dplyr::bind_rows(PSUT, PSUT_Re_continents, PSUT_Re_world))),


    ####################
    # PFU aggregations #
    ####################

    # Set up a grouped-by-country data frame
    # so all future calculations are parallelized across countries.
    tarchetypes::tar_group_by(
      name = PSUT_Re_all_by_country,
      command = PSUT_Re_all,
      # The columns to group by, as symbols.
      Country,
      storage = "worker",
      retrieval = "worker"
    ),

    # Establish prefixes for primary industries
    targets::tar_target_raw("p_industry_prefixes", quote(IEATools::tpes_flows %>% unname() %>% unlist() %>% list())),

    # Aggregate primary energy/exergy by total (total energy supply (TES)), product, and flow
    targets::tar_target(
      PSUT_Re_all_St_p,
      calculate_primary_ex_data(PSUT_Re_all_by_country,
                                p_industry_prefixes = p_industry_prefixes),
      pattern = map(PSUT_Re_all_by_country),
      iteration = "group",
      storage = "worker",
      retrieval = "worker"
    ),

    # Establish final demand sectors
    targets::tar_target_raw("final_demand_sectors", quote(IEATools::fd_sectors)),

    # Aggregate final and useful energy/exergy by total (total final consumption (TFC)), product, and sector
    targets::tar_target(
      name = PSUT_Re_all_St_fu,
      command = calculate_finaluseful_ex_data(PSUT_Re_all_by_country,
                                              fd_sectors = final_demand_sectors),
      pattern = map(PSUT_Re_all_by_country),
      iteration = "group",
      storage = "worker",
      retrieval = "worker"
    ),

    # Bring the aggregations together in a single data frame
    targets::tar_target_raw("PSUT_Re_all_St_pfu", quote(dplyr::bind_rows(PSUT_Re_all_St_p, PSUT_Re_all_St_fu))),


    ################
    # Efficiencies #
    ################

    tarchetypes::tar_group_by(
      name = PSUT_Re_all_St_pfu_by_country,
      command = PSUT_Re_all_St_pfu %>%
        dplyr::mutate(
          tar_group = NULL
        ),
      # The columns to group by, as symbols.
      Country,
      storage = "worker",
      retrieval = "worker"
    ),

    targets::tar_target(
      name = agg_eta_Re_all_St_pfu,
      command = calc_agg_etas(PSUT_Re_all_St_pfu_by_country),
      pattern = map(PSUT_Re_all_St_pfu_by_country),
      iteration = "group",
      storage = "worker",
      retrieval = "worker"
    ),

    # Split the aggregations and efficiencies apart
    # to enable easier saving of separate .csv files later.
    targets::tar_target(
      name = agg_Re_all_St_pfu,
      command = agg_eta_Re_all_St_pfu %>%
        dplyr::mutate(
          "{PFUAggDatabase::efficiency_cols$eta_pf}" := NULL,
          "{PFUAggDatabase::efficiency_cols$eta_fu}" := NULL,
          "{PFUAggDatabase::efficiency_cols$eta_pu}" := NULL,
        ),
      storage = "worker",
      retrieval = "worker"
    ),

    targets::tar_target(
      name = eta_Re_all_St_pfu,
      command = agg_eta_Re_all_St_pfu %>%
        dplyr::mutate(
          "{IEATools::all_stages$primary}" := NULL,
          "{IEATools::all_stages$final}" := NULL,
          "{IEATools::all_stages$useful}" := NULL,
        ),
      storage = "worker",
      retrieval = "worker"
    ),


    ################
    # Save results #
    ################

    # Pin the aggregates and efficiencies as an .rds file
    targets::tar_target_raw("pin_agg_eta_Re_all_St_pfu", quote(PFUWorkflow::release_target(pipeline_releases_folder = PinboardFolder,
                                                                                  targ = agg_eta_Re_all_St_pfu,
                                                                                  targ_name = "agg_eta_Re_all_St_pfu",
                                                                                  release = release))),

    # Pin aggregates as a wide-by-years .csv file
    targets::tar_target(
      pin_agg_csv,
      PFUWorkflow::release_target(pipeline_releases_folder = PinboardFolder,
                                  targ = agg_Re_all_St_pfu%>%
                                    pivot_agg_eta_wide_by_year(pivot_cols = c(IEATools::all_stages$primary,
                                                                              IEATools::all_stages$final,
                                                                              IEATools::all_stages$useful)),
                                  targ_name = "agg_Re_all_St_pfu",
                                  type = "csv",
                                  release = release)
    ),


    # Pin efficiencies as a wide-by-years .csv file
    targets::tar_target(
      pin_eta_csv,
      PFUWorkflow::release_target(pipeline_releases_folder = PinboardFolder,
                                  targ = eta_Re_all_St_pfu %>%
                                    pivot_agg_eta_wide_by_year(pivot_cols = c(PFUAggDatabase::efficiency_cols$eta_pf,
                                                                              PFUAggDatabase::efficiency_cols$eta_fu,
                                                                              PFUAggDatabase::efficiency_cols$eta_pu)),
                                  targ_name = "eta_Re_all_St_pfu",
                                  type = "csv",
                                  release = release)
    ),

    # Save the cache for posterity.
    targets::tar_target(
      store_cache,
      PFUWorkflow::stash_cache(pipeline_caches_folder = PipelineCachesOutputFolder,
                               cache_folder = "_targets",
                               file_prefix = "pfu_agg_workflow_cache_",
                               dependency = c(agg_Re_all_St_pfu, eta_Re_all_St_pfu))
    )
  )
}

