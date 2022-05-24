#' Create a targets workflow
#'
#' This is a target factory whose arguments
#' specify the details of a targets workflow to be executed.
#'
#' The `psut_releases` argument is assumed to be a named vector of strings where
#' names are pin names in `pipeline_releases_folder`, and
#' items give pin versions.
#' An example `psut_releases` argument is:
#'
#' `psut_releases = c(psut = "20220519T185450Z-55e04",`
#' `                  psut_iea = "20220519T185448Z-07a39",`
#' `                  psut_mw = "20220519T185235Z-771f8").`
#'
#' The "_" character has meaning in the names of `psut_releases` items.
#' All characters after the "_" are used when creating the target names for
#' aggregations and aggregate efficiencies.
#' Best _not_ to send more than one `psut_releases` item that lacks characters after the "_" character.
#'
#' @param countries A string vector of 3-letter country codes.
#'                  Default is "all", meaning all available countries should be analyzed.
#' @param years A numeric vector of years to be analyzed.
#'              Default is "all", meaning all available years should be analyzed.
#' @param psut_releases The releases we'll use from `pipeline_releases_folder`.
#'                      See details.
#' @param aggregation_maps_path The path to the Excel file of aggregation maps.
#' @param pipeline_caches_folder The path to a folder where .zip files of the targets pipeline are saved.
#' @param pipeline_releases_folder The path to a folder where releases of output targets are pinned.
#' @param release Boolean that tells whether to do a release of the results.
#'                Default is `FALSE`.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
get_pipeline <- function(countries = "all",
                         years = "all",
                         psut_releases,
                         aggregation_maps_path,
                         pipeline_caches_folder,
                         pipeline_releases_folder,
                         release = FALSE) {

  # Avoid notes when checking the package.
  PSUT_with_continent_col <- NULL
  PSUT <- NULL
  AggregationMaps <- NULL
  Continent <- NULL
  PSUT_Re_all_by_country <- NULL
  PSUT_Re_all <- NULL
  Country <- NULL
  PSUT_Re_all_St_pfu_by_country <- NULL
  PSUT_Re_all_St_pfu <- NULL

  if (length(psut_releases) > 1) {
    # Recursively call 1 at a time.
    lapply(psut_releases, function(pr) {
      get_pipeline(countries = countries,
                   years = years,
                   # Call get_pipeline() with this particular release (pr).
                   psut_releases = pr,
                   aggregation_maps_path = aggregation_maps_path,
                   pipeline_caches_folder = pipeline_caches_folder,
                   pipeline_releases_folder = pipeline_releases_folder,
                   release = release)
    }) %>%
      # Combine all pipelines into a single pipeline.
      c()
  }

  # At this point, we will have only a single, named value for psut_release.
  # Get the name (the pin).
  psut_pin <- names(psut_releases)
  # Split the pin name at an underscore to get the suffix
  agg_eta_suff <- strsplit(psut_pin, split = "_", fixed = TRUE)
  if (length(agg_eta_suff) == 1) {
    # Didn't find anything after a "_".
    agg_eta_suff <- ""
  }
  agg_eta_pref <- paste0("agg_eta", agg_eta_suff)
  agg_pref <- paste0("agg", agg_eta_suff)
  eta_pref <- paste0("eta", agg_eta_suff)

  # Set target names based on the psut_tar_str.
  aggregation_maps_tar_str <- "AggregationMaps"
  psut_tar_str <- toupper(psut_pin)
  psut_tar_str_with_continent_col <- paste0(psut_tar_str, "_with_continent_col")
  psut_tar_str_Re_continents <- paste0(psut_tar_str, "_Re_continents")
  psut_tar_str_Re_world <- paste0(psut_tar_str, "_Re_world")
  psut_tar_str_Re_all <- paste0(psut_tar_str, "_Re_all")
  psut_tar_str_Re_all_St_p <- paste0(psut_tar_str, "_Re_all_St_p")
  psut_tar_str_Re_all_St_fu <- paste0(psut_tar_str, "_Re_all_St_fu")
  psut_tar_str_Re_all_St_pfu <- paste0(psut_tar_str, "_Re_all_St_pfu")
  agg_eta_tar_str_Re_all_St_pfu <- paste0(agg_eta_pref, "_Re_all_St_pfu")
  agg_tar_str_Re_all_St_pfu <- paste0(agg_pref, "_Re_all_St_pfu")
  eta_tar_str_Re_all_St_pfu <- paste0(eta_pref, "_Re_all_St_pfu")

  # Set symbols for targets to which we refer later in the pipeline.
  aggregation_maps_tar_sym <- as.symbol(aggregation_maps_tar_str)
  psut_tar_sym <- as.symbol(psut_tar_str)
  psut_tar_sym_Re_continents <- as.symbol(psut_tar_str_Re_continents)
  psut_tar_sym_Re_world <- as.symbol(psut_tar_str_Re_world)
  psut_tar_sym_Re_all <- as.symbol(psut_tar_str_Re_all)
  psut_tar_sym_Re_all_St_p <- as.symbol(psut_tar_str_Re_all_St_p)
  psut_tar_sym_Re_all_St_fu <- as.symbol(psut_tar_str_Re_all_St_fu)
  psut_tar_sym_Re_all_St_pfu <- as.symbol(psut_tar_str_Re_all_St_pfu)
  agg_eta_tar_sym_Re_all_St_pfu <- as.symbol(agg_eta_tar_str_Re_all_St_pfu)
  agg_tar_sym_Re_all_St_pfu <- as.symbol(agg_tar_str_Re_all_St_pfu)
  eta_tar_sym_Re_all_St_pfu <- as.symbol(eta_tar_str_Re_all_St_pfu)

  # Create the pipeline
  list(

    #####################
    # Preliminary setup #
    #####################

    # Store some incoming data as targets
    # targets::tar_target_raw("Countries", rlang::enexpr(my_countries)),
    targets::tar_target_raw("Countries", list(countries)),
    targets::tar_target_raw("Years", list(years)),
    targets::tar_target_raw("PSUTPin", psut_pin),
    targets::tar_target_raw("PSUTRelease", unname(psut_release)),
    targets::tar_target_raw("AggregationMapsPath", aggregation_maps_path),
    targets::tar_target_raw("PipelineCachesOutputFolder", pipeline_caches_folder),
    targets::tar_target_raw("PinboardFolder", pipeline_releases_folder),
    targets::tar_target_raw("Release", release),

    # Note:
    # In _targets.R, storage and retrieval parameters for all targets are set to "worker",
    # eliminating the need to set those arguments in the targets below.

    # Gather the aggregation maps.
    # targets::tar_target_raw("AggregationMaps", quote(load_aggregation_maps(path = AggregationMapsPath))),
    targets::tar_target_raw(
      aggregation_maps_tar_str,
      quote(load_aggregation_maps(path = AggregationMapsPath))),

    # Pull in the PSUT data frame
    # targets::tar_target_raw("PSUT", quote(pins::board_folder(PinboardFolder, versioned = TRUE) %>%
    #                                         pins::pin_read("psut", version = PSUTRelease) %>%
    #                                         filter_countries_and_years(countries = Countries, years = Years))),
    targets::tar_target_raw(
      psut_tar_str,
      quote(pins::board_folder(PinboardFolder, versioned = TRUE) %>%
              pins::pin_read(PSUTPin, version = PSUTRelease) %>%
              filter_countries_and_years(countries = Countries, years = Years))),


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
      Continent),

    # Aggregate by continent
    targets::tar_target_raw(
      psut_tar_str_Re_continents,
      quote(Recca::region_aggregates(PSUT_with_continent_col,
                                     many_colname = IEATools::iea_cols$country,
                                     few_colname = "Continent") %>%
              # Eliminate the targets grouping on Continent
              # so we can group by country later.
              dplyr::mutate(
                tar_group = NULL
              )),
      pattern = quote(map(PSUT_with_continent_col)),
      iteration = "group"),


    # Aggregate to world
    targets::tar_target_raw(
      psut_tar_str_Re_world,
      # quote(Recca::region_aggregates(PSUT_Re_continents %>%
      #                                  dplyr::left_join(AggregationMaps$world_aggregation %>%
      #                                                     matsbyname::agg_map_to_agg_table(many_colname = IEATools::iea_cols$country,
      #                                                                                      few_colname = "World"),
      #                                                   by = IEATools::iea_cols$country),
      #                                many_colname = IEATools::iea_cols$country, # Which actually holds continents
      #                                few_colname = "World"))
      substitute(Recca::region_aggregates(psut_tar_sym_Re_continents %>%
                                            dplyr::left_join(aggregation_maps_tar_sym$world_aggregation %>%
                                                               matsbyname::agg_map_to_agg_table(many_colname = IEATools::iea_cols$country,
                                                                                                few_colname = "World"),
                                                             by = IEATools::iea_cols$country),
                                          many_colname = IEATools::iea_cols$country, # Which actually holds continents
                                          few_colname = "World"))),

    # Bind all region aggregations together
    # targets::tar_target_raw(
    #   "PSUT_Re_all",
    #   quote(dplyr::bind_rows(PSUT, PSUT_Re_continents, PSUT_Re_world))),
    targets::tar_target_raw(
      psut_tar_str_Re_all,
      substitute(dplyr::bind_rows(psut_tar_sym, psut_tar_sym_Re_continents, PSUT_Re_world))),



    ####################
    # PFU aggregations #
    ####################

    # Set up a grouped-by-country data frame
    # so all future calculations are parallelized across countries.
    # tarchetypes::tar_group_by(
    #   name = PSUT_Re_all_by_country,
    #   command = PSUT_Re_all,
    #   # The columns to group by, as symbols.
    #   Country),

    # Establish prefixes for primary industries
    targets::tar_target_raw(
      "p_industry_prefixes",
      quote(IEATools::tpes_flows %>% unname() %>% unlist() %>% list())),

    # Aggregate primary energy/exergy by total (total energy supply (TES)), product, and flow
    targets::tar_target_raw(
      psut_tar_str_Re_all_St_p,
      # quote(calculate_primary_ex_data(PSUT_Re_all_by_country,
      #                                 countries = Countries,
      #                                 years = Years,
      #                                 p_industry_prefixes = p_industry_prefixes)),
      substitute(calculate_primary_ex_data(psut_tar_sym_Re_all,
                                           countries = Countries,
                                           years = Years,
                                           p_industry_prefixes = p_industry_prefixes)),
      pattern = quote(map(Countries))),

    # Establish final demand sectors
    targets::tar_target_raw("final_demand_sectors", quote(IEATools::fd_sectors)),

    # Aggregate final and useful energy/exergy by total (total final consumption (TFC)), product, and sector
    targets::tar_target_raw(
      psut_tar_str_Re_all_St_fu,
      # quote(calculate_finaluseful_ex_data(PSUT_Re_all_by_country,
      #                                     countries = Countries,
      #                                     years = Years,
      #                                     fd_sectors = final_demand_sectors)),
      substitute(calculate_finaluseful_ex_data(psut_tar_sym_Re_all,
                                               countries = Countries,
                                               years = Years,
                                               fd_sectors = final_demand_sectors)),
      pattern = quote(map(Countries))),

    # Bring the aggregations together in a single data frame
    targets::tar_target_raw(
      psut_tar_str_Re_all_St_pfu,
      # quote(dplyr::bind_rows(PSUT_Re_all_St_p, PSUT_Re_all_St_fu))),
      substitute(dplyr::bind_rows(psut_tar_sym_Re_all_St_p, psut_tar_sym_Re_all_St_fu))),

    ################
    # Efficiencies #
    ################

    # tarchetypes::tar_group_by(
    #   name = PSUT_Re_all_St_pfu_by_country,
    #   command = PSUT_Re_all_St_pfu %>%
    #     dplyr::mutate(
    #       tar_group = NULL
    #     ),
    #   # The columns to group by, as symbols.
    #   Country),

    targets::tar_target_raw(
    #   "agg_eta_Re_all_St_pfu",
    #   quote(calc_agg_etas(PSUT_Re_all_St_pfu_by_country)),
    #   pattern = quote(map(PSUT_Re_all_St_pfu_by_country)),
    #   iteration = "group"),
      agg_eta_tar_str_Re_all_St_pfu,
      substitute(calc_agg_etas(psut_tar_sym_Re_all_St_pfu,
                               countries = Countries,
                               years = Years)),
      pattern = quote(map(Countries))),

    # Split the aggregations and efficiencies apart
    # to enable easier saving of separate .csv files later.
    targets::tar_target_raw(
      agg_tar_str_Re_all_St_pfu,
      # quote(agg_eta_Re_all_St_pfu %>%
      #         dplyr::mutate(
      #           "{PFUAggDatabase::efficiency_cols$eta_pf}" := NULL,
      #           "{PFUAggDatabase::efficiency_cols$eta_fu}" := NULL,
      #           "{PFUAggDatabase::efficiency_cols$eta_pu}" := NULL,
      #         ))),
      substitute(agg_eta_tar_sym_Re_all_St_pfu %>%
              dplyr::mutate(
                "{PFUAggDatabase::efficiency_cols$eta_pf}" := NULL,
                "{PFUAggDatabase::efficiency_cols$eta_fu}" := NULL,
                "{PFUAggDatabase::efficiency_cols$eta_pu}" := NULL,
              ))),

    targets::tar_target_raw(
      eta_tar_str_Re_all_St_pfu,
      # quote(agg_eta_Re_all_St_pfu %>%
      #         dplyr::mutate(
      #           "{IEATools::all_stages$primary}" := NULL,
      #           "{IEATools::all_stages$final}" := NULL,
      #           "{IEATools::all_stages$useful}" := NULL,
      #         ))),
      substitute(agg_eta_tar_sym_Re_all_St_pfu %>%
              dplyr::mutate(
                "{IEATools::all_stages$primary}" := NULL,
                "{IEATools::all_stages$final}" := NULL,
                "{IEATools::all_stages$useful}" := NULL,
              ))),


    ################
    # Save results #
    ################

    # Pin the aggregates and efficiencies as an .rds file
    targets::tar_target_raw(
      "pin_agg_eta_Re_all_St_pfu",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = agg_eta_Re_all_St_pfu,
                                        targ_name = "agg_eta_Re_all_St_pfu",
                                        release = Release))
      ),

    # Pin aggregates as a wide-by-years .csv file
    targets::tar_target_raw(
      "pin_agg_csv",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = agg_Re_all_St_pfu%>%
                                          pivot_agg_eta_wide_by_year(pivot_cols = c(IEATools::all_stages$primary,
                                                                                    IEATools::all_stages$final,
                                                                                    IEATools::all_stages$useful)),
                                        targ_name = "agg_Re_all_St_pfu",
                                        type = "csv",
                                        release = Release))
    ),


    # Pin efficiencies as a wide-by-years .csv file
    targets::tar_target_raw(
      "pin_eta_csv",
      quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                        targ = eta_Re_all_St_pfu %>%
                                          pivot_agg_eta_wide_by_year(pivot_cols = c(PFUAggDatabase::efficiency_cols$eta_pf,
                                                                                    PFUAggDatabase::efficiency_cols$eta_fu,
                                                                                    PFUAggDatabase::efficiency_cols$eta_pu)),
                                        targ_name = "eta_Re_all_St_pfu",
                                        type = "csv",
                                        release = Release))
    ),

    # Save the cache for posterity.
    targets::tar_target_raw(
      "store_cache",
      quote(PFUDatabase::stash_cache(pipeline_caches_folder = PipelineCachesOutputFolder,
                                     cache_folder = "_targets",
                                     file_prefix = "pfu_agg_workflow_cache_",
                                     dependency = c(agg_Re_all_St_pfu, eta_Re_all_St_pfu)))
    )
  )
}

