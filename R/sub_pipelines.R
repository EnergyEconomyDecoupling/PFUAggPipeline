#' Create setup targets for PFUAggDatabase
#'
#' There are several initial targets that are invariant
#' to the number of psut_releases that need to be analyzed
#' by this workflow.
#' This function creates the initial targets.
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
#' @return A list of initial targets.
#'
#' @export
setup_targets <- function(countries, years, psut_releases,
                          aggregation_maps_path, pipeline_caches_folder,
                          pipeline_releases_folder, release,
                          aggregation_maps_tar_str,
                          aggregation_maps_tar_sym,
                          continents_tar_str) {

  list(
    #####################
    # Preliminary setup #
    #####################

    # Store some incoming data as targets.
    # These targets are invariant across incoming psut_releases.
    targets::tar_target_raw("Countries", list(countries)),
    targets::tar_target_raw("Years", list(years)),
    targets::tar_target_raw("PSUTReleases", unname(psut_releases)),
    targets::tar_target_raw("AggregationMapsPath", aggregation_maps_path),
    targets::tar_target_raw("PipelineCachesOutputFolder", pipeline_caches_folder),
    targets::tar_target_raw("PinboardFolder", pipeline_releases_folder),
    targets::tar_target_raw("Release", release),

    # Establish prefixes for primary industries
    targets::tar_target_raw(
      "p_industry_prefixes",
      quote(IEATools::tpes_flows %>% unname() %>% unlist() %>% list())),

    # Establish final demand sectors
    targets::tar_target_raw(
      "final_demand_sectors",
      quote(IEATools::fd_sectors)),

    # Gather the aggregation maps.
    # targets::tar_target_raw("AggregationMaps", quote(load_aggregation_maps(path = AggregationMapsPath))),
    targets::tar_target_raw(
      aggregation_maps_tar_str,
      quote(load_aggregation_maps(path = AggregationMapsPath))),

    # Identify the continents to which we'll aggregate
    targets::tar_target_raw(
      continents_tar_str,
      substitute(aggregation_maps_tar_sym$world_aggregation$World)
    )
  )

}


#' A pipeline for one PSUT release
#'
#' `PFUAggDatabase` works with any number of PSUT releases from `PFUDatabase`.
#' This function creates a portion of a `targets` pipeline
#' for _one_ of those releases.
#'
#' @param pr The `PFUDatabase` pipeline release for which this pipeline
#'           segment will be constructed.
#'
#' @return A list of targets for the incoming PSUT release.
#'
#' @export
get_one_middle_pipeline <- function(pr) {

  # At this point, we will have only a single, named value for pr.
  # Get the name (the pin).
  psut_pin <- names(pr)
  # Split the pin name at an underscore to get the suffix
  agg_eta_suff <- strsplit(psut_pin, split = "_", fixed = TRUE)
  if (length(agg_eta_suff) == 1) {
    # Didn't find anything after a "_".
    agg_eta_suff <- ""
  }
  agg_eta_pref <- paste0("Agg_Eta", agg_eta_suff)
  agg_pref <- paste0("Agg", agg_eta_suff)
  eta_pref <- paste0("Eta", agg_eta_suff)

  # Set target names based on the psut_tar_str.
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
  pin_agg_eta_tar_str_Re_all_St_pfu <- paste0("Pin_", agg_eta_tar_str_Re_all_St_pfu)
  pin_agg_tar_str_Re_all_St_pfu_csv <- paste0("Pin_", agg_tar_str_Re_all_St_pfu, "_csv")
  pin_eta_tar_str_Re_all_St_pfu_csv <- paste0("Pin_", eta_tar_str_Re_all_St_pfu, "_csv")

  # Set symbols for targets to which we refer later in the pipeline.
  psut_tar_sym <- as.symbol(psut_tar_str)
  psut_tar_sym_with_continent_col <- as.symbol(psut_tar_str_with_continent_col)
  psut_tar_sym_Re_continents <- as.symbol(psut_tar_str_Re_continents)
  psut_tar_sym_Re_world <- as.symbol(psut_tar_str_Re_world)
  psut_tar_sym_Re_all <- as.symbol(psut_tar_str_Re_all)
  psut_tar_sym_Re_all_St_p <- as.symbol(psut_tar_str_Re_all_St_p)
  psut_tar_sym_Re_all_St_fu <- as.symbol(psut_tar_str_Re_all_St_fu)
  psut_tar_sym_Re_all_St_pfu <- as.symbol(psut_tar_str_Re_all_St_pfu)
  agg_eta_tar_sym_Re_all_St_pfu <- as.symbol(agg_eta_tar_str_Re_all_St_pfu)
  agg_tar_sym_Re_all_St_pfu <- as.symbol(agg_tar_str_Re_all_St_pfu)
  eta_tar_sym_Re_all_St_pfu <- as.symbol(eta_tar_str_Re_all_St_pfu)
  pin_agg_eta_tar_sym_Re_all_St_pfu <- as.symbol(pin_agg_eta_tar_str_Re_all_St_pfu)
  pin_agg_tar_sym_Re_all_St_pfu_csv <- as.symbol(pin_agg_tar_str_Re_all_St_pfu_csv)
  pin_eta_tar_sym_Re_all_St_pfu_csv <- as.symbol(pin_eta_tar_str_Re_all_St_pfu_csv)

  # Create the pipeline
  list(

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
    # tarchetypes::tar_group_by(
    #   name = PSUT_with_continent_col,
    #   command = join_psut_continents(PSUT = PSUT,
    #                                  continent_aggregation_map = AggregationMaps$continent_aggregation,
    #                                  continent = "Continent"),
    #   # The columns to group by, as symbols.
    #   Continent),
    targets::tar_target_raw(
      psut_tar_str_with_continent_col,
      substitute(join_psut_continents(PSUT = psut_tar_sym,
                                      continent_aggregation_map = aggregation_maps_tar_sym$continent_aggregation,
                                      continent = "Continent"))),

    # Aggregate by continent
    targets::tar_target_raw(
      psut_tar_str_Re_continents,
      substitute(continent_aggregation(psut_tar_sym_with_continent_col,
                                       continents = continents_tar_sym,
                                       many_colname = IEATools::iea_cols$country,
                                       few_colname = "Continent")),
      pattern = quote(map(Continents))),


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
      substitute(dplyr::bind_rows(psut_tar_sym, psut_tar_sym_Re_continents, psut_tar_sym_Re_world))),



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
      pin_agg_eta_tar_str_Re_all_St_pfu,
      # quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
      #                                   targ = agg_eta_Re_all_St_pfu,
      #                                   targ_name = "agg_eta_Re_all_St_pfu",
      #                                   release = Release))),
      substitute(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = agg_eta_tar_sym_Re_all_St_pfu,
                                             targ_name = agg_eta_tar_str_Re_all_St_pfu,
                                             release = Release))),

    # Pin aggregates as a wide-by-years .csv file
    targets::tar_target_raw(
      pin_agg_tar_str_Re_all_St_pfu_csv,
      # quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
      #                                   targ = agg_Re_all_St_pfu %>%
      #                                     pivot_agg_eta_wide_by_year(pivot_cols = c(IEATools::all_stages$primary,
      #                                                                               IEATools::all_stages$final,
      #                                                                               IEATools::all_stages$useful)),
      #                                   targ_name = "agg_Re_all_St_pfu",
      #                                   type = "csv",
      #                                   release = Release))
      substitute(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = agg_tar_sym_Re_all_St_pfu %>%
                                               pivot_agg_eta_wide_by_year(pivot_cols = c(IEATools::all_stages$primary,
                                                                                         IEATools::all_stages$final,
                                                                                         IEATools::all_stages$useful)),
                                             targ_name = agg_tar_str_Re_all_St_pfu,
                                             type = "csv",
                                             release = Release))
    ),


    # Pin efficiencies as a wide-by-years .csv file
    targets::tar_target_raw(
      pin_eta_tar_str_Re_all_St_pfu_csv,
      # quote(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
      #                                   targ = eta_Re_all_St_pfu %>%
      #                                     pivot_agg_eta_wide_by_year(pivot_cols = c(PFUAggDatabase::efficiency_cols$eta_pf,
      #                                                                               PFUAggDatabase::efficiency_cols$eta_fu,
      #                                                                               PFUAggDatabase::efficiency_cols$eta_pu)),
      #                                   targ_name = "eta_Re_all_St_pfu",
      #                                   type = "csv",
      #                                   release = Release))
      substitute(PFUDatabase::release_target(pipeline_releases_folder = PinboardFolder,
                                             targ = eta_tar_sym_Re_all_St_pfu %>%
                                               pivot_agg_eta_wide_by_year(pivot_cols = c(PFUAggDatabase::efficiency_cols$eta_pf,
                                                                                         PFUAggDatabase::efficiency_cols$eta_fu,
                                                                                         PFUAggDatabase::efficiency_cols$eta_pu)),
                                             targ_name = eta_tar_str_Re_all_St_pfu,
                                             type = "csv",
                                             release = Release))
    )
  )
}


#' Last target in the pipeline
#'
#' Provides a final target to store the cache of the pipeline.
#'
#' @return A logical saying whether the saving operation was successful.
#'         If `release = FALSE`, `FALSE` is returned.
#'
#' @export
cache_target <- function() {
  list(
    # Save the cache for posterity.
    # This target is invariant across various psut_releases.
    # In fact, this target should be executed only after
    # all targets for all psut releases have been executed.
    targets::tar_target_raw(
      "store_cache",
      quote(PFUDatabase::stash_cache(pipeline_caches_folder = PipelineCachesOutputFolder,
                                     cache_folder = "_targets",
                                     file_prefix = "pfu_agg_workflow_cache_",
                                     dependency = c(agg_Re_all_St_pfu, eta_Re_all_St_pfu)))
    )
  )
}


