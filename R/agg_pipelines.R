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
#' Note, too, that names of `psut_releases` items will be converted to uppercase
#' for target names.
#' So be sure to make the names of `psut_releases` items unique
#' if converted to uppercase.
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

  # Target names
  aggregation_maps_tar_str <- "AggregationMaps"
  aggregation_maps_tar_sym <- as.symbol(aggregation_maps_tar_str)


  # Target symbols
  continents_tar_str <- "Continents"
  continents_tar_sym <- as.symbol(continents_tar_str)

  # Create the initial targets
  initial_targets <- setup_targets(countries = countries,
                                   years = years,
                                   psut_pin,
                                   psut_releases = psut_releases,
                                   aggregation_maps_path = aggregation_maps_path,
                                   pipeline_caches_folder = pipeline_caches_folder,
                                   pipeline_releases_folder = pipeline_releases_folder,
                                   release = release,
                                   aggregation_maps_tar_str,
                                   aggregation_maps_tar_sym,
                                   continents_tar_str)



  # This function should return both a list of targets and dependencies.
  middle_targets <- lapply(psut_releases, get_one_middle_pipeline(pr))
  # Unpack the targets

  # Unpack the dependencies



  final_target <- cache_target()

  # Return all targets as a single list
  c(initial_targets, middle_targets, final_target)
}



#' Last targets in the pipeline
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
setup_targets <- function(countries, years, psut_pin, psut_releases,
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
    targets::tar_target_raw("PSUTPin", psut_pin),
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
