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
                                   psut_releases = psut_releases,
                                   aggregation_maps_path = aggregation_maps_path,
                                   pipeline_caches_folder = pipeline_caches_folder,
                                   pipeline_releases_folder = pipeline_releases_folder,
                                   release = release,
                                   aggregation_maps_tar_str = aggregation_maps_tar_str,
                                   aggregation_maps_tar_sym = aggregation_maps_tar_sym,
                                   continents_tar_str = continents_tar_str)



  # This function should return both a list of targets and dependencies.
  middle_targets <- list()
  for (i_pr in 1:length(psut_releases)) {
    # Preserve name of i_pr'th psut_release.
    pr <- psut_releases[i_pr]
    these_mid_targs <- get_one_middle_pipeline(pr = pr)
    # Unpack the targets

    # Unpack the dependencies
    middle_targets <- c(middle_targets, these_mid_targs)
  }



  final_target <- cache_target()

  # Return all targets as a single list
  c(initial_targets, middle_targets, final_target)
}


