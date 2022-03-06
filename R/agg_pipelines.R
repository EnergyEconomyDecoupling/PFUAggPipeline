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
#' @param psut_release The release we'll use from `psut_releases_folder`.
#' @param psut_releases_folder The path to the `pins` archive of `PSUT` releases.
#' @param exemplar_table_path The path to the examplar table.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
get_pipeline <- function(psut_release,
                         psut_releases_folder,
                         exemplar_table_path) {
  # Create the pipeline
  list(
    # Set the release that we'll use
    targets::tar_target_raw(
      name = "PSUT_release",
      command = psut_release
    ),

    # Set the pinboard folder
    targets::tar_target_raw(
      "pinboard_folder",
      psut_releases_folder,
      format = "file"
    ),

    # Pull in the PSUT data frame
    targets::tar_target(
      PSUT,
      pins::board_folder(pinboard_folder, versioned = TRUE) |>
        pins::pin_read("psut", version = PSUT_release)
    ),

    # Set the path to the exemplar_table,
    # which we use for regional aggregations.
    targets::tar_target_raw(
      "exemplar_table_path",
      exemplar_table_path
    ),

    # Create the data frame to be used for regional aggregation
    targets::tar_target(
      continent_aggregation_map,
      PFUAggDatabase::continent_aggregation_map(exemplar_table_path)
    )

    # Aggregate by continent
    # targets::tar_target(
    #   PSUT_Re_continents,
    #   agg_continents(PSUT)
    # )
  )
}



