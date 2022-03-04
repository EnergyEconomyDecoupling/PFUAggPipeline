#' Create a targets workflow
#'
#' Arguments to this function specify the details of a targets workflow to be executed.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
#'
#' @examples
get_pipeline <- function(psut_release, psut_releases_folder) {
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

    # Aggregate by continent
    targets::tar_target(
      PSUT_Re_continents,
      agg_continents(PSUT)
    )
  )
}


