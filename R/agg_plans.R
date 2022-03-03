get_plan <- function() {
  # Create the targets list
  list(
    # Set the release that we'll use
    targets::tar_target(
      PSUT_version,
      "20220225T012039Z-c2035"
    ),

    # Set the pinboard folder
    targets::tar_target(
      pinboard_folder,
      PFUSetup::get_abs_paths()[["workflow_releases_folder"]],
      format = "file"),

    # Pull in the PSUT data frame
    targets::tar_target(
      PSUT,
      pins::board_folder(pinboard_folder,
                         versioned = TRUE) |>
        pins::pin_read("psut", version = PSUT_version)
    )
  )
}


