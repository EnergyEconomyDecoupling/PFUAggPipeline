get_plan <- function() {
  # Create the targets list
  list(
    # Set the release that we'll use
    targets::tar_target(
      PSUT_pin,
      "20220225T012039Z-c2035"
    ),

    # Establish the pinboard
    targets::tar_target(
      PSUT_pinboard,
      pins::board_folder(PFUSetup::get_abs_paths()[["workflow_releases_folder"]],
                         versioned = TRUE)
    )
  )
}




# Pull in the PSUT data
# targets::tar_target(
#   PSUT,
#   PSUT_pinboard
# )
