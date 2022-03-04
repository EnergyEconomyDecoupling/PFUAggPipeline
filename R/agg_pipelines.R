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
      continent_table,
      PFUAggDatabase::create_continent_table(exemplar_table_path)
    )

    # Aggregate by continent
    # targets::tar_target(
    #   PSUT_Re_continents,
    #   agg_continents(PSUT)
    # )
  )
}



#' Load and create a data frame for use with continent aggregation
#'
#' The exemplar table is assumed to be an Excel file with the following columns:
#' `region_code` and years (as numbers).
#' The body of the table should contain 3-letter codes
#' of countries.
#' The exemplar table is assumed to be on the `exemplar_table_tab` tab of the Excel file.

#'
#' @param exemplar_table_path The path to the exemplar table
#' @param region_code The name of a column containing region codes.
#'                    Default is "Region.code".
#' @param exemplar_table_tab The name of the tab in the file at `exemplar_table_path`
#'                           where the region information exists.
#'                           Default is "exemplar_table".
#' @param country The name of the country column in the outgoing data frame.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column in the outgoing data frame.
#'             Default is `IEATools::iea_cols$year`.
#'
#' @return A data frame with columns "Country", "Year", and `region_code`.
#'
#' @export
create_continent_table <- function(exemplar_table_path,
                                   exemplar_table_tab = "exemplar_table",
                                   region_code = "Region.code",
                                   continent = "Continent",
                                   country = IEATools::iea_cols$country,
                                   year = IEATools::iea_cols$year) {

  # Load the file
  region_table <- readxl::read_excel(path = exemplar_table_path,
                                     sheet = exemplar_table_tab)
  # Find names for the region_code column and year columns
  region_code_col <- which((names(region_table) == region_code), arr.ind = TRUE)
  year_col_nums <- IEATools::year_cols(region_table)
  year_col_names <- IEATools::year_cols(region_table, return_names = TRUE)
  keep <- c(region_code_col, year_col_nums)
  # pivot the table and return
  region_table |>
    dplyr::select(keep) |>
    tidyr::pivot_longer(cols = year_col_names, names_to = year, values_to = country) |>
    # Remove an blank entries that appear as NA
    dplyr::filter(!is.na(.data[[country]])) |>
    dplyr::rename(
      "{continent}" := .data[[region_code]]
    )
}
