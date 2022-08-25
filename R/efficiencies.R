#' Calculate primary to final demand efficiency
#'
#' This function calculates primary to final demand efficiencies in a data frame of
#' primary and final demand aggregates (`.pfd_data`).
#' Columns of energy conversion chain matrices are removed.
#'
#' @param .agg_pfd_data A data frame of primary and final demand aggregates.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param ex_p,ex_fd The names of columns containing primary and final demand (respectively) aggregates.
#'                   See `Recca::aggregate_cols` for defaults.
#' @param eta_pfd The name of the primary-to-final demand efficiency column.
#'                Default is `Recca::efficiency_cols$eta_pfd`.
#'
#' @return A data frame of primary-to-final-demand efficiencies, both net and gross.
#'
#' @export
calculate_pfd_efficiencies <- function(.agg_pfd_data,
                                       countries,
                                       years,
                                       ex_p = Recca::aggregate_cols$aggregate_primary,
                                       ex_fd = Recca::aggregate_cols$aggregate_demand,
                                       eta_pfd = Recca::efficiency_cols$eta_pfd) {

  filtered_data <- .agg_pfd_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  filtered_data %>%
    dplyr::mutate(
      "{eta_pfd}" := as.numeric(.data[[ex_fd]]) / as.numeric(.data[[ex_p]])
    )
}


#' Pivot data frame to calculate PFU efficiencies
#'
#' This function pivots the data frame produced by `calculate_pfd_efficiencies()`
#' to obtain primary-final, final-useful, and primary-useful efficiencies.
#'
#' @param .eta_pfd_data A data frame produced by `calculate_pfd_efficiencies()`.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param ex_p,ex_fd Names of primary and final demand aggregate columns (respectively).
#'                   Defaults are from `Recca::aggregate_cols`.
#' @param last_stage The name of the last stage column. Default is `Recca::psut_cols$last_stage`.
#' @param eta_pf,eta_fu,eta_pu The names of efficiency columns: primary-to-final, final-to-useful, and primary-to-useful, respectively.
#'                             Defaults from `Recca::efficiency_cols`.
#' @param eta_pfd The name of a column containing primary-to-final demand efficiencies. Default is `Recca::efficiency_cols$eta_pfd`.
#' @param final,useful Entries in the `last_stage` column.
#'                     Defaults are "Final" and "Useful", respectively.
#'
#' @return A data frame with ECC stage efficiencies.
#'
#' @export
calculate_pfu_efficiencies <- function(.eta_pfd_data,
                                       countries,
                                       years,
                                       ex_p = Recca::aggregate_cols$aggregate_primary,
                                       ex_fd = Recca::aggregate_cols$aggregate_demand,
                                       last_stage = Recca::psut_cols$last_stage,
                                       eta_pf = Recca::efficiency_cols$eta_pf,
                                       eta_fu = Recca::efficiency_cols$eta_fu,
                                       eta_pu = Recca::efficiency_cols$eta_pu,
                                       eta_pfd = Recca::efficiency_cols$eta_pfd,
                                       final = "Final",
                                       useful = "Useful") {

  filtered_data <- .eta_pfd_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years)

  if (nrow(filtered_data) == 0) {
    return(NULL)
  }

  filtered_data %>%
    # Eliminate the aggregate values, because they aren't unique
    # and mess up the pf, fu, and pu efficiency rows.
    dplyr::mutate(
      "{ex_p}" := NULL,
      "{ex_fd}" := NULL
    ) %>%
    # Spread across stages
    tidyr::pivot_wider(names_from = last_stage, values_from = eta_pfd) %>%
    # Rename efficiency types
    dplyr::rename(
      "{eta_pf}" := final,
      "{eta_pu}" := useful
    ) %>%
    # Calculate eta_fu where possible
    dplyr::mutate(
      "{eta_fu}" := .data[[eta_pu]] / .data[[eta_pf]]
    ) %>%
    # Move it so the left-ro-right order is eta_pf, eta_fu, eta_pu
    dplyr::relocate(.data[[eta_fu]], .before = .data[[eta_pu]])
}


#' Write an Excel file of efficiency results
#'
#' Writes an Excel file of efficiency results.
#'
#' @param .eta_pfu A data frame of pf, fu, and pu efficiencies.
#' @param release A boolean that tells whether to write the file. Default is `FALSE`.
#' @param directory A folder for the output.
#'                  A subfolder and file name are calculated automatically,
#'                  following the pins naming convention.
#' @param overwrite A boolean that tells whether to overwrite an existing file at `path`. Default is `FALSE`.
#'
#' @return If a file is written, the value of `path`.
#'         If no file is written
#'         (e.g., because `overwrite = FALSE` and the file already exists),
#'         `character(0)`.
#'
#' @export
write_eta_pfu_xlsx <- function(.eta_pfu,
                               release = FALSE,
                               path = NULL,
                               overwrite = FALSE) {
  if (!release) {
    return(character(0))
  }
  # Calculate the path, following the pins model
  # Subfolders have names like "20220824T182259Z-f4b77"
  # iso_date_time <- parsedate::format_iso_8601(Sys.time())
  # without_tz <- gsub(pattern = "\\+00:00", replacement = "", x = iso_date_time)
  # without_dash <- gsub(pattern = "-", replacement = "", x = without_tz)
  # zulu_date_time <- gsub(pattern = ":", replacement = "", x = without_dash)
  #
  # hash_string <- 0



  if (!overwrite & file.exists(path)) {
    stop(paste("File", path, "already exists. Call write_eta_pfu_xlsx(overwrite = TRUE) to overwrite."))
  }
  # folder <- dirname(path)
  # if (!file.exists(folder)) {
  #   dir.create(folder)
  # }
  # writexl::write_xlsx(.eta_pfu, path = path)

  return(path)
}
