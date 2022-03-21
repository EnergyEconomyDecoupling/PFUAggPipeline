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
#' @param countries A string vector of 3-letter country codes.
#'                  Default is "all", meaning all available countries should be analyzed.
#' @param years A numeric vector of years to be analyzed.
#'              Default is "all", meaning all available years should be analyzed.
#' @param psut_release The release we'll use from `psut_releases_folder`.
#' @param psut_releases_folder The path to the `pins` archive of `PSUT` releases.
#' @param aggregation_maps_path The path to the Excel file of aggregation maps.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
get_pipeline <- function(countries = "all",
                         years = "all",
                         psut_release,
                         psut_releases_folder,
                         aggregation_maps_path) {

  # Avoid notes when checking the package.
  keep_countries <- NULL
  keep_years <- NULL
  PSUT <- NULL
  pinboard_folder <- NULL
  PSUT_release <- NULL
  PSUT_Re_continents <- NULL
  PSUT_Re_world <- NULL
  PSUT_Re_all <- NULL
  p_industry_prefixes <- NULL
  PSUT_Re_all_St_p <- NULL
  final_demand_sectors <- NULL
  PSUT_Re_all_St_fu <- NULL
  PSUT_Re_all_St_pfu <- NULL
  PSUT_with_continent_col <- NULL
  PSUT_Re_all_by_country <- NULL
  PSUT_Re_all_St_pfu_by_country <- NULL
  eta_Re_all_St_pfu <- NULL
  aggregation_maps <- NULL

  Continent <- NULL
  Country <- NULL
  map <- NULL

  # Create the pipeline
  list(

    # Identify the countries for this analysis.
    # "all" means all countries.
    targets::tar_target(
      name = keep_countries,
      command = countries
    ),

    # Identify the countries for this analysis.
    # "all" means all countries.
    targets::tar_target(
      name = keep_years,
      command = years
    ),

    # Set the release that we'll use
    targets::tar_target_raw(
      name = "PSUT_release",
      command = psut_release
    ),

    # Set the path to the aggregation maps file.
    targets::tar_target_raw(
      name = "aggregation_maps_path",
      command = aggregation_maps_path
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
      pins::board_folder(pinboard_folder, versioned = TRUE) %>%
        pins::pin_read("psut", version = PSUT_release) %>%
        filter_countries_and_years(countries = keep_countries, years = keep_years),
      # Very important to assign storage and retrieval tasks to workers,
      # else the pipeline seemingly never finishes.
      storage = "worker",
      retrieval = "worker",
    ),


    ############################
    # Prepare for aggregations #
    ############################

    # Gather the aggregation maps.
    targets::tar_target(
      aggregation_maps,
      load_aggregation_maps(path = aggregation_maps_path)
    ),


    #########################
    # Regional aggregations #
    #########################

    # Create a continents data frame, grouped by continent,
    # so subsequent operations (region aggregation)
    # will be performed in parallel, if desired.
    tarchetypes::tar_group_by(
      name = PSUT_with_continent_col,
      command = join_psut_continents(PSUT = PSUT,
                                     continent_aggregation_map = aggregation_maps$continent_aggregation,
                                     continent = "Continent"),
      # The columns to group by, as symbols.
      Continent,
      storage = "worker",
      retrieval = "worker"
    ),

    # Aggregate by continent
    targets::tar_target(
      PSUT_Re_continents,
      Recca::region_aggregates(PSUT_with_continent_col,
                               many_colname = IEATools::iea_cols$country,
                               few_colname = "Continent") %>%
        # Eliminate the targets grouping on Continent
        # so we can group by country later.
        dplyr::mutate(
          tar_group = NULL
        ),
      pattern = map(PSUT_with_continent_col),
      storage = "worker",
      retrieval = "worker"
    ),

    # Aggregate to world (WLD)
    targets::tar_target(
      PSUT_Re_world,
      Recca::region_aggregates(PSUT_Re_continents %>%
                                 dplyr::mutate(
                                   World = "WRLD"
                                 ),
                               many_colname = IEATools::iea_cols$country, # Which actually holds continents
                               few_colname = "World")
    ),

    # Bind all region aggregations together
    targets::tar_target(
      PSUT_Re_all,
      dplyr::bind_rows(PSUT, PSUT_Re_continents, PSUT_Re_world)
    ),


    ####################
    # PFU aggregations #
    ####################

    # Set up a grouped-by-country data frame
    # so all future calculations are parallelized across countries.
    tarchetypes::tar_group_by(
      name = PSUT_Re_all_by_country,
      command = PSUT_Re_all,
      # The columns to group by, as symbols.
      Country,
      storage = "worker",
      retrieval = "worker"
    ),

    # Establish prefixes for primary industries
    targets::tar_target(
      p_industry_prefixes,
      IEATools::prim_agg_flows %>% unname() %>% unlist() %>% list()
    ),

    # Aggregate primary energy/exergy by total (total energy supply (TES)), product, and flow
    targets::tar_target(
      PSUT_Re_all_St_p,
      calculate_primary_ex_data(PSUT_Re_all_by_country,
                                p_industry_prefixes = p_industry_prefixes),
      pattern = map(PSUT_Re_all_by_country),
      storage = "worker",
      retrieval = "worker"
    ),

    # Establish final demand sectors
    targets::tar_target(
      final_demand_sectors,
      IEATools::fd_sectors
    ),

    # Aggregate final and useful energy/exergy by total (total final consumption (TFC)), product, and sector
    targets::tar_target(
      name = PSUT_Re_all_St_fu,
      command = calculate_finaluseful_ex_data(PSUT_Re_all_by_country,
                                              fd_sectors = final_demand_sectors),
      pattern = map(PSUT_Re_all_by_country),
      storage = "worker",
      retrieval = "worker"
    ),

    # Bring the aggregations together in a single data frame
    targets::tar_target(
      PSUT_Re_all_St_pfu,
      dplyr::bind_rows(PSUT_Re_all_St_p, PSUT_Re_all_St_fu)
    ),


    ################
    # Efficiencies #
    ################

    tarchetypes::tar_group_by(
      name = PSUT_Re_all_St_pfu_by_country,
      command = PSUT_Re_all_St_pfu %>%
        dplyr::mutate(
          tar_group = NULL
        ),
      # The columns to group by, as symbols.
      Country,
      storage = "worker",
      retrieval = "worker"
    ),

    targets::tar_target(
      name = eta_Re_all_St_pfu,
      command = calc_agg_etas(PSUT_Re_all_St_pfu_by_country),
      pattern = map(PSUT_Re_all_St_pfu_by_country),
      storage = "worker",
      retrieval = "worker"
    ),

    targets::tar_target(
      write_agg_etas_xlsx,
      write_agg_etas_xlsx(eta_Re_all_St_pfu,
                          path = file.path(PFUSetup::get_abs_paths()[["reports_dest_folder"]], "AggregateEfficiencyResults.xlsx"))
    )



  )
}

