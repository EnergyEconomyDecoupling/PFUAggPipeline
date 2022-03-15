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
#'                  Default is `NULL`, meaning all countries should be analyzed.
#' @param psut_release The release we'll use from `psut_releases_folder`.
#' @param psut_releases_folder The path to the `pins` archive of `PSUT` releases.
#' @param exemplar_table_path The path to the exemplar table.
#' @param world_agg_map The aggregation map to aggregate from continents to the world.
#'
#' @return A list of `tar_target`s to be executed in a workflow.
#'
#' @export
get_pipeline <- function(countries = "all",
                         psut_release,
                         psut_releases_folder,
                         exemplar_table_path,
                         world_agg_map) {

  # Avoid notes when checking the package.
  keep_countries <- NULL
  PSUT <- NULL
  pinboard_folder <- NULL
  PSUT_release <- NULL
  PSUT_Re_continents <- NULL
  PSUT_Re_world <- NULL
  world_aggregation_map <- NULL
  PSUT_Re_all <- NULL
  p_industry_prefixes <- NULL
  PSUT_Re_all_St_p <- NULL
  final_demand_sectors <- NULL
  PSUT_Re_all_St_fu <- NULL
  PSUT_Re_all_St_pfu <- NULL

  # Create the pipeline
  list(

    # Identify the countries for this analysis.
    # NULL means all countries.
    targets::tar_target(
      name = keep_countries,
      command = countries
    ),

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
      pins::board_folder(pinboard_folder, versioned = TRUE) %>%
        pins::pin_read("psut", version = PSUT_release) %>%
        filter_countries(keep_countries),
      # Very important to assign storage and retrieval tasks to workers,
      # else the pipeline seemingly never finishes.
      storage = "worker",
      retrieval = "worker",
    ),

    # Set the path to the exemplar_table,
    # which we use for regional aggregations.
    targets::tar_target_raw(
      "exemplar_table_path",
      exemplar_table_path
    ),


    #########################
    # Regional aggregations #
    #########################

    # Create the data frame to be used for continental aggregation
    targets::tar_target(
      continent_aggregation_map,
      PFUAggDatabase::continent_aggregation_map(exemplar_table_path)
    ),

    # Create a continents data frame, grouped by continent,
    # so subsequent operations (region aggregation)
    # will be performed in parallel, if desired.
    tarchetypes::tar_group_by(
      name = PSUT_with_continent_col,
      command = join_psut_continents(PSUT = PSUT,
                                     continent_aggregation_map = continent_aggregation_map,
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
                               few_colname = "Continent"),
      pattern = map(PSUT_with_continent_col),
      storage = "worker",
      retrieval = "worker"
    ),

    # Create the world aggregation map,
    # which is simply the incoming object.
    targets::tar_target_raw(
      "world_aggregation_map",
      world_agg_map
    ),

    # Aggregate to world (WLD)
    targets::tar_target(
      PSUT_Re_world,
      Recca::region_aggregates(PSUT_Re_continents %>%
                                 dplyr::mutate(
                                   World = "WLD"
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

    # Establish prefixes for primary industries
    targets::tar_target(
      p_industry_prefixes,
      IEATools::prim_agg_flows %>% unname() %>% unlist() %>% list()
    ),

    # Aggregate primary energy/exergy by total (total energy supply (TES)), product, and flow
    targets::tar_target(
      PSUT_Re_all_St_p,
      calculate_primary_ex_data(PSUT_Re_all, p_industry_prefixes = p_industry_prefixes)
    ),

    # Establish final demand sectors
    targets::tar_target(
      final_demand_sectors,
      IEATools::fd_sectors
    ),

    # Aggregate final and useful energy/exergy by total (total final consumption (TFC)), product, and sector
    targets::tar_target(
      PSUT_Re_all_St_fu,
      calculate_finaluseful_ex_data(.sutdata = PSUT_Re_all, fd_sectors = final_demand_sectors)
    ),

    # Bring the aggregations together in a single data frame
    targets::tar_target(
      PSUT_Re_all_St_pfu,
      dplyr::bind_rows(PSUT_Re_all_St_p, PSUT_Re_all_St_fu)
    ),


    ################
    # Efficiencies #
    ################

    targets::tar_target(
      eta_Re_all_St_pfu,
      calc_agg_etas(PSUT_Re_all_St_pfu)
    )



  )
}

