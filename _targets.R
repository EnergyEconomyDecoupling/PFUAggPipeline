library(magrittr)
library(targets)
# targets::tar_make() to run the pipeline in a single thread.
# targets::tar_make_future(workers = 8) to execute across multiple cores.
# targets::tar_make(callr_function = NULL) to debug.
# targets::tar_read(<<target_name>>) to view the results.
# targets::tar_invalidate(<<target_name>>) to re-compute <<target_name>> and its dependents.
# targets::tar_destroy() to start over with everything,

# Set control parameters for the pipeline.

# Set the countries to be analyzed.
# countries <- c("GBR", "USA", "MEX")
countries <- "USA"
# countries <- "WRLD"
# countries <- "CHNM"
# countries <- "all" # Run all countries in the PSUT target.
# countries <- PFUDatabase::canonical_countries %>% as.character()
# Countries with unique allocation data plus BEL and TUR (for Pierre).
# countries <- c("BRA", "CAN", "CHNM", "DEU", "DNK", "ESP", "FRA", "GBR", "GHA",
#                "GRC", "HKG", "HND", "IDN", "IND", "JOR", "JPN", "KOR", "MEX",
#                "NOR", "PRT", "RUS", "USA", "WABK", "WMBK", "ZAF", "BEL", "TUR")


# Set the years to be analyzed.
years <- 1960:2019
# years <- 1971:1973
# years <- 1971:1972
# years <- 1971

# Tells whether to do the R and Y chops.
do_chops <- FALSE

# Set the release to be used for input.
psut_release <- "20230220T160616Z-35e3e"    # v0.9 (USA only)
# psut_release <- "20221109T152414Z-7d7ad"  # v1
# psut_release <- "20221219T143657Z-964a6"  # For WRLD
# psut_release <- "20230130T150642Z-631e2"  # For WRLD, 1971
# psut_release <- "20230130T192359Z-1d3ec"  # For WRLD, 1971-2019

# Should we release the results?
release <- TRUE







# End user-adjustable parameters.


# Set up for multithreaded work on the local machine.
future::plan(future.callr::callr)

# Set options for all targets.
targets::tar_option_set(
  packages = "PFUAggDatabase",
  # Indicate that storage and retrieval of subtargets
  # should be done by the worker thread,
  # not the main thread.
  # These options set defaults for all targets.
  # Individual targets can override.
  storage = "worker",
  retrieval = "worker",
  # Tell targets to NOT keep everything in memory ...
  memory = "transient",
  # ... and to garbage-collect the memory when done.
  garbage_collection = TRUE
)

# Pull in the pipeline
PFUAggDatabase::get_pipeline(countries = countries,
                             years = years,
                             do_chops = do_chops,
                             psut_release = psut_release,
                             aggregation_maps_path = PFUSetup::get_abs_paths()[["aggregation_mapping_path"]],
                             pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]],
                             pipeline_caches_folder = PFUSetup::get_abs_paths()[["pipeline_caches_folder"]],
                             release = release)



# For WRLD

# project_path <- PFUSetup::get_abs_paths()[["project_path"]]
# wrld_path <- paste0(project_path, "/PFUDatabase-WRLD-InputData/")
#
# PFUAggDatabase::get_pipeline(countries = countries,
#                              years = years,
#                              do_chops = do_chops,
#                              psut_release = psut_release,
#                              aggregation_maps_path = paste0(wrld_path, "aggregation_mapping.xlsx"),
#                              pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]],
#                              pipeline_caches_folder = PFUSetup::get_abs_paths()[["pipeline_caches_folder"]],
#                              release = release)




