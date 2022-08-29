library(magrittr)
library(targets)
# targets::tar_make() to run the pipeline in a single thread.
# targets::tar_make(callr_function = NULL) to debug.
# targets::tar_make_future(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# targets::tar_destroy() to start over with everything,

# Set control parameters for the pipeline.

# Set the countries to be analyzed.
# countries <- c("GBR", "USA", "MEX")
# countries <- "USA"
# countries <- "all" # Run all countries in the PSUT target.
# countries <- PFUDatabase::canonical_countries %>% as.character()
# Countries with unique allocation data.
countries <- c("BRA", "CAN", "CHNM", "DEU", "DNK", "ESP", "FRA", "GBR", "GHA",
               "GRC", "HKG", "HND", "IDN", "IND", "JOR", "JPN", "KOR", "MEX",
               "NOR", "PRT", "RUS", "USA", "WABK", "WMBK", "ZAF")


# Set the years to be analyzed.
years <- 1960:2019
# years <- 1971

# Set the releases to be used for input.
psut_release = "20220828T174526Z-60a07"

# Should we do a release of the results?
release <- FALSE







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
  retrieval = "worker"
)

# Pull in the pipeline
PFUAggDatabase::get_pipeline(countries = countries,
                             years = years,
                             psut_release = psut_release,
                             aggregation_maps_path = PFUSetup::get_abs_paths()[["aggregation_mapping_path"]],
                             pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]],
                             pipeline_caches_folder = PFUSetup::get_abs_paths()[["pipeline_caches_folder"]],
                             release = release)

