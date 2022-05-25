library(magrittr)
library(targets)
# targets::tar_make() to run the pipeline in a single thread.
# targets::tar_make_future(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# targets::tar_destroy() to start over with everything,

# Set control parameters for the pipeline.

# Set the countries to be analyzed.
# countries <- c("GBR")
# countries <- c("WMBK", "WABK", "ZAF")
# countries <- c("WMBK", "WABK")
# countries <- c("USA", "CAN", "GBR", "PRT", "ZAF", "WMB", "WAB")
# countries <- PFUDatabase::canonical_countries %>% unlist()
countries <- "all" # Run all countries in PSUT.

# Set the years to be analyzed.
# years <- 1960
# years <- 2000
# years <- 1971:1972
# years <- 1971
# years <- "all" # might get 2020 or other partial years.
years <- 1960:2019

# Set the release of PSUT to be used for input.
# psut_releases <- c(psut = "20220519T185450Z-55e04")
psut_releases = c(psut = "20220519T185450Z-55e04",
                  psut_iea = "20220519T185448Z-07a39",
                  psut_mw = "20220519T185235Z-771f8")


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
                             psut_releases = psut_releases,
                             aggregation_maps_path = PFUSetup::get_abs_paths()[["aggregation_mapping_path"]],
                             pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]],
                             release = release)

