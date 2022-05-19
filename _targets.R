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
# countries <- PFUWorkflow::canonical_countries %>% unlist()
countries <- "all" # Run all countries in PSUT.

# Set the years to be analyzed.
# years <- 1960
# years <- 2000
# years <- 1971:1972
# years <- 1971
# years <- "all" # might get 2020 or other partial years.
years <- 1960:2019

# Set the release of PSUT to be used for input.
psut_release <- "20220414T140245Z-2952b"

# Should we do a release of the results?
release <- FALSE







# End user-adjustable parameters.


# Set up for multithreaded work on the local machine.
future::plan(future.callr::callr)

# Set options for all targets.
targets::tar_option_set(
  # Set packages to be used.
  packages = c("PFUAggDatabase"),
  storage = "worker",
  retrieval = "worker"
)

# Pull in the pipeline
PFUAggDatabase::get_pipeline(countries = countries,
                             years = years,
                             psut_release = psut_release,
                             aggregation_maps_path = PFUSetup::get_abs_paths()[["aggregation_mapping_path"]],
                             pipeline_caches_folder = PFUSetup::get_abs_paths()[["pipeline_caches_folder"]],
                             pipeline_releases_folder = PFUSetup::get_abs_paths()[["pipeline_releases_folder"]],
                             release = release)

