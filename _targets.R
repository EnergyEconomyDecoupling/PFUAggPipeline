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
# countries <- "USA"
# countries <- "WRLD"
# countries <- "CHNM"
# countries <- c("GHA", "ZAF")
# countries <- "all" # Run all countries in the PSUT target.
countries <- c(PFUDatabase::canonical_countries, "WRLD") |> as.character()
# Countries with unique allocation data plus BEL and TUR (for Pierre).
# countries <- c("BRA", "CAN", "CHNM", "DEU", "DNK", "ESP", "FRA", "GBR", "GHA", "GRC",
#                "HKG", "HND", "IDN", "IND", "JOR", "JPN", "KOR", "MEX", "NOR", "PRT",
#                "RUS", "USA", "WABK", "WMBK", "ZAF", "BEL", "TUR")


# Set the years to be analyzed.
years <- 1960:2020
# years <- 1971:1973
# years <- 1971:1978
# years <- 1971
# years <- 1960:1961

# Tells whether to do the R and Y chops.
do_chops <- FALSE

# Set the release to be used for input.
# psut_release <- "20230309T184624Z-7ace5"  # v0.9 (USA only)
# psut_release <- "20221109T152414Z-7d7ad"  # v1.0 (with matrix objects)
# psut_release <- "20230312T211924Z-007da"  # v1.0 (with Matrix objects)
psut_release <- "20230606T153820Z-d3dfe"    # v1.1 (with Matrix objects)
# psut_release <- "20221219T143657Z-964a6"  # For WRLD
# psut_release <- "20230130T150642Z-631e2"  # For WRLD, 1971
# psut_release <- "20230130T192359Z-1d3ec"  # For WRLD, 1971-2019

# Should we release the results?
release <- FALSE







# End user-adjustable parameters.

#
# Set up some machine-specific parameters,
# mostly for input and output locations.
#

sys_info <- Sys.info()
if (startsWith(sys_info[["nodename"]], "Mac")) {
  setup <- PFUSetup::get_abs_paths()
} else if (endsWith(sys_info[["nodename"]], "arc4.leeds.ac.uk")) {
  uname <- sys_info[["user"]]
  setup <- PFUSetup::get_abs_paths(home_path <- "/nobackup",
                                   dropbox_path = uname)
  # Set the location for the _targets folder.
  targets::tar_config_set(store = setup[["output_data_path"]])
} else {
  stop("Unknown system in _targets.R for PFUAggDatabase. Can't set input and output locations.")
}

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
                             aggregation_maps_path = setup[["aggregation_mapping_path"]],
                             pipeline_releases_folder = setup[["pipeline_releases_folder"]],
                             pipeline_caches_folder = setup[["pipeline_caches_folder"]],
                             release = release)




