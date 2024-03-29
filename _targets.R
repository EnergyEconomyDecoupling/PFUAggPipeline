library(Matrix)
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
# countries <- c("ZWE", "USA", "WRLD")
# countries <- "USA"
# countries <- "WRLD"
# countries <- "CHNM"
# countries <- "GHA"
# countries <- "all" # Run all countries in the PSUT target.
# Countries with unique allocation data plus BEL and TUR (for Pierre).
# countries <- c("BRA", "CAN", "CHNM", "DEU", "DNK", "ESP", "FRA", "GBR", "GHA", "GRC",
#                "HKG", "HND", "IDN", "IND", "JOR", "JPN", "KOR", "MEX", "NOR", "PRT",
#                "RUS", "USA", "WABK", "WMBK", "ZAF", "BEL", "TUR")
countries <- c(PFUPipelineTools::canonical_countries, "WRLD") |> as.character()


# Set the years to be analyzed.
# years <- 1971:1973
# years <- 1971:1978
# years <- 1971
# years <- 1960
# years <- 1960:1961
years <- 1960:2020

# Tells whether to do the R and Y chops.
do_chops <- FALSE

setup_version <- "v1.3"
output_version <- "v1.3b2"

# Set the release to be used for input.
# psut_release <- "20230309T184624Z-7ace5"  # v0.9 (USA only)
# psut_release <- "20221109T152414Z-7d7ad"  # v1.0 (with matrix objects)
# psut_release <- "20230312T211924Z-007da"  # v1.0 (with Matrix objects)
# psut_release <- "20230618T131003Z-4c70f"  # v1.1 (with Matrix objects)
# psut_release <- "20230915T185731Z-c48a0"  # v1.2a1 (Lacks new phi values and updated IEA data)
# psut_release <- "20230924T185331Z-13381"  # v1.2a2 (Includes new phi values, removes CHNM as RUS exemplar. Lacks updated IEA data)
# psut_release <- "20231207T124854Z-73744"    # v1.2
# psut_release <- "20221219T143657Z-964a6"  # For WRLD
# psut_release <- "20230130T150642Z-631e2"  # For WRLD, 1971
# psut_release <- "20230130T192359Z-1d3ec"  # For WRLD, 1971-2019
psut_release <- PFUSetup::pin_versions(output_version)[["psut"]]
Y_fu_U_EIOU_fu_details_release <- PFUSetup::pin_versions(output_version)[["Y_fu_U_EIOU_fu_details"]]

# psut_without_neu_release <- "20231207T124907Z-bdcbc"    # v1.2 (hopefully) final
psut_without_neu_release <- PFUSetup::pin_versions(output_version)[["psut_without_neu"]]
# phi_vecs_release <- "20231207T124813Z-c8827" # v1.2
phi_vecs_release <- PFUSetup::pin_versions(output_version)[["phi_vecs"]]



# Should we release the results?
release <- FALSE







# End user-adjustable parameters.

#
# Set up some machine-specific parameters,
# mostly for input and output locations.
#

sys_info <- Sys.info()
if (startsWith(sys_info[["nodename"]], "Mac")) {
  setup <- PFUSetup::get_abs_paths(version = setup_version)
} else if (endsWith(sys_info[["nodename"]], "arc4.leeds.ac.uk")) {
  uname <- sys_info[["user"]]
  setup <- PFUSetup::get_abs_paths(version = setup_version,
                                   home_path <- "/nobackup",
                                   dropbox_path = uname)
  # Set the location for the _targets folder.
  targets::tar_config_set(store = file.path(setup[["output_data_path"]], "_targets/"))
} else if ((sys_info[["sysname"]] == "Linux") && (sys_info[["user"]] == "eeear")){
  setup <- PFUSetup::get_abs_paths(version = setup_version)
  setup["pipeline_releases_folder"] <- "/home/eeear/Documents/Datasets/GPFU_database/Releases"
  setup[["aggregation_mapping_path"]] <- paste0("/home/eeear/Documents/Datasets/GPFU_database/InputData/",
                                                setup_version,
                                                "aggregation_mapping.xlsx")
  setup[["country_concordance_path"]] <- "inst/exiobase_data/Country_Concordance_Full.xlsx"
} else {
  stop("Unknown system in _targets.R for PFUAggPipeline. Can't set input and output locations.")
}

# Set up for multithreaded work on the local machine.
future::plan(future.callr::callr)

# Set options for all targets.
targets::tar_option_set(
  packages = "PFUAggPipeline",
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
PFUAggPipeline::get_pipeline(countries = countries,
                             years = years,
                             do_chops = do_chops,
                             psut_release = psut_release,
                             psut_without_neu_release = psut_without_neu_release,
                             phi_vecs_release = phi_vecs_release,
                             Y_fu_U_EIOU_fu_details_release = Y_fu_U_EIOU_fu_details_release,
                             aggregation_maps_path = setup[["aggregation_mapping_path"]],
                             pipeline_releases_folder = setup[["pipeline_releases_folder"]],
                             pipeline_caches_folder = setup[["pipeline_caches_folder"]],
                             release = release)




