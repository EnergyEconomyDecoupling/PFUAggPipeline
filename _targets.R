library(targets)
# targets::tar_make() to run the pipeline
# targets::tar_make_clustermq(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# targets::tar_destroy() to start over with everything,

# Set control parameters for the pipeline.

# Set the countries to be analyzed.
countries <- c("WMB", "WAB")
# countries <- c("USA", "CAN", "GBR", "PRT", "ZAF", "WMB", "WAB")
# countries <- "all" # Run all countries

# Set the release of PSUT to be used.
psut_release <- "20220225T012039Z-c2035"

# world_agg_map needs to be a double-nested list, because the first layer
# is stripped off by the targets pipeline.
world_agg_map <- list(list(WLD = c("AMR", "ASA", "EUR", "OCN", "AFR", "BNK")))

# Number of machine cores to use. Set to less than available on your machine.
# Applies only to tar_make_clustermq()
num_cores <- 8

# Set the target to debug.  Set to NULL to turn off debugging.
# To debug, set appropriate breakpoints and use
# tar_make(callr_function = NULL).
debug_target <- "continent_aggregation_map"


# End user-adjustable parameters.





# Set up for multithreaded work on the local machine.
options(clustermq.scheduler = "multiprocess")

# Set options for the targets package.
targets::tar_option_set(

  # Set the target to debug, if needed.
  debug = debug_target,

  # Set packages to be used.
  packages = c(
    "dplyr",
    "IEATools",
    "PFUAggDatabase",
    "pins",
    "tidyr"),

  # Set the number of cores for multiprocessing.
  resources = targets::tar_resources(
    clustermq = targets::tar_resources_clustermq(template = list(num_cores = num_cores))
  )
)

# Pull in the pipeline
PFUAggDatabase::get_pipeline(countries = countries,
                             psut_release = psut_release,
                             psut_releases_folder = PFUSetup::get_abs_paths()[["workflow_releases_folder"]],
                             exemplar_table_path = PFUSetup::get_abs_paths()[["exemplar_table_path"]],
                             world_agg_map = world_agg_map)
