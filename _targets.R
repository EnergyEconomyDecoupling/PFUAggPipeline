library(targets)
# targets::tar_make() to run the pipeline
# targets::tar_make_clustermq(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# targets::tar_destroy() to start over with everything,

# Set target-specific options such as packages.
targets::tar_option_set(
  # debug = "PSUT_Re_world",
  packages = c(
    "dplyr",
    "IEATools",
    "PFUAggDatabase",
    "pins",
    "tidyr"),
  # debug = "continent_table",
  resources = tar_resources(
    clustermq = tar_resources_clustermq(template = list(num_cores = 8))
  )
)
options(clustermq.scheduler = "multiprocess")

# countries <- c("WMB")
# countries <- c("USA", "CAN", "GBR", "PRT", "ZAF", "WMB", "WAB")
countries <- NULL


# Pull in the pipeline
PFUAggDatabase::get_pipeline(countries = countries,
                             psut_release = "20220225T012039Z-c2035",
                             psut_releases_folder = PFUSetup::get_abs_paths()[["workflow_releases_folder"]],
                             exemplar_table_path = PFUSetup::get_abs_paths()[["exemplar_table_path"]],
                             # world_agg_map needs to be a double-nested list, because the first layer
                             # is stripped off in the pipeline.
                             world_agg_map = list(list(WLD = c("AMR", "ASA", "EUR", "OCN", "AFR", "BNK")))
                             )
