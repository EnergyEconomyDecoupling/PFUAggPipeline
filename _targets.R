library(targets)
# targets::tar_make() to run the pipeline
# targets::tar_make_clustermq(workers = 8) to execute across multiple cores.
# targets::tar_read(<<target_name>>) to view the results.
# targets::tar_destroy() to start over with everything,

# Set target-specific options such as packages.
targets::tar_option_set(
  packages = "dplyr",
  resources = tar_resources(
    clustermq = tar_resources_clustermq(template = list(num_cores = 2))
  )
)
options(clustermq.scheduler = "multiprocess")


# Pull in the pipeline
PFUAggDatabase::get_pipeline(psut_release = "20220225T012039Z-c2035",
                             psut_releases_folder = PFUSetup::get_abs_paths()[["workflow_releases_folder"]])
