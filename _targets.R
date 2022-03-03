library(targets)
# Run targets::tar_make() to run the pipeline
# Rung targets::tar_make_clustermq(workers = 8) to execute across multiple cores.
# and targets::tar_read(<<target_name>>) to view the results.

# Set target-specific options such as packages.
targets::tar_option_set(
  packages = "dplyr",
  resources = tar_resources(
    clustermq = tar_resources_clustermq(template = list(num_cores = 2))
  )
)
options(clustermq.scheduler = "multiprocess")


PFUAggDatabase::get_workflow()
