library(targets)
# Run targets::tar_make() to run the pipeline
# and targets::tar_read(summary) to view the results.

# Set target-specific options such as packages.
tar_option_set(packages = "dplyr")


PFUAggDatabase::get_workflow()
