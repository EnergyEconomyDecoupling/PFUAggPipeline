library(targets)
# Run targets::tar_make() to run the pipeline
# and targets::tar_read(summary) to view the results.

summ <- function(dataset) {
  summarize(dataset, mean_x = mean(x))
}

# Set target-specific options such as packages.
tar_option_set(packages = "dplyr")

get_plan <- function() {
  list(
    tar_target(data, data.frame(x = sample.int(100), y = sample.int(100))),
    tar_target(summary, summ(data)) # Call your custom functions as needed.
  )
}


# End this file with a list of target objects.
# list(
#   tar_target(data, data.frame(x = sample.int(100), y = sample.int(100))),
#   tar_target(summary, summ(data)) # Call your custom functions as needed.
# )


get_plan()
