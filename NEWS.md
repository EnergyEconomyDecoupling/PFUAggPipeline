---
title: "Release notes for `PFUAggDatabase`"
output: html_document
---


* Updated targets store location.


# PFUAggDatabase 0.1.1 (2022-06-05) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8007869.svg)](https://doi.org/10.5281/zenodo.8007869)

* Preparing pipeline for execution on ARC facilities at Leeds University.
* Rearranged the order of operations for the pipeline.
  Now, chopping happens first.
  Despecifying and aggregating happens after.
  This change should make upstream and downstream swims
  more stable, numerically.
* Added a target and .csv pin final-to-useful efficiencies
  for final demand sectors.
* Simplified calculation of primary, final, and useful aggregates.
* Now parallelizing across all combinations of countries and years.
  Previously only parallelized across countries.
  Hopefully this fixes some memory issues.
* Now includes targets for product and industry aggregations.
* Now creates targets for a vector of pins from the `PFUDatabase`.


# PFUAggDatabase 0.1.0 (2022-04-15) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6463858.svg)](https://doi.org/10.5281/zenodo.6463858)

* First fully-working version where the pipeline
  completes in parallel on its own,
  without intervention or restarting.
* Fixed a nasty bug where the `Countries` target picked up
  information from a different environment.
  Solution was to wrap `countries` in a `list()`.
  Did the same for `years` to defend against similar problems.
* Now using `future` instead of `clustermq` for parallel processing,
  to avoid hangs when executing the pipeline.
* Add GitHub actions continuous integration.
* Add DOI badge to Readme.Rmd.


# PFUAggDatabase 0.0.1 (2022-04-03) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6409760.svg)](https://doi.org/10.5281/zenodo.6409760)

* Now saving important results to a pinboard.
* Now saving workflow cache to a .zip file.
* Added a new `years` variable that allows user-selection of years to be analyzed.
* Added efficiency calculations with target `write_agg_etas_xlsx`.
* Added spell-checking to the build process.
* Added primary, final, and useful energy and exergy targets
  that use all regional aggregations.
* Added regional aggregation targets
* Added a `NEWS.md` file to track changes to the package.
