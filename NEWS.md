---
title: "Release notes for `PFUAggDatabase`"
output: html_document
---


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
