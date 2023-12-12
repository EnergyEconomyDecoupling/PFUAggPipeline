---
title: "Release notes for `PFUAggDatabase`"
output: html_document
---

Cite all releases with doi [10.5281/zenodo.6409759](https://doi.org/10.5281/zenodo.6409759), 
which always resolves to the latest release.


* Add `Remotes:` field to `DESCRIPTION` to assist
  installation with metapackage `CLPFUDatabase`.


# PFUAggDatabase 0.1.5 (2023-12-10) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10345811.svg)](https://doi.org/10.5281/zenodo.10345811)

* Delete continuous integration workflow.
  The vignettes cannot be built on GitHub, 
  because input data are unavailable.
* Now creating efficiency data frames from both regular and "without NEU" 
  versions of PSUT data frame.


# PFUAggDatabase 0.1.4 (2023-11-27) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.10210406.svg)](https://doi.org/10.5281/zenodo.10210406)

* Fixed typos in README.Rmd and README.md stemming from earlier name change
  in response to JOSS paper review.
  Specifically, PFUAggWorkflow --> PFUAggDatabase.


# PFUAggDatabase 0.1.3 (2023-08-19) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8265840.svg)](https://doi.org/10.5281/zenodo.8265840)

* Fixed a bug where U_eiou wasn't in argument list 
  for a call to `Recca::finaldemand_aggregates()`.
* Added a new vignette that shows how to 
  read data from pins and make a graph of 
  country efficiencies coloured by continent.
* New function `rename_prime_cols()` deletes original matrices and 
  renames "*_prime" to original names.
  Used in the chopping process.
* Now testing for primary, final, and useful
  aggregations that should be equal.
  An nicely formatted error is thrown if they are not.


# PFUAggDatabase 0.1.2 (2023-06-06) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8011597.svg)](https://doi.org/10.5281/zenodo.8011597)

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
