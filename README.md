
<!-- README.md is generated from README.Rmd. Please edit that file -->

# PFUAggDatabase

<!-- README.md is generated from README.Rmd. Please edit README.Rmd. -->
<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/PFUAggDatabase)](https://cran.r-project.org/package=PFUAggDatabase)
[![Lifecycle:
experimental](https://img.shields.io/badge/lifecycle-experimental-orange.svg)](https://lifecycle.r-lib.org/articles/stages.html#experimental)
[![Project Status: Active – The project has reached a stable, usable
state and is being actively
developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.6409759.svg)](https://doi.org/10.5281/zenodo.6409759)
<!-- badges: end -->

The goal of PFUAggDatabase is to aggregate data in the `PFUDatabase`.
Analyses are completed using the
[targets](https://github.com/ropensci/targets) environment which
provides helpful dependency management for the calculation pipeline.

## Installation

You can install the development version of `PFUAggDatabase` from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("EnergyEconomyDecoupling/PFUAggDatabase")
```

## Quick start

At the RStudio console, type

``` r
library(targets)              # to load the targets package   
tar_visnetwork()              # to see a directed acyclic graph of the calculations that will take place   
tar_make_future(workers = 2)  # to execute the calculations (or `workers = 8`, if you have enough cores)
```

### Accessing targets

`targets::tar_read(<<target>>)` pulls the value of a target out of the
`targets` cache. (`<<target>>` should be an unquoted symbol such as
`Specified`.)

### Fresh start

`targets::tar_destroy()` invalidates the `targets` cache and forces
reanalysis of everything. Reanalyzing everything may take a while.

## Example

See the vignette entitled “Access PFU Database Products Via `Pins`”.

## More Information

For information about the `targets` package, see the [targets
manual](https://books.ropensci.org/targets/).

For documentation on the `PFUAggDatabase` package, see
<https://EnergyEconomyDecoupling.github.io/PFUAggDatabase/>.
