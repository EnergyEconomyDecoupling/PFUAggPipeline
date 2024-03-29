% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/PrIn_aggregations.R
\name{grouped_aggregations}
\alias{grouped_aggregations}
\title{Aggregate PSUT matrices by row and column groups}
\usage{
grouped_aggregations(
  .psut_data,
  countries,
  years,
  aggregation_map,
  margin = list(c(1, 2)),
  pattern_type = "exact",
  R = Recca::psut_cols$R,
  U = Recca::psut_cols$U,
  V = Recca::psut_cols$V,
  Y = Recca::psut_cols$Y,
  r_eiou = Recca::psut_cols$r_eiou,
  U_eiou = Recca::psut_cols$U_eiou,
  U_feed = Recca::psut_cols$U_feed,
  S_units = Recca::psut_cols$S_units,
  R_aggregated_colname = paste0(Recca::psut_cols$R, aggregated_suffix),
  U_aggregated_colname = paste0(Recca::psut_cols$U, aggregated_suffix),
  V_aggregated_colname = paste0(Recca::psut_cols$V, aggregated_suffix),
  Y_aggregated_colname = paste0(Recca::psut_cols$Y, aggregated_suffix),
  r_eiou_aggregated_colname = paste0(Recca::psut_cols$r_eiou, aggregated_suffix),
  U_eiou_aggregated_colname = paste0(Recca::psut_cols$U_eiou, aggregated_suffix),
  U_feed_aggregated_colname = paste0(Recca::psut_cols$U_feed, aggregated_suffix),
  S_units_aggregated_colname = paste0(Recca::psut_cols$S_units, aggregated_suffix),
  aggregated_suffix = Recca::aggregate_cols$aggregated_suffix
)
}
\arguments{
\item{.psut_data}{A data frame of PSUT matrices.}

\item{countries}{The countries to be analyzed.}

\item{years}{The years to be analyzed.}

\item{aggregation_map}{A named list of rows or columns to be aggregated (or \code{NULL}).}

\item{margin}{\code{1}, \code{2}, or \code{c(1, 2)} for row aggregation, column aggregation, or both.
As a string, \code{margin} can be a row or column type.
Default is \code{c(1, 2)}.}

\item{pattern_type}{See \code{RCLabels::make_or_pattern()}.
Default is "exact".}

\item{R, U, V, Y, r_eiou, U_eiou, U_feed, S_units}{The names of input columns in \code{.psut_data}.
Default values are taken from \code{Recca::psut_cols}.}

\item{R_aggregated_colname, U_aggregated_colname, V_aggregated_colname, Y_aggregated_colname, r_eiou_aggregated_colname, U_eiou_aggregated_colname, U_feed_aggregated_colname, S_units_aggregated_colname}{The names of output aggregated columns.}

\item{aggregated_suffix}{The suffix for columns of aggregated matrices.
Default is \code{Recca::aggregate_cols$aggregated_suffix}.}
}
\value{
A version of \code{.psut_data} with aggregated rows or columns.
}
\description{
Aggregating to groups of rows or columns can be accomplished by an \code{aggregation_map}.
Internally, this function calls \code{Recca::grou_aggregates()}, so
arguments \code{aggregation_map}, \code{margin}, and \code{pattern_type} control its behavior.
}
