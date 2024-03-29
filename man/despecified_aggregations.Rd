% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/PrIn_aggregations.R
\name{despecified_aggregations}
\alias{despecified_aggregations}
\title{Despecify and aggregate all PSUT matrices}
\usage{
despecified_aggregations(
  .psut_data,
  countries,
  years,
  notation,
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

\item{notation}{The notations from which notation for row and column names can be inferred.}

\item{R, U, V, Y, r_eiou, U_eiou, U_feed, S_units}{The names of input columns in \code{.psut_data}.
Default values are from \code{Recca::psut_cols}.}

\item{R_aggregated_colname, U_aggregated_colname, V_aggregated_colname, Y_aggregated_colname, r_eiou_aggregated_colname, U_eiou_aggregated_colname, U_feed_aggregated_colname, S_units_aggregated_colname}{The names of output aggregated columns.}

\item{aggregated_suffix}{The suffix for columns of aggregated matrices.
Default is \code{Recca::aggregate_cols$aggregated_suffix}.}
}
\value{
A data frame with despecified and aggregated matrices.
}
\description{
This function uses \code{Recca::despecified_aggregates()} internally.
}
