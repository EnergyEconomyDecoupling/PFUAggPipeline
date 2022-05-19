#' PFUAggDatabase data frame column names
#'
#' A string list containing named names of columns in PFUAggDatabase data frames.
#' The data frames can be
#' tidy (with one row for each data point) or
#' wide (with years spread to the right).
#' Items in the list act to compliment the column names in `IEATools::iea_cols`.
#'
#' @format A string list with `r length(sea_cols)` entries.
#' \describe{
#' \item{stage_colname}{The name of a metadata column containing the stage of the energy conversion chain, usually "Primary", "Final", or "Useful".}
#' \item{gross_net_colname}{The name of a metadata column containing information as to whether aggregated data at the final and useful stage is in "Gross" or "Net" terms, see `Recca::finaldemand_aggregates()` and `Recca::primary_aggregates()`.}
#' \item{e_product}{The name of a metadata column containing the names of energy products.}
#' \item{sector_colname}{The name of a metadata column containing the names of final demand sectors.}
#' \item{flow_colname}{The name of a metadata column containing the names of primary flows.}
#' \item{agg_by_colname}{The name of a column containing the variable by which data was aggregated. Usually using `Recca::finaldemand_aggregates()` and `Recca::primary_aggregates()`, and usually one of "Flow", "Sector", "Product", or "Total".}
#' \item{fd_sectors_colname}{The name of a column containing the list of final demand sectors desired for analysis. Usually created by `PFUDatabase::get_fd_sectors()` and `PFUDatabase::create_fd_sectors_list()`.}
#' \item{p_ind_comp_colname}{The name of a column containing lists of primary industries desired for analysis. Usually created by using `Recca::find_p_industry_names()`.}
#' \item{p_ind_prefix_colname}{The name of a column containing the list of primary industry prefixes desired for analysis. Usually supplied to `Recca::find_p_industry_names()` to return `p_ind_comp`.}
#' \item{ex_colname}{The name of a column containing energy or exergy data.}
#' \item{ex_p_colname}{The name of a column containing energy or exergy data at the primary stage. Usually produced by `Recca::primary_aggregates()`.}
#' \item{ex_net_colname}{The name of a column containing energy or exergy data at the final and/or useful stage and in net terms. Usually produced by `Recca::finaldemand_aggregates()`.}
#' \item{ex_gross_colname}{The name of a column containing energy or exergy data at the final and/or useful stage and in gross terms. Usually produced by `Recca::finaldemand_aggregates()`.}
#' }
#'
#' @examples
#' sea_cols
"sea_cols"


#' Aggregation groups metadata information
#'
#' A string list containing values to be supplied to the metadata columns `PFUDatabase::sea_cols$e_product_colname`,
#' `PFUDatabase::sea_cols$agg_by_colname`, `PFUDatabase::sea_cols$sector_colname`, and `PFUDatabase::sea_cols$flow_colname`.
#'
#' @format A string list with `r length(agg_metadata)` entries.
#' \describe{
#' \item{total_value}{The string "Total" indicating that data has been aggregated across all products and sectors/flows. Supplied to `PFUDatabase::sea_cols$agg_by_colname`.}
#' \item{all_value}{The string "All" indicating that data has been aggregated across one or more of: "Product", "Flow", or "Sector". Supplied to one or more of `PFUDatabase::sea_cols$e_product_colname`, `PFUDatabase::sea_cols$sector_colname`, and `PFUDatabase::sea_cols$flow_colname` depending on the aggregation.}
#' \item{product_value}{The string "Product" indicating that data has been aggregated by product. Supplied to `PFUDatabase::sea_cols$agg_by_colname`.}
#' \item{sector_value}{The string "Sector" indicating that data has been aggregated by sector. Supplied to `PFUDatabase::sea_cols$agg_by_colname`.}
#' \item{flow_value}{The string "Flow" indicating that data has been aggregated by flow. Supplied to `PFUDatabase::sea_cols$agg_by_colname`.}
#' }
#'
#' @examples
#' agg_metadata
"agg_metadata"


#' Gross or Net metadata information
#'
#' A string list containing values indicating whether the output of the functions `Recca::finaldemand_aggregates`, `PFUDatabase::calculate_fu_ex_total`,
#' `PFUDatabase::calculate_fu_ex_product`, `PFUDatabase::calculate_fu_ex_sector`, and `PFUDatabase::calculate_finaluseful_ex_data`
#' are in Gross or Net terms. To be supplied to the metadata columns `PFUDatabase::sea_cols$gross_net_colname`.
#'
#' @format A string list with `r length(gross_net_metadata)` entries.
#' \describe{
#' \item{gross_value}{The string "Gross" indicating that final demand was calculated for both EIOU and non-EIOU sectors. See `Recca::finaldemand_aggregates`.}
#' \item{net_value}{The string "Net" indicating that final demand was calculated for only non-EIOU sectors. See `Recca::finaldemand_aggregates`.}
#' }
#'
#' @examples
#' gross_net_metadata
"gross_net_metadata"


#' PFUAggDatabase efficiency data frame column names
#'
#' A string list containing named names of columns in the efficiency data frame of the
#' PFUAggDatabase.
#'
#' @format A string list with `r length(efficiency_cols)` entries.
#' \describe{
#' \item{eta_pf}{The name of a column containing primary-to-final efficiencies.}
#' \item{eta_fu}{The name of a column containing final-to-useful efficiencies.}
#' \item{eta_pu}{The name of a column containing primary-to-useful efficiencies.}
#' }
#'
#' @examples
#' efficiency_cols
"efficiency_cols"


#' Output file information
#'
#' A string list containing named names of files and tabs for outputs from this pipeline.
#'
#' @format A string list with `r length(output_file_info)` entries.
#' \describe{
#' \item{agg_eta_filename}{The name of the Excel file containing aggregate energy and exergy values and aggregate efficiencies.}
#' \item{agg_tabname}{The name of the tab in `agg_eta_filename` containing aggregated energy and exergy values.}
#' \item{eta_tabname}{The name of the tab in `agg_eta_filename` containing aggregated efficiencies.}
#' }
#'
#' @examples
#' output_file_info
"output_file_info"


#' Aggregation file tabs
#'
#' A string list containing names of tabs for an Excel aggregation file.
#'
#' The aggregation file should have several tabs, one for each type of aggregation.
#'
#' @format A string list with `r length(aggregation_file_tab_names)` entries.
#' \describe{
#' \item{continent_aggregation_tab}{The name of the continent aggregation tab.}
#' \item{world_aggregation_tab}{The name of the world aggregation tab.}
#' \item{exiobase_region_aggregation_tab}{The name of the Exiobase region aggregation tab.}
#' \item{eu.product_aggregation_tab}{The name of the useful product aggregation tab.}
#' \item{ef.product_aggregation_tab}{The name of the final product aggregation tab.}
#' \item{destination_aggregation_tab}{The name of the destination aggregation tab.}
#' }
#'
#' @examples
#' aggregation_file_tab_names
"aggregation_file_tab_names"


#' Aggregation table column names
#'
#' A string list containing names of column names in aggregation tables.
#'
#' @format A string list with `r length(aggregation_file_cols)` entries.
#' \describe{
#' \item{many_colname}{The name of the many column in each tab.}
#' \item{few_colname}{The name of the few column in each tab.}
#' }
#'
#' @examples
#' aggregation_file_cols
"aggregation_file_cols"



