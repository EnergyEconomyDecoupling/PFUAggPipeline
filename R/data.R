#' Data frame column names
#'
#' A string list containing names of column names and values for aggregation data frames.
#'
#' @format A string list with `r length(sea_cols)` entries.
#' \describe{
#' \item{stage_colname}{The name of the metadata column that tells about the stage of the energy conversion chain. "Stage".}
#' \item{gross_net_colname}{The name of the metadata column that indicates gross or net energy. "Gross.Net"}
#' \item{e_product_colname}{The name of the metadata column that indicates energy products. "E.product".}
#' \item{e_sector_colname}{The name of the metadata column that indicates energy sectors. "Sector".}
#' \item{flow_colname}{The name of the metadata column that indicates IEA flows. "Flow".}
#' \item{agg_by_colname}{The name of the metadata column that indicates how things are aggregated. "Aggregation.by".}
#' \item{fd_sectors_colname}{The name of the metadata column that indicates final demand sectors. "Fd.sectors".}
#' \item{p_ind_comp_colname}{The name of the metadata column that indicates primary industries. "p_industries_colname".}
#' \item{p_ind_prefix_colname}{The name of the metadata column that indicates primary industry indices. "p_industry_prefixes".}
#' \item{ex_colname}{The name of the metadata column that indicates energy or exergy. "EX".}
#' }
#'
#' @examples
#' sea_cols
"sea_cols"



#' Aggregation metadata
#'
#' A string list containing aggregation metadata.
#'
#' @format A string list with `r length(agg_metadata)` entries.
#' \describe{
#' \item{total_value}{The string indicating aggregation totals. "Total".}
#' \item{all_value}{The string indicating all of something is aggregated. "All".}
#' \item{product_value}{The string indicating that products are aggregated. "Product".}
#' \item{sector_value}{The string indicating that sectors are aggregated. "Sector".}
#' \item{flow_value}{The string indicating that flows are aggregated. "Flow".}
#' \item{none}{The string indicating that no aggregations are present. "None".}
#' }
#'
#' @examples
#' agg_metadata
"agg_metadata"



#' Gross and net metadata
#'
#' A string list containing gross and net metadata.
#'
#' @format A string list with `r length(gross_net_metadata)` entries.
#' \describe{
#' \item{gross_value}{The string indicating gross aggregation. "Gross".}
#' \item{net_value}{The string indicating net aggregation. "Net".}
#' }
#'
#' @examples
#' gross_net_metadata
"gross_net_metadata"



#' Efficiency columns
#'
#' A string list containing names of efficiency columns.
#'
#' @format A string list with `r length(efficiency_cols)` entries.
#' \describe{
#' \item{eta_pf}{The string indicating primary-to-final efficiency. "eta_pf".}
#' \item{eta_fu}{The string indicating final-to-useful efficiency. "eta_fu".}
#' \item{eta_pu}{The string indicating primary-to-final efficiency. "eta_pu".}
#' }
#'
#' @examples
#' efficiency_cols
"efficiency_cols"



#' Output file information
#'
#' A string list containing the filename and tabs for output files.
#'
#' @format A string list with `r length(output_file_info)` entries.
#' \describe{
#' \item{agg_eta_filename}{A string value of the output file name. "AggregateEfficiencyResults.xlsx".}
#' \item{agg_tabname}{The string name of the aggregates tab. "Aggregates".}
#' \item{eta_tabname}{The string name of the efficiency tab. "etas".}
#' }
#'
#' @examples
#' output_file_info
"output_file_info"



#' Aggregation file tab information
#'
#' A string list containing the aggregation file's tab names.
#'
#' @format A string list with `r length(aggregation_file_tab_names)` entries.
#' \describe{
#' \item{continent_aggregation}{The name of the continent aggregation tab, "continent_aggregation".}
#' \item{world_aggregation}{The string name of the world aggregation tab. "world_aggregation".}
#' \item{ef_product_aggregation}{The string name of the final energy product aggregation tab. "ef_product_aggregation".}
#' \item{eu_product_aggregation}{The string name of the useful energy product aggregation tab. "eu_product_aggregation".}
#' \item{ef_sector_aggregation}{The string name of the final energy sector aggregation tab. "ef_sector_aggregation".}
#' }
#'
#' @examples
#' aggregation_file_tab_names
"aggregation_file_tab_names"



#' Aggregation file column names
#'
#' A string list containing aggregation file column names.
#'
#' @format A string list with `r length(aggregation_file_cols)` entries.
#' \describe{
#' \item{many_colname}{The string name of the many column. "Many".}
#' \item{few_colname}{The string name of the few column. "Few".}
#' }
#'
#' @examples
#' aggregation_file_cols
"aggregation_file_cols"



#' PFUAggDatabase data frame column names
#'
#' Aggregation data frame column names
#'
#' A string list containing names of column names and values for aggregation data frames.
#'
#' @format A string list with `r length(aggregation_df_cols)` entries.
#' \describe{
#' \item{product_aggregation}{The name of the metadata column that tells about product aggregation. "Product.Aggregation"}
#' \item{industry_aggregation}{The name of the metadata column that tells about industry aggregation. "Industry.Aggregation"}
#' \item{specified}{The value that indicates products or industries remain is specified. "Specified"}
#' \item{despecified}{The value that indicates products or industries have been despecified and aggregated. "Despecified"}
#' \item{ungrouped}{The value that indicates products or industries have not been grouped. "Ungrouped"}
#' \item{grouped}{The value that indicates products or industries have been grouped. "Grouped"}
#' \item{chopped_mat}{The value that indicates which matrix has been chopped. "Chopped.Mat"}
#' \item{chopped_var}{The value that indicates the chopping product or industry. "Chop.Var"}
#' \item{product_sector}{The column containing values for chopped_var. `Recca::aggregate_cols$product_sector`.}
#' }
#'
#' @examples
#' aggregation_df_cols
"aggregation_df_cols"
