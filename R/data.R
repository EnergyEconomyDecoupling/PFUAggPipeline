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
#' \item{product_sector}{The column containing values for chopped_var. Default is `Recca::aggregate_cols$product_sector`.}
#' }
#'
#' @examples
#' aggregation_df_cols
"aggregation_df_cols"
