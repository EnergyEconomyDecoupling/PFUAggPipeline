#
# Give the names of PFUWorkflow columns, this function compliments "IEATools::iea_cols".
#

sea_cols <- list(stage_colname = "Stage",
                 gross_net_colname = "Gross.Net",
                 e_product_colname = "E.product",
                 sector_colname = "Sector",
                 flow_colname = "Flow",
                 agg_by_colname = "Aggregation.by",
                 fd_sectors_colname = "Fd.sectors",
                 p_ind_comp_colname = "p_industries_complete",
                 p_ind_prefix_colname = "p_industry_prefixes",
                 ex_colname = "EX")
usethis::use_data(sea_cols, overwrite = TRUE)


#
# Metadata information for aggregation groups
#

agg_metadata <- list(total_value = "Total",
                     all_value = "All",
                     product_value = "Product",
                     sector_value = "Sector",
                     flow_value = "Flow")
usethis::use_data(agg_metadata, overwrite = TRUE)


#
# Metadata information for gross or net
#

gross_net_metadata <- list(gross_value = "Gross",
                           net_value = "Net")
usethis::use_data(gross_net_metadata, overwrite = TRUE)


#' Gross or Net metadata information
#'
#' A string list containing values indicating whether the output of the functions `Recca::finaldemand_aggregates`, `PFUWorkflow::calculate_fu_ex_total`,
#' `PFUWorkflow::calculate_fu_ex_product`, `PFUWorkflow::calculate_fu_ex_sector`, and `PFUWorkflow::calculate_finaluseful_ex_data`
#' are in Gross or Net terms. To be supplied to the metadata columns `PFUWorkflow::sea_cols$gross_net_colname`.
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

