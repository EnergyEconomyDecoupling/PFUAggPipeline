#
# Give the names of PFUDatabase columns, this function compliments "IEATools::iea_cols".
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


#
# Efficiency column names
#

efficiency_cols <- list(eta_pf = "eta_pf",
                        eta_fu = "eta_fu",
                        eta_pu = "eta_pu")
usethis::use_data(efficiency_cols, overwrite = TRUE)


#
# File and tab names
#

output_file_info <- list(agg_eta_filename = "AggregateEfficiencyResults.xlsx",
                         agg_tabname = "Aggregates",
                         eta_tabname = "etas")
usethis::use_data(output_file_info, overwrite = TRUE)


#
# Aggregation file information
#

aggregation_file_tab_names <- list(continent_aggregation = "continent_aggregation",
                                   world_aggregation = "world_aggregation",
                                   ef_product_aggregation = "ef_product_aggregation",
                                   eu_product_aggregation = "eu_product_aggregation",
                                   ef_sector_aggregation = "ef_sector_aggregation")
usethis::use_data(aggregation_file_tab_names, overwrite = TRUE)


aggregation_file_cols <- list(many_colname = "Many",
                              few_colname = "Few")
usethis::use_data(aggregation_file_cols, overwrite = TRUE)


#
# Aggregation data frame columns
#

aggregation_df_cols <- list(product_aggregation = "Product.Aggregation",
                            industry_aggregation = "Industry.Aggregation",
                            specified = "Specified",
                            despecified = "Despecified",
                            grouped = "Grouped",
                            chopped_mat = "Chopped.Mat",
                            chop_var = "Chop.Var",
                            product_sector = Recca::aggregate_cols$product_sector)
usethis::use_data(aggregation_df_cols, overwrite = TRUE)
