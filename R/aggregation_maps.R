#' Load aggregation maps
#'
#' Load aggregation tables and create aggregation maps
#' for use in the workflow.
#'
#' An aggregation table consists of a many column and a few column.
#' An aggregation map is a named list of items, where the names are the few
#' and the items are the many.
#'
#' @param path The path to an Excel file containing aggregation tables.
#' @param aggregation_file_tabs A list of tabs in the aggregation file.
#'                              The names are the programmatic names for the tabs.
#'                              The items are the actual names.
#'                              Default is `PFUAggPipeline::aggregation_file_tabs`.
#' @param many_colname The name for the many column in the aggregation tables.
#'                     Default is `PFUAggPipeline::aggregation_file_info$many_colname`.
#' @param few_colname The name for the few column in the aggregation tables.
#'                    Default is `PFUAggPipeline::aggregation_file_info$few_colname`.
#'
#' @return A named list of aggregation maps.
#'         The names give the aggregation map.
#'         The items are the aggregation maps themselves.
#'
#' @export
load_aggregation_maps <- function(path,
                                  aggregation_file_tabs = PFUAggPipeline::aggregation_file_tab_names,
                                  many_colname = PFUAggPipeline::aggregation_file_cols$many_colname,
                                  few_colname = PFUAggPipeline::aggregation_file_cols$few_colname) {

  lapply(aggregation_file_tabs, function(sheet) {
    readxl::read_excel(path = path, sheet = sheet) %>%
      matsbyname::agg_table_to_agg_map(many_colname = many_colname, few_colname = few_colname)
  }) %>%
    magrittr::set_names(names(aggregation_file_tabs))
}

