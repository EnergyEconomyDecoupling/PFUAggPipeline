
#' Create a data frame containing primary aggregate energy/exergy data
#'
#' This functions creates a single data frame containing the total energy/exergy by country,
#' year, method, energy quantification, and grouping variable (Total, Product, and Flow),
#' for the Primary stage using the functions:
#' `calculate_p_ex_total`, `calculate_p_ex_flow`, `calculate_p_ex_product`,
#' and binding the outputs of these functions into a single data frame.
#'
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices with associated final demand sector names
#' @param countries The countries for which primary energy and exergy data are to be calculated.
#' @param years The years for which primary energy and exergy data are to be calculated.
#' @param p_industry_prefixes A character vector of primary energy industry prefixes.
#'                            Usually "Resources", "Imports", and "Stock changes".
#' @param country The name of the country column in `.sutdata`.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column in `.sutdata`.
#'             Default is `IEATools::iea_cols$year`.
#' @param ex The name of the energy and exergy column in `.sutdata`.
#'           Default is `PFUAggDatabase::sea_cols$ex_colname`.
#'
#' @return A data frame containing primary energy/exergy values aggregated by total,
#'         flow and product.
#'
#' @export
#'
#' @examples
#' library(Recca)
#' primary_data <- Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_primary_ex_data(countries = "all",
#'                             years = "all",
#'                             p_industry_prefixes = list(c("Resources", "Imports")))
calculate_primary_ex_data <- function(.sutdata,
                                      countries,
                                      years,
                                      p_industry_prefixes,
                                      country = IEATools::iea_cols$country,
                                      year = IEATools::iea_cols$year,
                                      ex = PFUAggDatabase::sea_cols$ex_colname) {
  filtered_sut_data <- .sutdata %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years,
                                        country = country, year = year)

  # Calculate total primary energy/exergy
  p_total <- calculate_p_ex_total(.sutdata = filtered_sut_data, p_industry_prefixes = p_industry_prefixes)

  # Calculate primary energy/exergy by flow
  p_flow <- calculate_p_ex_flow(.sutdata = filtered_sut_data, p_industry_prefixes = p_industry_prefixes)

  # Calculate primary energy/exergy by product
  p_product <- calculate_p_ex_product(.sutdata = filtered_sut_data, p_industry_prefixes = p_industry_prefixes)

  # Bind all data together and ensure numeric column for aggregates.
  dplyr::bind_rows(p_total, p_flow, p_product) %>%
    dplyr::mutate(
      "{ex}" := as.numeric(.data[[ex]])
    )
}


#' Calculate total energy supply
#'
#' Calculate the total energy supply (TES) in primary energy or exergy terms.
#' This metric was formerly called the total primary energy supply (TPES) by the IEA.
#' This function first uses the uses `Recca::find_p_industry_names()` function,
#' with a user-supplied set of primary industry prefixes `p_industry_prefixes`
#' to identify the primary industries desired for analysis.
#' The `Recca::primary_aggregates()` function is then applied to the `.sutdata` data frame by total,
#' to calculate the total energy supply across all products and flows.
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT) matrices.
#' @param p_industry_prefixes A character vector of primary energy industry prefixes.
#'                            Usually "Resources", "Imports", and "Stock changes".
#' @param country_colname,method_colname,energy_type_colname,year_colname See `IEATools::iea_cols`.
#' @param flow_colname,e_product_colname,stage_colname,gross_net_colname,agg_by_colname,p_ind_comp_colname,p_ind_prefix_colname,ex_colname,ex_p_colname See `PFUAggDatabase::sea_cols`.
#' @param primary_value The string "Primary", representing the Primary stage of the energy conversion chain, see `IEATools::all_stages`.
#' @param all_value,total_value See `PFUAggDatabase::agg_metadata`.
#'
#' @return A data frame containing aggregate primary energy/exergy data by total (total energy supply (TES)).
#'
#' @export
#'
#' @examples
#' library(Recca)
#' total_energy_supply <- Recca::UKEnergy2000mats %>%
#' tidyr::pivot_wider(names_from = matrix.name,
#'                    values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_p_ex_total(p_industry_prefixes = list(c("Resources", "Imports")))
calculate_p_ex_total <- function(.sutdata,
                                 p_industry_prefixes,
                                 country_colname = IEATools::iea_cols$country,
                                 method_colname = IEATools::iea_cols$method,
                                 energy_type_colname = IEATools::iea_cols$energy_type,
                                 year_colname = IEATools::iea_cols$year,
                                 flow_colname = PFUAggDatabase::sea_cols$flow_colname,
                                 e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                                 stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                                 gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                                 agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                                 p_ind_comp_colname = PFUAggDatabase::sea_cols$p_ind_comp_colname,
                                 p_ind_prefix_colname = PFUAggDatabase::sea_cols$p_ind_prefix_colname,
                                 ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                                 ex_p_colname = Recca::aggregate_cols$aggregate_primary,
                                 primary_value = IEATools::all_stages$primary,
                                 all_value = PFUAggDatabase::agg_metadata$all_value,
                                 total_value = PFUAggDatabase::agg_metadata$total_value) {

  # Adds primary industry name prefixes to DF and creates a complete list of
  # primary industries
  PSUT_DF_p <- .sutdata %>%
    dplyr::mutate(
      "{p_ind_prefix_colname}" := p_industry_prefixes
    ) %>%
    Recca::find_p_industry_names() %>%
    dplyr::relocate(.data[[p_ind_comp_colname]], .after = .data[[p_ind_prefix_colname]]) %>%

    # Removes duplicate entries. Primary energy/exergy data stored in R matrices
    # are the same for each of the final, useful and services stages
    dplyr::distinct(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .keep_all = TRUE) # Last.stage???

  # Call Recca::primary_aggregates() to obtain the IEA version of aggregate primary energy
  # from the R, V, and Y matrices (which includes imported final energy, effect of bunkers),
  p_total <- PSUT_DF_p %>%
    Recca::primary_aggregates(p_industries = p_ind_comp_colname,
                              by = total_value) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .data[[ex_p_colname]]) %>%
    magrittr::set_colnames(c(country_colname, method_colname, energy_type_colname, year_colname, ex_colname))

  # Add additional metadata columns
  p_total %>%
    dplyr::mutate(
      "{stage_colname}" := primary_value,
      "{gross_net_colname}" := NA,
      "{e_product_colname}" := all_value,
      "{flow_colname}" := all_value,
      "{agg_by_colname}" := total_value,
      "{ex_colname}" := as.numeric(.data[[ex_colname]])
    ) %>%
    dplyr::relocate(.data[[year_colname]], .after = .data[[agg_by_colname]]) %>%
    dplyr::relocate(.data[[ex_colname]], .after = .data[[year_colname]])
}


#' Calculate total primary energy by flow
#'
#' Calculate the total energy supply (TES) in primary energy terms by flow. This metric
#' was formerly called the total primary energy supply (TPES).
#' This function first uses the uses `Recca::find_p_industry_names()` function,
#' with a user-supplied set of primary industry prefixes `p_industry_prefixes`
#' to identify the primary industries desired for analysis.
#' The `Recca::primary_aggregates()` function is then applied to `.sutdata`
#' data frame by flow, to calculate the total energy supply across all products
#' for each flow.
#'
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices.
#' @param p_industry_prefixes A character vector of primary energy industry prefixes.
#'                            Usually "Resources", "Imports", and "Stock changes".
#' @param country_colname,method_colname,energy_type_colname,year_colname See `IEATools::iea_cols`.
#' @param flow_colname,e_product_colname,stage_colname,gross_net_colname,agg_by_colname,p_ind_comp_colname,p_ind_prefix_colname,ex_colname,ex_p_colname See `PFUAggDatabase::sea_cols`.
#' @param primary_value The string "Primary", representing the Primary stage of the energy conversion chain, see `IEATools::all_stages`.
#' @param all_value,flow_value See `PFUAggDatabase::agg_metadata`.
#'
#' @return A data frame containing aggregate primary energy/exergy data by flow.
#'
#' @export
#'
#' @examples
#' library(Recca)
#' total_energy_supply <- Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_p_ex_flow(p_industry_prefixes = list(c("Resources", "Imports")))
calculate_p_ex_flow <- function(.sutdata, p_industry_prefixes,
                                country_colname = IEATools::iea_cols$country,
                                method_colname = IEATools::iea_cols$method,
                                energy_type_colname = IEATools::iea_cols$energy_type,
                                year_colname = IEATools::iea_cols$year,
                                flow_colname = PFUAggDatabase::sea_cols$flow_colname,
                                e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                                stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                                gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                                agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                                p_ind_comp_colname = PFUAggDatabase::sea_cols$p_ind_comp_colname,
                                p_ind_prefix_colname = PFUAggDatabase::sea_cols$p_ind_prefix_colname,
                                ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                                ex_p_colname = Recca::aggregate_cols$aggregate_primary,
                                primary_value = IEATools::all_stages$primary,
                                all_value = PFUAggDatabase::agg_metadata$all_value,
                                flow_value = PFUAggDatabase::agg_metadata$flow_value) {

  # Adds primary industry name prefixes to DF and creates a complete list of
  # primary industries
  PSUT_DF_p <- .sutdata %>%
    dplyr::mutate(
      "{p_ind_prefix_colname}" := p_industry_prefixes
    ) %>%
    Recca::find_p_industry_names() %>%
    dplyr::relocate(.data[[p_ind_comp_colname]], .after = .data[[p_ind_prefix_colname]]) %>%

    # Removes duplicate entries. Primary energy/exergy is the same regardless of whether
    # it is at the final, useful, or services stage as it is calculated from the same matrices
    dplyr::distinct(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .keep_all = TRUE)

  # Call Recca::primary_aggregates() to obtain the IEA version of aggregate primary energy
  # from the R, V, and Y matrices (which includes imported final energy, effect of bunkers),
  p_flow <- PSUT_DF_p %>%
    Recca::primary_aggregates(p_industries = p_ind_comp_colname,
                              by = flow_colname) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .data[[ex_p_colname]]) %>%
    magrittr::set_colnames(c(country_colname, method_colname, energy_type_colname, year_colname, ex_colname))

  # Expands matrices
  p_flow_expanded <- p_flow %>%
    matsindf::expand_to_tidy(matvals = ex_colname,
                             colnames = flow_colname) %>% # Check
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .data[[flow_colname]], .data[[ex_colname]])

  # Add additional metadata columns
  p_flow_expanded %>%
    dplyr::mutate(
      "{stage_colname}" := primary_value,
      "{gross_net_colname}" := NA,
      "{e_product_colname}" := all_value,
      "{agg_by_colname}" := flow_value,
      "{ex_colname}" := as.numeric(.data[[ex_colname]])
    ) %>%
    dplyr::relocate(.data[[flow_colname]], .after = .data[[e_product_colname]]) %>%
    dplyr::relocate(.data[[year_colname]], .after = .data[[agg_by_colname]]) %>%
    dplyr::relocate(.data[[ex_colname]], .after = .data[[year_colname]])
}


#' Calculate total primary energy by product
#'
#' Calculate the total energy supply (TES) in primary energy terms by product. This metric
#' was formerly called the total primary energy supply (TPES).
#' This function first uses the uses `Recca::find_p_industry_names()` function,
#' with a user-supplied set of primary industry prefixes `p_industry_prefixes`
#' to identify the primary industries desired for analysis.
#' The `Recca::primary_aggregates()` function is then applied to `.sutdata`
#' data frame by product, to calculate the total energy supply across all flows
#' for each product.
#'
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices.
#' @param p_industry_prefixes A character vector of primary energy industry prefixes.
#'                            Usually "Resources", "Imports", and "Stock changes".
#' @param country_colname,method_colname,energy_type_colname,year_colname See `IEATools::iea_cols`.
#' @param flow_colname,e_product_colname,stage_colname,gross_net_colname,agg_by_colname,p_ind_comp_colname,p_ind_prefix_colname,ex_colname,ex_p_colname See `PFUAggDatabase::sea_cols`.
#' @param primary_value The string "Primary", representing the Primary stage of the energy conversion chain, see `IEATools::all_stages`.
#' @param all_value,product_value See `PFUAggDatabase::agg_metadata`.
#'
#' @return A data frame containing aggregate primary energy/exergy data by product.
#'
#' @export
#'
#' @examples
#' library(Recca)
#' total_energy_supply <- Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_p_ex_product(p_industry_prefixes = list(c("Resources", "Imports")))
calculate_p_ex_product <- function(.sutdata, p_industry_prefixes,
                                   country_colname = IEATools::iea_cols$country,
                                   method_colname = IEATools::iea_cols$method,
                                   energy_type_colname = IEATools::iea_cols$energy_type,
                                   year_colname = IEATools::iea_cols$year,
                                   flow_colname = PFUAggDatabase::sea_cols$flow_colname,
                                   e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                                   stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                                   gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                                   agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                                   p_ind_comp_colname = PFUAggDatabase::sea_cols$p_ind_comp_colname,
                                   p_ind_prefix_colname = PFUAggDatabase::sea_cols$p_ind_prefix_colname,
                                   ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                                   ex_p_colname = Recca::aggregate_cols$aggregate_primary,
                                   primary_value = IEATools::all_stages$primary,
                                   all_value = PFUAggDatabase::agg_metadata$all_value,
                                   product_value = PFUAggDatabase::agg_metadata$product_value) {

  # Adds primary industry name prefixes to DF and creates a complete list of
  # primary industries
  PSUT_DF_p <- .sutdata %>%
    dplyr::mutate(
      "{p_ind_prefix_colname}" := p_industry_prefixes
    ) %>%
    Recca::find_p_industry_names() %>%
    dplyr::relocate(.data[[p_ind_comp_colname]], .after = .data[[p_ind_prefix_colname]]) %>%

    # Removes duplicate entries. Primary energy/exergy is the same regardless of whether
    # it is at the final, useful, or services stage as it is calculated from the same matrices
    dplyr::distinct(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .keep_all = TRUE)

  # Call Recca::primary_aggregates() to obtain the IEA version of aggregate primary energy
  # from the R, V, and Y matrices (which includes imported final energy, effect of bunkers),
  p_product <- PSUT_DF_p %>%
    Recca::primary_aggregates(p_industries = p_ind_comp_colname,
                              by = product_value) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .data[[ex_p_colname]]) %>%
    magrittr::set_colnames(c(country_colname, method_colname, energy_type_colname, year_colname, ex_colname))

  # Expands matrices
  p_product_expanded <- p_product %>%
    matsindf::expand_to_tidy(matvals = ex_colname,
                             rownames = e_product_colname) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[year_colname]], .data[[e_product_colname]], .data[[ex_colname]])

  # Add additional metadata columns
  p_product_expanded %>%
    dplyr::mutate(
      "{stage_colname}" := primary_value,
      "{gross_net_colname}" := NA,
      "{flow_colname}" := all_value,
      "{agg_by_colname}" := product_value,
      "{ex_colname}" := as.numeric(.data[[ex_colname]])
    ) %>%
    dplyr::relocate(.data[[e_product_colname]], .after = .data[[gross_net_colname]]) %>%
    dplyr::relocate(.data[[year_colname]], .after = .data[[agg_by_colname]]) %>%
    dplyr::relocate(.data[[ex_colname]], .after = .data[[year_colname]])
}


#' Create a data frame containing final and useful aggregate energy/exergy data
#'
#' This functions creates a single data frame containing final and useful
#' energy/exergy by country, year, method, energy quantification,
#' and grouping variable (Total, Product, and Sector) using the functions:
#' `calculate_fu_ex_total`, `calculate_fu_ex_sector`, `calculate_fu_ex_product`
#' and binding the outputs of these functions into a single data frame.
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices with associated final demand sector names
#' @param countries The countries for which primary energy and exergy data are to be calculated.
#' @param years The years for which primary energy and exergy data are to be calculated.
#' @param fd_sectors A character vector of final demand sectors.
#' @param country The name of the country column in `.sutdata`.
#'                Default is `IEATools::iea_cols$country`.
#' @param year The name of the year column in `.sutdata`.
#'             Default is `IEATools::iea_cols$year`.
#' @param ex The name of the energy and exergy column in `.sutdata`.
#'           Default is `PFUAggDatabase::sea_cols$ex_colname`.
#'
#' @return A data frame containing final and useful energy/exergy values aggregated by total,
#'         sector and product.
#'
#' @export
#'
#' @examples
#' library(Recca)
#' Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_finaluseful_ex_data(countries = "all",
#'                                 years = "all",
#'                                 fd_sectors = c("Residential"))
calculate_finaluseful_ex_data <- function(.sutdata,
                                          countries,
                                          years,
                                          fd_sectors,
                                          country = IEATools::iea_cols$country,
                                          year = IEATools::iea_cols$year,
                                          ex = PFUAggDatabase::sea_cols$ex_colname) {

  filtered_sut_data <- .sutdata %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years,
                                        country = country, year = year)

  # Calculates total final demand of energy/exergy
  fu_total <- calculate_fu_ex_total(.sutdata = filtered_sut_data, fd_sectors = fd_sectors)

  # Calculates final demand of energy/exergy by sector
  fu_sector <- calculate_fu_ex_sector(.sutdata = filtered_sut_data, fd_sectors = fd_sectors)

  # Calculates final demand of energy/exergy by product
  fu_product <- calculate_fu_ex_product(.sutdata = filtered_sut_data, fd_sectors = fd_sectors)

  # Bind all data together
  dplyr::bind_rows(fu_total, fu_sector, fu_product) %>%
    dplyr::mutate(
      "{ex}" := as.numeric(.data[[ex]])
    )
}


#' Calculate total final consumption of final and useful energy
#'
#' Calculate the total final consumption (TFC) at the final and useful stages
#' (along with any additional stages).
#' This function first uses the uses `create_fd_sectors_list()` function,
#' with a user-supplied set of final demand sectors `fd_sectors`
#' to identify the final demand sectors desired for analysis.
#' The `Recca::finaldemand_aggregates()` function is then applied to `.sutdata`
#' data frame, to calculate the total final consumption across all products and sectors.
#'
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices with associated final demand sector names
#' @param fd_sectors A character vector of final demand sectors.
#'
#' @param country_colname,method_colname,energy_type_colname,last_stage_colname,year_colname See `IEATools::iea_cols`.
#' @param sector_colname,fd_sectors_colname,e_product_colname,stage_colname,gross_net_colname,agg_by_colname,ex_colname,ex_net_colname,ex_gross_colname See `PFUAggDatabase::sea_cols`.
#' @param net_value,gross_value See `PFUAggDatabase::gross_net_metadata`.
#' @param all_value,total_value See `PFUAggDatabase::agg_metadata`.
#'
#' @return A data frame containing aggregate final and useful energy/exergy data by total
#' @export
#'
#' @examples
#' library(Recca)
#' tfc_total <- Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_fu_ex_total(fd_sectors = c("Residential"))
calculate_fu_ex_total <- function(.sutdata,
                                  fd_sectors,
                                  country_colname = IEATools::iea_cols$country,
                                  method_colname = IEATools::iea_cols$method,
                                  energy_type_colname = IEATools::iea_cols$energy_type,
                                  last_stage_colname = IEATools::iea_cols$last_stage,
                                  year_colname = IEATools::iea_cols$year,
                                  sector_colname = PFUAggDatabase::sea_cols$sector_colname,
                                  fd_sectors_colname = PFUAggDatabase::sea_cols$fd_sectors_colname,
                                  e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                                  stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                                  gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                                  agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                                  ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                                  ex_net_colname = Recca::aggregate_cols$net_aggregate_demand,
                                  ex_gross_colname = Recca::aggregate_cols$gross_aggregate_demand,
                                  net_value = PFUAggDatabase::gross_net_metadata$net_value,
                                  gross_value = PFUAggDatabase::gross_net_metadata$gross_value,
                                  all_value = PFUAggDatabase::agg_metadata$all_value,
                                  total_value = PFUAggDatabase::agg_metadata$total_value
) {

  # Creates a list of the final demand sector list equal to the length of the supplied data frame
  fd_sector_list <- create_fd_sectors_list(fd_sectors = fd_sectors, .sutdata = .sutdata)

  # Adds a column which each observation containing the list of final demand sectors
  PSUT_DF_fu <- .sutdata %>%
    dplyr::mutate(
      "{fd_sectors_colname}" := fd_sector_list
    )

  # Calculates final demand by total
  fu_total <- PSUT_DF_fu %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors_colname, by = total_value) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[last_stage_colname]], .data[[year_colname]], .data[[ex_net_colname]], .data[[ex_gross_colname]]) %>%
    magrittr::set_colnames(c(country_colname, method_colname, energy_type_colname, stage_colname, year_colname, ex_net_colname, ex_gross_colname)) %>%
    tidyr::pivot_longer(cols = ex_net_colname:ex_gross_colname,
                        names_to = gross_net_colname,
                        values_to = ex_colname) %>%
    dplyr::mutate(
      "{gross_net_colname}" := stringr::str_replace(.data[[gross_net_colname]], ex_net_colname, net_value)
    ) %>%
    dplyr::mutate(
      "{gross_net_colname}" := stringr::str_replace(.data[[gross_net_colname]], ex_gross_colname, gross_value)
    ) %>%
    dplyr::relocate(.data[[gross_net_colname]], .after = .data[[stage_colname]])

  # Add additional metadata columns
  fu_total %>%
    dplyr::mutate(
      "{e_product_colname}" := all_value,
      "{sector_colname}" := all_value,
      "{agg_by_colname}" := total_value,
      "{ex_colname}" := as.numeric(.data[[ex_colname]])
    ) %>%
    dplyr::relocate(.data[[year_colname]], .after = .data[[agg_by_colname]]) %>%
    dplyr::relocate(.data[[ex_colname]], .after = .data[[year_colname]])
}


#' Calculate total final consumption of final and useful energy by product
#'
#' Calculate the total final consumption (TFC) at the final and useful stages
#' (along with any additional stages) by product.
#' This function first uses the uses `create_fd_sectors_list()` function,
#' with a user-supplied set of final demand sectors `fd_sectors`
#' to identify the final demand sectors desired for analysis.
#' The `Recca::finaldemand_aggregates()` function is then applied to `.sutdata`
#' data frame by product, to calculate the total final consumption across
#' all of the sectors supplied in `fd_sectors` for each product.
#'
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices with associated final demand sector names
#' @param fd_sectors A character vector of final demand sectors.
#'
#' @param country_colname,method_colname,energy_type_colname,last_stage_colname,year_colname See `IEATools::iea_cols`.
#' @param sector_colname,fd_sectors_colname,e_product_colname,stage_colname,gross_net_colname,agg_by_colname,ex_colname,ex_net_colname,ex_gross_colname See `PFUAggDatabase::sea_cols`.
#' @param net_value,gross_value See `PFUAggDatabase::gross_net_metadata`.
#' @param all_value,product_value See `PFUAggDatabase::agg_metadata`.
#'
#' @return A data frame containing aggregate final and useful energy/exergy data by product
#' @export
#'
#' @examples
#' library(Recca)
#' tfc_product <- Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_fu_ex_product(fd_sectors = c("Residential"))
calculate_fu_ex_product <- function(.sutdata, fd_sectors,
                                    country_colname = IEATools::iea_cols$country,
                                    method_colname = IEATools::iea_cols$method,
                                    energy_type_colname = IEATools::iea_cols$energy_type,
                                    last_stage_colname = IEATools::iea_cols$last_stage,
                                    year_colname = IEATools::iea_cols$year,
                                    sector_colname = PFUAggDatabase::sea_cols$sector_colname,
                                    fd_sectors_colname = PFUAggDatabase::sea_cols$fd_sectors_colname,
                                    e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                                    stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                                    gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                                    agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                                    ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                                    ex_net_colname = Recca::aggregate_cols$net_aggregate_demand,
                                    ex_gross_colname = Recca::aggregate_cols$gross_aggregate_demand,
                                    net_value = PFUAggDatabase::gross_net_metadata$net_value,
                                    gross_value = PFUAggDatabase::gross_net_metadata$gross_value,
                                    all_value = PFUAggDatabase::agg_metadata$all_value,
                                    product_value = PFUAggDatabase::agg_metadata$product_value) {

  # Creates a list of final demand sectors
  fd_sector_list <- create_fd_sectors_list(fd_sectors = fd_sectors, .sutdata = .sutdata)

  # Adds a column which each observation containing the list of final demand sectors
  PSUT_DF_fu <- .sutdata %>%
    dplyr::mutate(
      "{fd_sectors_colname}" := fd_sector_list
    )

  # Calculates final demand by _product
  fu_product <- PSUT_DF_fu %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors_colname, by = product_value) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[last_stage_colname]], .data[[year_colname]], .data[[ex_net_colname]], .data[[ex_gross_colname]]) %>%
    magrittr::set_colnames(c(country_colname, method_colname, energy_type_colname, stage_colname, year_colname, ex_net_colname, ex_gross_colname)) %>%
    tidyr::pivot_longer(cols = ex_net_colname:ex_gross_colname,
                        names_to = gross_net_colname,
                        values_to = ex_colname) %>%
    dplyr::mutate(
      "{gross_net_colname}" := stringr::str_replace(.data[[gross_net_colname]], ex_net_colname, net_value)
    ) %>%
    dplyr::mutate(
      "{gross_net_colname}" := stringr::str_replace(.data[[gross_net_colname]], ex_gross_colname, gross_value)
    ) %>%
    dplyr::relocate(.data[[gross_net_colname]], .after = .data[[stage_colname]])

  # Expands matrices
  fu_product_expanded <- fu_product %>%
    matsindf::expand_to_tidy(matvals = ex_colname,
                             rownames = e_product_colname) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[stage_colname]], .data[[gross_net_colname]], .data[[year_colname]], .data[[e_product_colname]], .data[[ex_colname]])

  # Add additional metadata columns
  fu_product_expanded %>%
    dplyr::mutate(
      "{sector_colname}" := all_value,
      "{agg_by_colname}" := product_value,
      "{ex_colname}" := as.numeric(.data[[ex_colname]])
    ) %>%
    dplyr::relocate(.data[[year_colname]], .after = .data[[agg_by_colname]]) %>%
    dplyr::relocate(.data[[ex_colname]], .after = .data[[year_colname]])
}


#' Calculate total final consumption of final and useful energy by sector
#'
#' Calculate the total final consumption (TFC) at the final and useful stages
#' (along with any additional stages) by sector.
#' This function first uses the uses `create_fd_sectors_list()` function,
#' with a user-supplied set of final demand sectors `fd_sectors`
#' to identify the final demand sectors desired for analysis.
#' The `Recca::finaldemand_aggregates()` function is then applied to `.sutdata`
#' data frame by sector, to calculate the total final consumption of all products
#' in each of the sectors supplied in `fd_sectors`.
#'
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices with associated final demand sector names
#' @param fd_sectors A character vector of final demand sectors.
#'
#' @param country_colname,method_colname,energy_type_colname,last_stage_colname,year_colname See `IEATools::iea_cols`.
#' @param sector_colname,fd_sectors_colname,e_product_colname,stage_colname,gross_net_colname,agg_by_colname,ex_colname,ex_net_colname,ex_gross_colname See `PFUAggDatabase::sea_cols`.
#' @param net_value,gross_value See `PFUAggDatabase::gross_net_metadata`.
#' @param all_value,sector_value See `PFUAggDatabase::agg_metadata`.
#'
#' @return A data frame containing total final and useful consumption by sector
#' @export
#'
#' @examples
#' library(Recca)
#' tfc_sector <- Recca::UKEnergy2000mats %>%
#'   tidyr::pivot_wider(names_from = matrix.name,
#'                      values_from = matrix) %>%
#'   dplyr::mutate(Method = "PCM") %>%
#'   calculate_fu_ex_sector(fd_sectors = c("Residential"))
calculate_fu_ex_sector <- function(.sutdata, fd_sectors,
                                   country_colname = IEATools::iea_cols$country,
                                   method_colname = IEATools::iea_cols$method,
                                   energy_type_colname = IEATools::iea_cols$energy_type,
                                   last_stage_colname = IEATools::iea_cols$last_stage,
                                   year_colname = IEATools::iea_cols$year,
                                   sector_colname = PFUAggDatabase::sea_cols$sector_colname,
                                   fd_sectors_colname = PFUAggDatabase::sea_cols$fd_sectors_colname,
                                   e_product_colname = PFUAggDatabase::sea_cols$e_product_colname,
                                   stage_colname = PFUAggDatabase::sea_cols$stage_colname,
                                   gross_net_colname = PFUAggDatabase::sea_cols$gross_net_colname,
                                   agg_by_colname = PFUAggDatabase::sea_cols$agg_by_colname,
                                   ex_colname = PFUAggDatabase::sea_cols$ex_colname,
                                   ex_net_colname = Recca::aggregate_cols$net_aggregate_demand,
                                   ex_gross_colname = Recca::aggregate_cols$gross_aggregate_demand,
                                   net_value = PFUAggDatabase::gross_net_metadata$net_value,
                                   gross_value = PFUAggDatabase::gross_net_metadata$gross_value,
                                   all_value = PFUAggDatabase::agg_metadata$all_value,
                                   sector_value = PFUAggDatabase::agg_metadata$sector_value) {

  # Creates a list of final demand sectors
  fd_sector_list <- create_fd_sectors_list(fd_sectors = fd_sectors, .sutdata = .sutdata)

  # Adds a column which each observation containing the list of final demand sectors
  PSUT_DF_fu <- .sutdata %>%
    dplyr::mutate(
      "{fd_sectors_colname}" := fd_sector_list
    )

  # Calculates final demand by _product
  fu_sector <- PSUT_DF_fu %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors_colname, by = sector_value) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[last_stage_colname]], .data[[year_colname]], .data[[ex_net_colname]], .data[[ex_gross_colname]]) %>%
    magrittr::set_colnames(c(country_colname, method_colname, energy_type_colname, stage_colname, year_colname, ex_net_colname, ex_gross_colname)) %>%
    tidyr::pivot_longer(cols = ex_net_colname:ex_gross_colname,
                        names_to = gross_net_colname,
                        values_to = ex_colname) %>%
    dplyr::mutate(
      "{gross_net_colname}" := stringr::str_replace(.data[[gross_net_colname]], ex_net_colname, net_value)
    ) %>%
    dplyr::mutate(
      "{gross_net_colname}" := stringr::str_replace(.data[[gross_net_colname]], ex_gross_colname, gross_value)
    ) %>%
    dplyr::relocate(.data[[gross_net_colname]], .after = .data[[stage_colname]])

  # Expands matrices
  fu_sector_expanded <- fu_sector %>%
    matsindf::expand_to_tidy(matvals = ex_colname,
                             rownames = sector_colname) %>%
    dplyr::select(.data[[country_colname]], .data[[method_colname]], .data[[energy_type_colname]], .data[[stage_colname]], .data[[gross_net_colname]], .data[[year_colname]], .data[[sector_colname]], .data[[ex_colname]])

  # Asserts that the length of the character vector containing the sectors present
  # in the expanded data is less than or equal to the length of fd_sectors.
  assertthat::assert_that(length(unique(fu_sector_expanded$Sector)) <= length(fd_sectors),
                          msg = "There are more final demand sectors present than stipulated in fd_sectors")

  # Asserts that the final demand sectors present in fu_sector_expanded are present in fd_sectors
  assertthat::assert_that(isTRUE(unique(unique(fu_sector_expanded$Sector) %in% fd_sectors)),
                          msg = "There are final demand sectors present that were not stipulated in fd_sectors")

  # Add additional metadata columns
  fu_sector_expanded %>%
    dplyr::mutate(
      "{e_product_colname}" := all_value,
      "{agg_by_colname}" := sector_value,
      "{ex_colname}" := as.numeric(.data[[ex_colname]])
    ) %>%
    dplyr::relocate(.data[[sector_colname]], .after = .data[[e_product_colname]]) %>%
    dplyr::relocate(.data[[year_colname]], .after = .data[[agg_by_colname]]) %>%
    dplyr::relocate(.data[[ex_colname]], .after = .data[[year_colname]])
}


#' Create a list containing final demand sectors
#'
#' This function creates a list equal to the length of any data frame supplied.
#' It is typically used on a data frame containing Physical Supply-Use Tables (PSUT)
#' with the associated final demand sectors in the nested `Y` and `U_EIOU` matrices.
#'
#' @param fd_sectors A character vector of final demand sectors.
#' @param .sutdata A data frame containing Physical Supply-Use Table (PSUT)
#'                 matrices with associated final demand sector names
#'
#' @return A list the length of a desired data frame containing final demand vectors
#' @export
#'
#' @examples
#' library(Recca)
#' final_demand_sector_list <- create_fd_sectors_list(fd_sectors = c("Residential"),
#'                                                    .sutdata = Recca::UKEnergy2000mats)
create_fd_sectors_list <- function(fd_sectors, .sutdata) {

  fd_sectors_list <- rep(x = list(c(fd_sectors)), times = nrow(.sutdata))

  return(fd_sectors_list)

}







# Everything above this line can probably be deleted at a later date.
# Best to comment first, just to make sure we're no longer using the
# functions above.
# ---MKH, 27 July 2022











#' Calculate primary aggregates for PSUT data
#'
#' This function routes to `Recca::primary_aggregates`.
#'
#' @param .psut_data The data for which primary aggregates are to be calculated.
#' @param countries The countries for which primary aggregates are to be calculated.
#' @param years The years for which primary aggregates are to be calculated.
#' @param p_industries A string vector of industries that count as "primary".
#' @param pattern_type The type of matching to be used for primary industry names.
#'                     Default is "leading".
#'
#' @return A version of `.psut_data` with additional column for primary aggregate data.
#'
#' @export
calculate_primary_aggregates <- function(.psut_data,
                                         countries,
                                         years,
                                         p_industries,
                                         pattern_type = "leading") {

  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::primary_aggregates(p_industries = p_industries,
                              pattern_type = pattern_type)
}


#' Calculate final demand aggregates for PSUT data
#'
#' This function routes to `Recca::finaldemand_aggregates`.
#'
#' @param .psut_data The data for which final demand aggregates are to be calculated.
#' @param countries The countries for which final demand aggregates are to be calculated.
#' @param years The years for which final demand aggregates are to be calculated.
#' @param fd_industries A string vector of sectors that count as "final demand".
#' @param pattern_type The type of matching to be used for final demand sectors names.
#'                     Default is "leading".
#'
#' @return A version of `.psut_data` with additional column for final demand aggregate data.
#'
#' @export
calculate_finaldemand_aggregates <- function(.psut_data,
                                             countries,
                                             years,
                                             fd_sectors,
                                             pattern_type = "leading") {

  .psut_data %>%
    PFUDatabase::filter_countries_years(countries = countries, years = years) %>%
    Recca::finaldemand_aggregates(fd_sectors = fd_sectors,
                                  pattern_type = pattern_type)
}




