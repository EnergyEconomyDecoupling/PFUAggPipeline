# This script subsets the
# PFU aggregate and efficiency data
# to Austria.
#
# This script fulfills a request for data from
# Caroline Zimm at IIASA.
#
# ---Matthew Kuperus Heun, 12 Sept 2023


pinboard_folder <- PFUSetup::get_abs_paths()[["pipeline_releases_folder"]]
pinboard <- pins::board_folder(pinboard_folder, versioned = TRUE)
agg_eta_pfu_df <- pins::pin_read(board = pinboard,
                                 name = "agg_eta_pfu",
                                 version = "20230619T051304Z-f653c")
agg_eta_pfu_df_aut <- agg_eta_pfu_df |>
  dplyr::filter(Country == "AUT",
                Product.aggregation == "Specified",
                Industry.aggregation == "Specified")

agg_eta_pfu_df_aut |>
  write.csv(file = "~/Desktop/AUT-PFU.csv")
