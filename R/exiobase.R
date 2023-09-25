#' Calculate Exiobase coefficients
#'
#' The CL-PFU Database supplies coefficients
#' to Exiobase.
#' This function calculates those coefficients
#' and returns in a data frame.
#'
#' @param phi_vecs A data frame of phi (exergy-to-energy ratio) coefficients.
#'
#' @return A data frame of Exiobase coefficients.
#'
#' @export
exiobase_coeffs <- function(phi_vecs) {
  # EA's code goes here.
  # For now just return this tibble to test the code.
  tibble::tribble(~a, ~b, ~c,
                  1,  2,   3,
                  4,  5,   6)
}
