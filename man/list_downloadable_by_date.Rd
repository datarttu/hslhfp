% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/azure_storage.R
\name{list_downloadable_by_date}
\alias{list_downloadable_by_date}
\title{List csv available & not locally existing csv dumps from Azure storage}
\usage{
list_downloadable_by_date(
  d,
  storage_url = Sys.getenv("AZURE_STORAGE_URL"),
  blob_prefix = "csv/VehiclePosition",
  result_prefix = "vp",
  target_dir = Sys.getenv("RAW_HFP_DIR")
)
}
\arguments{
\item{d}{A Date or a string of format \code{YYYY-MM-DD}}

\item{storage_url}{A storage URL string (defaults to env var \code{AZURE_STORAGE_URL})}

\item{blob_prefix}{A prefix defining a subset of blobs (defaults to \code{csv/VehiclePosition})}

\item{result_prefix}{A prefix to be added to the result file names (defaults to \code{vp})}

\item{target_dir}{Path to the target dir of downloaded files (defaults to env var \code{RAW_HFP_DIR})}
}
\value{
A tibble containing remote and local dataset info.
}
\description{
Returns a tibble of datasets by date \code{d}, indicated whether the datasets
\enumerate{
\item exist in Azure Storage and
\item do not already exist in the \code{target_dir}.
}
}
