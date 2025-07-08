#' @keywords internal
read_GBD <- function(ZIP_folder = FALSE, file_fold = FALSE, Parallel = FALSE, foldername) {
  if (!ZIP_folder & !file_fold) stop("请至少选择 ZIP_folder 或 file_fold 之一")

  if (ZIP_folder) {
    zipdir <- tempfile()
    dir.create(zipdir)
    zipfiles <- list.files(foldername, full.names = TRUE, pattern = "\\.zip$")
    if (length(zipfiles) == 0) stop("未找到 zip 文件")
    lapply(zipfiles, function(zip) {
      utils::unzip(zip, exdir = zipdir)
    })
    files <- list.files(zipdir, pattern = "\\.csv$", full.names = TRUE)
  } else {
    files <- list.files(foldername, full.names = TRUE, pattern = "\\.csv$")
  }

  if (length(files) == 0) stop("未找到 CSV 文件")

  library(data.table)
  if (Parallel) {
    library(foreach)
    library(doParallel)
    cl <- parallel::makeCluster(parallel::detectCores() - 1)
    doParallel::registerDoParallel(cl)
    data_list <- foreach(j = seq_along(files), .packages = "data.table") %dopar% {
      fread(files[j])
    }
    stopCluster(cl)
  } else {
    data_list <- lapply(files, fread)
  }

  data <- data.table::rbindlist(data_list, use.names = TRUE, fill = TRUE)
  return(data)
}

#' @keywords internal
read_codebooks <- function(folder) {
  files <- list.files(folder, pattern = "\\.csv$", full.names = TRUE)
  codebook_names <- gsub("\\.csv$", "", basename(files))
  codebook_list <- lapply(files, data.table::fread)
  names(codebook_list) <- codebook_names
  return(codebook_list)
}

#' @keywords internal
id_normalization <- function(data, codebook) {
  data$measure <- codebook[["measure_codebook"]]$measure_name[match(data$measure, codebook[["measure_codebook"]]$measure_id)]
  data$location <- codebook[["location_codebook"]]$location_name[match(data$location, codebook[["location_codebook"]]$location_id)]
  data$sex <- codebook[["sex_codebook"]]$sex_name[match(data$sex, codebook[["sex_codebook"]]$sex_id)]
  data$age <- codebook[["age_codebook"]]$age_name[match(data$age, codebook[["age_codebook"]]$age_id)]
  data$metric <- codebook[["metric_codebook"]]$metric_name[match(data$metric, codebook[["metric_codebook"]]$metric_id)]
  if ("cause" %in% names(data)) data$cause <- codebook[["cause_codebook"]]$cause_name[match(data$cause, codebook[["cause_codebook"]]$cause_id)]
  if ("rei" %in% names(data)) data$rei <- codebook[["rei_codebook"]]$rei_name[match(data$rei, codebook[["rei_codebook"]]$rei_id)]
  return(data)
}

#' @keywords internal
location_normalization <- function(data, trans_location) {
  data$location <- sub("Türkiye", "Turkey", data$location)
  data$location <- ifelse(data$location %in% trans_location$location,
                          trans_location$normalized_location[match(data$location, trans_location$location)],
                          data$location)
  return(data)
}

#' GBD数据读取与处理主函数
#'
#' 一键读取、标准化、合并GBD数据
#'
#' @param ZIP_folder 是否读取zip压缩包（TRUE/FALSE）
#' @param file_fold 是否直接读取csv文件夹（TRUE/FALSE）
#' @param Parallel 是否启用并行读取csv
#' @param foldername zip或csv所在的文件夹路径
#'
#' @return 返回一个标准化后的GBD数据框
#' @export
process_GBD_data <- function(ZIP_folder = FALSE, file_fold = TRUE, Parallel = FALSE,
                             foldername) {
  raw_data <- read_GBD(ZIP_folder, file_fold, Parallel, foldername)

  if (length(grep("id", names(raw_data))) > 1) {
    if (any(grepl("start", names(raw_data)))) {
      raw_data <- as.data.frame(raw_data)[, unique(c(
        grep("id|start|end", names(raw_data)),
        (ncol(raw_data) - 3):ncol(raw_data)
      ))]
    } else {
      raw_data <- as.data.frame(raw_data)[, c(
        grep("id", names(raw_data)),
        (ncol(raw_data) - 3):ncol(raw_data)
      )]
    }
    names(raw_data) <- gsub("_id", "", names(raw_data))
  }

  data("codebook", package = "GBDt18", envir = environment())
  data("trans_location", package = "GBDt18", envir = environment())
  # codebook <- GBDtest:::codebook
  # trans_location <- GBDtest:::trans_location

  if (is.numeric(raw_data$measure)) {
    data_std <- id_normalization(raw_data, codebook)
  } else {
    data_std <- raw_data
  }

  data_final <- location_normalization(data_std, trans_location)
  return(data_final)
}
