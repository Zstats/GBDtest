#' My Function
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
                             foldername){
  # 采用compiler 方式加载
  # fun_env <- new.env()
  # compiler::loadcmp("inst/bytecode/utils.rds", envir = fun_env)
  # utilTool <-fun_env$UtilsTool$new()

  #utilTool <-UtilsTool$new()

  # return(utilTool$med_cal(beta1,se1,beta2,se2,beta_total,n))
  args <- list(ZIP_folder , file_fold, Parallel ,
               foldername)

  # 使用加密文件路径
  enc_path <- system.file("bytecode", "read2.enc", package = "GBDt18")
  result <-.Call("call_bytecode_func", as.character("process_GBD_data"), args, enc_path)
  return(result)
}

