# compile.R
library(openssl)

# 解析R文件为表达式列表并编译为字节码
exprs <- parse("R/read2.R")
compiled_list <- lapply(exprs, compiler::compile)

# 设置加密密钥和初始向量（在实际使用中应该安全存储，不要硬编码）
key <- charToRaw("1234567890abcdef1234567890abcdef")  # 32字节
iv <- openssl::rand_bytes(16)  # 16字节初始化向量

# 序列化为原始二进制数据
serialized_data <- serialize(compiled_list, NULL)

# AES-CBC加密
encrypted_data <- openssl::aes_cbc_encrypt(serialized_data, key = key, iv = iv)

# 创建包含IV和加密数据的完整二进制文件
# 将IV和加密数据拼接起来，以便解密时可以提取
full_data <- c(iv, encrypted_data)
writeBin(full_data, "inst/bytecode/read2.enc")

# 保存测试版未加密文件（开发阶段用，最终可移除）
# saveRDS(compiled_list, "inst/bytecode/utilsList.rds")

cat("字节码已编译并加密\n")
