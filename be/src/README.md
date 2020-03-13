## doris udf

### doris_ga
GA项目所用的UDF, 包括路径分析，留存分析，漏斗分析以及首次出现字段

### doris_mistat
小米统计与GA合并所用的UDF, 包括字符串hash函数, 字符串分割函数, 首次激活时间，末次激活时间，页面用户访问session以及页面用户访问时长

### How to use
1. 将文件拷贝到对应的doris目录下be/src
2. 在be/CMakeLists.txt中添加add_subdirectory(${SRC_DIR}/udf_ga)以及add_subdirectory(${SRC_DIR}/udf_mistat)
