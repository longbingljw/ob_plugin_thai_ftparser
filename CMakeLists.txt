CMAKE_MINIMUM_REQUIRED(VERSION 3.22)

# Thai tokenizer plugin
SET(PLUGIN_NAME thai_ftparser)

# TODO The list below are your implementention files
SET(SOURCES
    thai_ftparser.cpp)

# You also should set the information below
PROJECT(${PLUGIN_NAME}
        DESCRIPTION "OceanBase Thai language tokenizer plugin"
        HOMEPAGE_URL "https://open.oceanbase.com/"
        LANGUAGES CXX C ASM)

# 查找Python3.12开发包进行测试
find_package(Python3 3.12 EXACT COMPONENTS Development REQUIRED)

# Don't touch me
FIND_PACKAGE(ObPlugin REQUIRED)

# Macro OB_ADD_PLUGIN is defined in ObPluginConfig.cmake which provided by oceanabse-plugin-devel
OB_ADD_PLUGIN(${PLUGIN_NAME}
  ${SOURCES}
)

# 链接Python库和pthread
TARGET_LINK_LIBRARIES(${PLUGIN_NAME} PRIVATE Python3::Python pthread)

# 设置包含目录
TARGET_INCLUDE_DIRECTORIES(${PLUGIN_NAME} PRIVATE ${Python3_INCLUDE_DIRS})

# 设置C++标准为C++11
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 设置C标准为C99
set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

# 设置编译标志
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")

# 设置链接标志
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--no-as-needed")

# TARGET_INCLUDE_DIRECTORIES (${PLUGIN_NAME} PRIVATE include1 include2)
# TARGET_LINK_LIBRARIES (${PLUGIN_NAME} PRIVATE library1 library2)
# TARGET_XX (${PLUGIN_NAME} PRIVATE xxx)
