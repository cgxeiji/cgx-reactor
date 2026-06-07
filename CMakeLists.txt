cmake_minimum_required(VERSION 3.20)
project(cgx-reactor LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

enable_testing()

add_library(cgx-reactor INTERFACE)
target_include_directories(cgx-reactor INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)
target_compile_features(cgx-reactor INTERFACE cxx_std_20)

add_subdirectory(gtests)
add_subdirectory(examples)
