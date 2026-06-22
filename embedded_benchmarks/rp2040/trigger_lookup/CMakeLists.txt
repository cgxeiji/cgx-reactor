cmake_minimum_required(VERSION 3.13)

# Board: pico (RP2040)
set(PICO_BOARD pico CACHE STRING "" FORCE)

include(pico_sdk_import.cmake)

project(trigger_lookup)

pico_sdk_init()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# The cgx-reactor library is a header-only INTERFACE lib under
# ../../../include. Point at its headers directly.
add_executable(trigger_lookup main.cpp)

target_include_directories(trigger_lookup PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../../include
)

target_link_libraries(trigger_lookup
    pico_stdlib
)

# GCC 14.2.1 promotes -Wchanges-meaning (a header-only warning about
# task_handle::error() shadowing the cgx::reactor::error enum type) to
# an error. Suppress it for the cross-build only.
target_compile_options(trigger_lookup PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wno-changes-meaning>
)

pico_enable_stdio_usb(trigger_lookup 1)
pico_enable_stdio_uart(trigger_lookup 0)

pico_add_extra_outputs(trigger_lookup)
