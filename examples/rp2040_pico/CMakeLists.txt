cmake_minimum_required(VERSION 3.13)

# Board: pico (RP2040)
set(PICO_BOARD pico CACHE STRING "" FORCE)

include(pico_sdk_import.cmake)

project(rp2040_pico)

pico_sdk_init()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# The cgx-reactor library is a header-only INTERFACE lib under ../../include.
# Point at its headers directly — do NOT add_subdirectory() the reactor repo
# (it would force a host build of the test examples on bare-metal).
add_executable(rp2040_pico main.cpp)

target_include_directories(rp2040_pico PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
)

target_link_libraries(rp2040_pico
    pico_stdlib
    hardware_adc
)

# GCC 14.2.1 promotes -Wchanges-meaning (a header-only warning about
# task_handle::error() shadowing the cgx::reactor::error enum type) to
# an error. Suppress it for the cross-build only — the host build is
# unaffected because the reactor library itself is unchanged.
target_compile_options(rp2040_pico PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wno-changes-meaning>
)

pico_enable_stdio_usb(rp2040_pico 1)
pico_enable_stdio_uart(rp2040_pico 0)

pico_add_extra_outputs(rp2040_pico)
