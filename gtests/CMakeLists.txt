include(FetchContent)
FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

include(GoogleTest)

file(GLOB test_sources "test_*.cpp")
add_executable(cgx-reactor-tests ${test_sources})
target_link_libraries(cgx-reactor-tests
    cgx-reactor
    GTest::gtest_main
)
target_include_directories(cgx-reactor-tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

gtest_discover_tests(cgx-reactor-tests)
