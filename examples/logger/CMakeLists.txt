add_executable(cgx-reactor-logger main.cpp)
target_link_libraries(cgx-reactor-logger PRIVATE cgx-reactor)
target_compile_features(cgx-reactor-logger PRIVATE cxx_std_20)
