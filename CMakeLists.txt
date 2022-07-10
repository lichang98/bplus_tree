cmake_minimum_required(VERSION 3.12)

project(bplus_tree VERSION 1.0)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
add_executable(main
  ./src/main.cc)
find_library(PTHREAD_LIBRARY
    pthread
)
find_library(GTEST_LIBRARY
    gtest
)
target_include_directories(main PUBLIC
  ./include
  ./third_part
)
target_link_libraries(main PUBLIC "${PTHREAD_LIBRARY}" "${GTEST_LIBRARY}")
