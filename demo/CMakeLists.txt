cmake_minimum_required(VERSION 3.1.0)

# set the project name
project(demo VERSION 0.9)

# add the executable
add_executable(demo demo.cpp)
target_include_directories(demo PUBLIC $(CMAKE_CURRENT_SOURCE_DIR)/..)
# specify the C++ standard
#set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-std=c++11 -lpthread -Wall -O3 -ftemplate-depth=4096")

