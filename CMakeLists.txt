cmake_minimum_required(VERSION 4.0)
project(disk_multicast)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")

add_executable(disk_multicast main.cpp)

find_package(lz4 CONFIG REQUIRED)
target_link_libraries(disk_multicast PRIVATE lz4::lz4)
