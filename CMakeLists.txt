cmake_minimum_required(VERSION 3.19)
project(librados_demo_c C)

set(CMAKE_C_STANDARD 11)

include_directories(/usr/include)
link_libraries(/usr/lib64)

add_executable(librados_demo_c main.c)

target_link_libraries(librados_demo_c rados)