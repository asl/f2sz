cmake_minimum_required(VERSION 3.16)
project(t2sz VERSION 1.1.2 LANGUAGES C)

add_definitions(-DVERSION="${PROJECT_VERSION}")

set(CMAKE_C_STANDARD 99)

find_package(zstd REQUIRED)

add_executable(t2sz src/t2sz.c)
target_link_libraries(t2sz zstd::libzstd_shared m)

if (CMAKE_BUILD_TYPE STREQUAL Release)
  add_custom_command(TARGET t2sz POST_BUILD COMMAND ${CMAKE_STRIP} t2sz)
endif ()

install(TARGETS t2sz)
