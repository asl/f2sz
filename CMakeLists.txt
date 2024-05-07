cmake_minimum_required(VERSION 3.20)
project(f2sz VERSION 0.5.0 LANGUAGES C CXX)

add_definitions(-DVERSION="${PROJECT_VERSION}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

find_package(zstd REQUIRED)

if (NOT CMAKE_CXX_BYTE_ORDER MATCHES "^(BIG_ENDIAN|LITTLE_ENDIAN)$")
  message(FATAL_ERROR "CMAKE_C_BYTE_ORDER has unexpected value '${CMAKE_C_BYTE_ORDER}'")
endif()
if (CMAKE_CXX_BYTE_ORDER STREQUAL BIG_ENDIAN)
  add_compile_definitions(F2SZ_BIG_ENDIAN=1)
endif()

add_library(f2sz-common STATIC src/utils.cpp src/seekable.cpp src/index.cpp)
target_link_libraries(f2sz-common zstd::libzstd_shared m)

add_executable(f2sz src/f2sz.cpp)
target_link_libraries(f2sz f2sz-common zstd::libzstd_shared m)

add_executable(f2sz-get-fasta src/f2sz-get-fasta.cpp)
target_link_libraries(f2sz-get-fasta f2sz-common zstd::libzstd_shared m)


if (CMAKE_BUILD_TYPE STREQUAL Release)
  add_custom_command(TARGET f2sz POST_BUILD COMMAND ${CMAKE_STRIP} f2sz)
  add_custom_command(TARGET f2sz-get-fasta POST_BUILD COMMAND ${CMAKE_STRIP} f2sz-get-fasta)
endif()

install(TARGETS f2sz f2sz-get-fasta)
