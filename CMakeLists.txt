cmake_minimum_required(VERSION 3.20)
project(f2sz VERSION 1.0.0 LANGUAGES C)

add_definitions(-DVERSION="${PROJECT_VERSION}")

set(CMAKE_C_STANDARD 99)

find_package(zstd REQUIRED)

if (NOT CMAKE_C_BYTE_ORDER MATCHES "^(BIG_ENDIAN|LITTLE_ENDIAN)$")
  message(FATAL_ERROR "CMAKE_C_BYTE_ORDER has unexpected value '${CMAKE_C_BYTE_ORDER}'")
endif()
if (CMAKE_C_BYTE_ORDER STREQUAL BIG_ENDIAN)
  add_compile_definitions(F2SZ_BIG_ENDIAN=1)
endif()

add_executable(f2sz src/f2sz.c src/utils.c)
target_link_libraries(f2sz zstd::libzstd_shared m)

if (CMAKE_BUILD_TYPE STREQUAL Release)
  add_custom_command(TARGET f2sz POST_BUILD COMMAND ${CMAKE_STRIP} f2sz)
endif ()

install(TARGETS f2sz)
