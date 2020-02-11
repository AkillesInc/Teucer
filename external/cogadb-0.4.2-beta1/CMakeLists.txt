###
### Master build system for the independent projects HyPE
### and CoGaDB
###
project(GPUDBMS)
cmake_minimum_required(VERSION 2.6)

# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
    message(STATUS "CCACHE found")
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
else(CCACHE_FOUND)
    message(STATUS "CCACHE not found")
endif(CCACHE_FOUND)

add_subdirectory(hype-library)

# Subsequent FIND_PACKAGE(HyPE) commands will find
# the internal HyPEConfig.cmake file instead of
# any installed one.
set(HyPE_DIR "${PROJECT_SOURCE_DIR}/CMake"
    CACHE PATH "The directory containing a CMake configuration file for HyPE.")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
    "${PROJECT_SOURCE_DIR}/CMake")
find_package(Thrust REQUIRED)
#add_definitions(${MonetDBOcelot_DEFINITIONS})
include_directories(${THRUST_INCLUDE_DIR})
get_property(dirs DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
MESSAGE(${dirs})

add_subdirectory(external_libraries/IntelPerformanceCounterMonitorV2.7)
#include_directories(${PCM_INCLUDE_DIRS})
include_directories(${INTEL_PCM_INCLUDE_DIRS})
set(EXTRA_SOURCES ${EXTRA_SOURCES} ${INTEL_PCM_SOURCES})

#add_subdirectory(thrust)
#set(Thrust_DIR "${PROJECT_SOURCE_DIR}/CMake"
#   CACHE PATH "The directory containing a CMake configuration file for HyPE.")

add_subdirectory(cogadb)

# Unified doc target
add_custom_target(doc DEPENDS hype-doc cogadb-doc)

# Testing: create top-level test configuration
enable_testing()
