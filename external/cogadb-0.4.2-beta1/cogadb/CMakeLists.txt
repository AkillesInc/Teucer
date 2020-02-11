project(CoGaDB)
cmake_minimum_required(VERSION 2.6)
# do not treat constants as variable names
cmake_policy(SET CMP0012 NEW)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH}
    "${PROJECT_SOURCE_DIR}/CMake"
    "${PROJECT_SOURCE_DIR}/CMake/FindTBB")
include(CheckSymbolExists)

# General options
option(BUILD_SHARED_LIBS "Build shared libraries" ON)
option(USE_GOOGLE_PERF_TOOLS "Use Google performance tools if available" OFF)

# Configure required Boost libraries
set(BOOST_ROOT "" CACHE PATH "Boost build root (useful on Windows)")
option(Boost_USE_STATIC_LIBS
       "Search for static boost libs" OFF)
option(Boost_USE_MULTITHREADED
       "Search for multithreaded boost libs" ON)
option(Boost_USE_STATIC_RUNTIME
       "Search for boost libs linked against static C++ runtime" OFF)
find_package(Boost 1.47.0 REQUIRED
	     filesystem system thread program_options serialization
	     chrono random date_time iostreams regex)
# ensure that dependant libraries not explicitly specified here
# are found by the linker:
link_directories(${Boost_LIBRARY_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
set(LIBS ${LIBS} ${Boost_LIBRARIES})

# Configure TR1 headers: prefer Boost TR1 library
# NOTE: Currently not required for CoGaDB
#find_path(TR1_INCLUDE_DIR memory
#	  HINTS "${Boost_INCLUDE_DIR}/boost/tr1/tr1"
#	  DOC "TR1 header location")
#if (NOT TR1_INCLUDE_DIR)
#	message(FATAL_ERROR "TR1 headers not found!")
#endif ()
#include_directories(${TR1_INCLUDE_DIR})

# Configure FindTBB (Find-module is a svn:external)
find_package(TBB REQUIRED)
include_directories(${TBB_INCLUDE_DIRS})
set(LIBS ${LIBS} ${TBB_LIBRARIES})

# Configure CUDA
option(ENABLE_GPU_ACCELERATION "GPU acceleration enabled" OFF)

if(ENABLE_GPU_ACCELERATION)
set(CUDA_GENERATED_OUTPUT_DIR "${PROJECT_BINARY_DIR}/CMakeFiles")
set(CUDA_NVCC_FLAGS "-arch=sm_30"
    CACHE STRING "CUDA nvcc flags (semicolon separated)")
find_package(CUDA 6.5 REQUIRED)
endif ()

# Configure GPU Ocelot
find_package(Ocelot 2.1)
if (${OCELOT_FOUND})
	option(ENABLE_GPUOCELOT "Link with GPU Ocelot instead of CUDA runtime" OFF)
	if (${ENABLE_GPUOCELOT})
		set(CUDA_LIBRARIES ${Ocelot_LIBRARIES})
	endif ()
endif ()

# Check for cudaMemGetInfo() availability
set(CMAKE_REQUIRED_INCLUDES ${CUDA_INCLUDE_DIRS})
set(CMAKE_REQUIRED_LIBRARIES ${CUDA_LIBRARIES})
# avoid caching
unset(HAVE_CUDAMEMGETINFO CACHE)
check_symbol_exists(cudaMemGetInfo "cuda.h;cuda_runtime_api.h"
		    HAVE_CUDAMEMGETINFO)

# Configure Flex and Bison
find_package(FLEX REQUIRED)
find_package(BISON 2.5 REQUIRED)
execute_process(COMMAND ${BISON_EXECUTABLE} --print-datadir
		OUTPUT_VARIABLE BISON_DATA_DIR
		OUTPUT_STRIP_TRAILING_WHITESPACE)

#message(INFO "Bison Version: ${BISON_VERSION}")
#option(BISON_VERSION_3_OR_HIGHER "Bison Version three or higher found" ON)
if(BISON_VERSION VERSION_EQUAL 3.0.2 OR BISON_VERSION  VERSION_GREATER 3.0.2)
    set(BISON_VERSION_3_OR_HIGHER true)
else(BISON_VERSION VERSION_LESS 3.0.2)
    set(BISON_VERSION_3_OR_HIGHER false)
    #option(BISON_VERSION_3_OR_HIGHER "Bison Version three or higher found" OFF)
endif()

# Configure BAM - for genomics support
find_package(BAM)
if (BAM_FOUND)
        set(LIBS ${LIBS} ${BAM_LIBRARIES})
        message(INFO "Enabling genomics importer for FASTA, SAM and BAM files.")
else (!BAM_FOUND)
        message(WARNING "Disabling genomics importer for FASTA, SAM and BAM files. Reason: " ${BAM_ERROR_REASON})
endif()

find_package(OpenMP REQUIRED)
if (OPENMP_FOUND)
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
endif()

if(USE_GOOGLE_PERF_TOOLS)
    find_package(GooglePerfTools)
    if (GOOGLE_PERFTOOLS_FOUND)
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I${GOOGLE_PERFTOOLS_INCLUDE_DIR}")
        set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -I${GOOGLE_PERFTOOLS_INCLUDE_DIR}")
        set(LIBS ${LIBS} ${TCMALLOC_LIBRARIES})
        set(LIBS ${LIBS} ${STACKTRACE_LIBRARIES})
        set(LIBS ${LIBS} ${PROFILER_LIBRARIES})
    endif()
else()
    message(INFO " Google performance tools will not be used.")
endif()

#, where to find heap-profiler.h, etc.
#  GOOGLE_PERFTOOLS_FOUND, If false, do not try to use Google perftools.
# also defined for general use are
#  TCMALLOC_LIBRARIES, where to find the tcmalloc library.
#  STACKTRACE_LIBRARIES, where to find the stacktrace library.
#  PROFILER_LIBRARIES


# Configure GNU Readline (optional)
find_package(LibReadline)
if (LIBREADLINE_FOUND)
	include_directories(${LIBREADLINE_INCLUDE_DIRS})
	set(LIBS ${LIBS} ${LIBREADLINE_LIBRARIES})
else ()
	message(WARNING "GNU Readline not found! Falling back to simple command line reader.")
endif ()

# Configure HyPE
find_package(HyPE REQUIRED)
add_definitions(${HyPE_DEFINITIONS})
include_directories(${HyPE_INCLUDE_DIRS})
set(LIBS ${LIBS} ${HyPE_LIBRARIES})

# CoGaDB-specific options
#option(ENABLE_GPU_ACCELERATION "GPU acceleration enabled" OFF)
option(ENABLE_TWO_PHASE_PHYSICAL_OPTIMIZATION "Two phase physical optimization enabled" ON)
option(ENABLE_SIMD_ACCELERATION "SIMD acceleration enabled" OFF)
option(ENABLE_BRANCHING_SCAN "enables branching inside scan operator variants" ON)
option(ENABLE_CPU_NESTED_LOOP_JOIN "CPU nested loop join enabled" OFF)

set(CMAKE_CXX_FLAGS_RELEASE "-O6 -fomit-frame-pointer -falign-functions=4 -falign-loops=4 -falign-jumps=4 -ftree-vectorize -fexpensive-optimizations -frerun-cse-after-loop  -funroll-loops -frerun-loop-opt -finline-functions" CACHE STRING "C++ Optimization flags used when build in release mode")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_C_FLAGS "-std=gnu99")

set(CUDA_NVCC_FLAGS "-rdc=true -gencode arch=compute_35,code=sm_35 -gencode arch=compute_30,code=sm_30 -gencode arch=compute_20,code=sm_20" CACHE STRING "Configures for which GPU Compute Capabilities CoGaDB is compiled.")



set(COGADB_VERSION "\"0.4.2\"")
set(PATH_TO_COGADB_EXECUTABLE "\"${PROJECT_BINARY_DIR}\"")
set(PATH_TO_COGADB_SOURCE_CODE "\"${PROJECT_SOURCE_DIR}\"")
#set(PATH_TO_DATA_OF_TESTS "\"${PROJECT_BINARY_DIR}/test/testdata\"")
set(PATH_TO_DATA_OF_TESTS "\"${PROJECT_BINARY_DIR}/test/testdata\"")

# Generate Autoconf-like config.h
configure_file("${PROJECT_SOURCE_DIR}/config.h.in"
	       "${PROJECT_BINARY_DIR}/config.h")
add_definitions(-DHAVE_CONFIG_H)

# CoGaDB includes
include_directories("${PROJECT_BINARY_DIR}"
		    "${PROJECT_SOURCE_DIR}/include"
		    "${PROJECT_SOURCE_DIR}/include/hardware_optimizations/main_memory_joins/serial_hash_join")
install(DIRECTORY "${PROJECT_SOURCE_DIR}/include/"
	DESTINATION include/cogadb
	FILES_MATCHING PATTERN "*.h" PATTERN "*.hpp"
	PATTERN ".svn" EXCLUDE)

# SQL Parser generation
file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/src/sql/server")
include_directories("${PROJECT_BINARY_DIR}/src/sql/server")

set(BISON_SQLParser_FLAGS "--defines=${PROJECT_BINARY_DIR}/src/sql/server/sql_parser.hpp"
			  "--xml=${PROJECT_BINARY_DIR}/doc/sql_grammar.xml")
bison_target(SQLParser src/sql/server/sql_parser.ypp
	     "${PROJECT_BINARY_DIR}/src/sql/server/sql_parser.cpp"
	     COMPILE_FLAGS "${BISON_SQLParser_FLAGS}")
add_custom_target(SQLParser DEPENDS ${BISON_SQLParser_OUTPUTS})

flex_target(SQLScanner src/sql/server/sql_scanner.lpp
	    "${PROJECT_BINARY_DIR}/src/sql/server/sql_scanner.cpp")

add_flex_bison_dependency(SQLScanner SQLParser)

#Should go into next "if" clause but doesnt compile
include_directories("${PROJECT_SOURCE_DIR}/../external_libraries/cub-1.4.1")
include_directories("${PROJECT_SOURCE_DIR}/../external_libraries/moderngpu/include")

if(ENABLE_GPU_ACCELERATION)
    cuda_add_library(cuda_gpu_backend
                src/core/gpu_admission_control.cpp
                src/backends/gpu/memory_cost_models.cpp
                src/backends/gpu/stream_manager.cpp
                src/backends/gpu/gpu_backend.cpp
                src/backends/gpu/gpu_aggregation.cu
                src/backends/gpu/gpu_bitmap_set_operation.cu
                src/backends/gpu/gpu_bit_operations.cu
                src/backends/gpu/gpu_column_algebra.cu
                src/backends/gpu/gpu_conversion_operations.cu
                src/backends/gpu/gpu_fetch_join.cu
                src/backends/gpu/gpu_join.cu
                src/backends/gpu/positionlist_set_operation.cu
                src/backends/gpu/gpu_selection.cu
                src/backends/gpu/gpu_util.cu
                src/unittests/gpu_work_unittests.cu
                src/backends/gpu/hashtable/hash_functions.cu
                src/backends/gpu/hashtable/hash_table.cpp
                src/backends/gpu/hashtable/hash_table.cu
                src/backends/gpu/hashtable/mt19937ar.cpp
		src/query_compilation/gpu_handbuilt/tpch6.cu
                src/query_compilation/gpu_handbuilt/tpch5.cu
                src/query_compilation/gpu_handbuilt/tpch4.cu
                src/query_compilation/gpu_handbuilt/tpch3.cu
                src/query_compilation/gpu_handbuilt/ssb4.cu
		${PROJECT_SOURCE_DIR}/../external_libraries/moderngpu/src/mgpucontext.cu
		${PROJECT_SOURCE_DIR}/../external_libraries/moderngpu/src/mgpuutil.cpp)

    set(LIBS ${LIBS} cuda_gpu_backend)
    # static, shared and import libraries are put in lib/,
    # while DLLs are put in bin/
    set_target_properties(cuda_gpu_backend PROPERTIES
                          ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
                          LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
else ()
	message(WARNING "GPU acceleration is disabled, I will not build the CUDA backend!")
endif ()

SET (COGADB_SOURCE_FILES 
		 src/parser/commandline_interpreter.cpp
		 src/parser/client.cpp
		 src/query_compilation/aggregate_specification.cpp
		 src/query_compilation/code_generator.cpp
		 src/query_compilation/code_generators/cpp_code_generator.cpp
                 src/query_compilation/code_generators/cuda_c_code_generator.cpp
		 src/query_compilation/kernel_execution.cpp
		 src/query_compilation/minimal_api.cpp
		 src/query_compilation/pipeline.cpp
		 src/query_compilation/predicate_expression.cpp
		 src/query_compilation/predicate_specification.cpp
		 src/query_compilation/query_context.cpp
		 src/query_processing/column_processing/bitmap_operator.cpp
		 src/query_processing/column_processing/cpu_columnscan_operator.cpp
                 src/query_processing/column_processing/cpu_column_constant_filter_operator.cpp
                 src/query_processing/column_processing/column_bitmap_selection_operator.cpp
                 src/query_processing/column_processing/column_fetch_join_operator.cpp
                 src/query_processing/column_processing/column_bitmap_fetch_join_operator.cpp
                 src/query_processing/column_processing/column_comparator.cpp
                 src/query_processing/column_processing/column_convert_positionlist_to_bitmap.cpp
                 src/query_processing/column_processing/column_convert_bitmap_to_positionlist.cpp
                 src/query_processing/column_processing/positionlist_operator.cpp
		 src/query_processing/column_processing/cpu_algebra_operator.cpp
		 src/query_processing/query_processor.cpp
		 src/query_processing/sort_operator.cpp
		 src/query_processing/column_computation_constant_operator.cpp
                 src/query_processing/column_computation_algebra_operator.cpp
		 src/query_processing/selection_operator.cpp
		 src/query_processing/fetch_join_operator.cpp
		 src/query_processing/operator_extensions.cpp
                 src/query_processing/chain_join_operator.cpp
		 src/query_processing/invisible_join_operator.cpp
		 src/query_processing/indexed_tuple_reconstruction_operator.cpp
		 src/query_processing/join_operator.cpp
		 src/query_processing/pk_fk_join_operator.cpp
		 src/query_processing/cross_join_operator.cpp
		 src/query_processing/rename_operator.cpp
		 src/query_processing/groupby_operator.cpp
		 src/query_processing/projection_operator.cpp
		 src/query_processing/scan_operator.cpp
                 src/query_processing/complex_selection_operator.cpp
                 src/query_processing/generate_constant_column_operator.cpp
                 src/query_processing/udf_operator.cpp
                 src/lookup_table/join_index.cpp
		 src/lookup_table/lookup_table.cpp
		 src/lookup_table/lookup_column.cpp
		 src/backends/processor_backend.cpp
                 src/backends/cpu/cpu_backend.cpp
                 src/backends/cpu/cpu_join.cpp
#                 src/backends/xeonphi/xeonphi_backend.cpp
		 src/core/attribute_reference.cpp
		 src/core/bitmap.cpp
		 src/core/column.cpp
                 src/core/cstring_column.cpp
		 src/core/copy_function_factory.cpp
		 src/core/processor_data_cache.cpp
		 src/core/lookup_array.cpp
		 src/core/data_dictionary.cpp
                 src/core/selection_expression.cpp
		 src/core/base_column.cpp
		 src/core/runtime_configuration.cpp
		 src/core/base_table.cpp
		 src/core/memory_allocator.cpp
		 src/core/table.cpp
                 src/core/operator_parameter_types.cpp
                 src/core/positionlist.cpp
                 src/core/foreign_key_constraint.cpp
                 src/core/user_defined_function.cpp
                 src/core/variable_manager.cpp
                 src/compression/dictionary_compressed_column.cpp
                 src/compression/rle_compressed_column.cpp
                 src/compression/rle_compressed_column_with_prefix_counts.cpp
                 src/compression/rle_delta_one_compressed_column_int.cpp
                 src/compression/rle_delta_one_compressed_column_int_with_prefix_counts.cpp
                 src/compression/void_compressed_column_int.cpp
                 src/hardware_optimizations/simd_acceleration.cpp
                 src/hardware_optimizations/primitives.cpp
                 src/core/column_based_typed.cpp
                 src/optimizer/optimizer.cpp
                 src/optimizer/join_order_optimization.cpp 
		 src/unittests/unittests.cpp
		 src/unittests/compression_techniques_unittests.cpp
		 src/unittests/join_unittests.cpp
		 src/unittests/cdk_primitives_unittests.cpp
		 src/unittests/positionlist_unittests.cpp
		 src/unittests/histogram_unittests.cpp
		 src/util/backtrace.cpp
		 src/util/column_grouping_keys.cpp
		 src/util/code_generation.cpp
		 src/util/filesystem.cpp
		 src/util/getname.cpp
		 src/util/exit.cpp
		 src/util/star_schema_benchmark.cpp
		 src/util/denormalized_star_schema_benchmark.cpp
		 src/util/genomics_extension/alignment_files.cpp
		 src/util/genomics_extension/genome_data_importer.cpp
		 src/util/genomics_extension/base_centric_simple_key_importer.cpp
		 src/util/genomics_extension/storage_experiments_importer.cpp
		 src/util/genomics_extension/sequence_centric_simple_key_importer.cpp
		 src/util/genomics_extension/sequence_centric_simple_key_importer_stashing_unmapped_reads.cpp
		 src/compression/wah_bitmap_column.cpp
		 src/util/genomics_extension/exporter.cpp
		 src/util/genomics_extension/CigarFSM.cpp
                 src/util/genomics_extension/samQueryCreator.cpp
                 src/util/genomics_extension/samExportCommandParser.cpp
                 src/util/genomics_extension/samVerification.cpp
		 src/util/tpch_benchmark.cpp
		 src/util/hardware_detector.cpp
		 src/util/profiling.cpp
		 src/util/result_output_format.cpp
		 src/util/shared_library.cpp
		 src/util/statistics.cpp
		 src/util/tests.cpp
		 src/util/time_measurement.cpp
		 src/util/types.cpp
		 src/persistence/storage_manager.cpp
		 src/statistics/histogram.cpp
		 src/statistics/column_statistics.cpp
		 src/statistics/selectivity_estimator.cpp
		 src/statistics/statistics_manager.cpp
                 src/hardware_optimizations/main_memory_joins/hash_joins.cpp
                 src/hardware_optimizations/main_memory_joins/serial_hash_join/hashtable/hashtable.c
                 src/hardware_optimizations/main_memory_joins/serial_hash_join/join/partition.c
                 src/hardware_optimizations/main_memory_joins/serial_hash_join/join/hashjoin.c
                 src/hardware_optimizations/main_memory_joins/serial_hash_join/datagen/datagen.c
                 src/hardware_optimizations/main_memory_joins/serial_hash_join/time/time.c
		 ${BISON_SQLParser_OUTPUTS}
		 ${FLEX_SQLScanner_OUTPUTS}
		 src/sql/server/sql_parsetree.cpp
		 src/sql/server/sql_driver.cpp
		 test/unittests/code_generation_ssbm.cpp
		 ${PROJECT_SOURCE_DIR}/../external_libraries/IntelPerformanceCounterMonitorV2.7/client_bw.cpp
		 ${PROJECT_SOURCE_DIR}/../external_libraries/IntelPerformanceCounterMonitorV2.7/cpucounters.cpp
		 ${PROJECT_SOURCE_DIR}/../external_libraries/IntelPerformanceCounterMonitorV2.7/msr.cpp
		 ${PROJECT_SOURCE_DIR}/../external_libraries/IntelPerformanceCounterMonitorV2.7/pci.cpp
		 ${PROJECT_SOURCE_DIR}/../external_libraries/IntelPerformanceCounterMonitorV2.7/utils.cpp)

if(ENABLE_GPU_ACCELERATION)
    cuda_add_library(cogadb ${COGADB_SOURCE_FILES})
else()
    add_library(cogadb ${COGADB_SOURCE_FILES})
endif()

target_link_libraries(cogadb ${LIBS})

# static, shared and import libraries are put in lib/,
# while DLLs are put in bin/
set_target_properties(cogadb PROPERTIES
		      ARCHIVE_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
		      LIBRARY_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/lib"
		      RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

# NOTE: src/main.cpp is currently a test but will later be the
# entry point for a standalone daemon
add_executable(cogadbd src/main.cpp)
target_link_libraries(cogadbd cogadb)
# put executable in bin/
set_target_properties(cogadbd PROPERTIES
		      RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

# CoGaDB installation
install(TARGETS cogadb cogadbd
	ARCHIVE DESTINATION lib
	LIBRARY DESTINATION lib
	RUNTIME DESTINATION bin)

SET(CPACK_GENERATOR "DEB")
SET(CPACK_DEBIAN_PACKAGE_MAINTAINER "CoGaDB Development Team <cogadb@googlegroups.com>")
SET(CPACK_PACKAGE_VERSION 0.4.2)
SET(CPACK_PACKAGE_NAME "cogadb")
SET(CPACK_PACKAGE_VERSION_MAJOR 0)
SET(CPACK_PACKAGE_VERSION_MINOR 4)
SET(CPACK_PACKAGE_VERSION_PATCH 2)
SET(CPACK_SYSTEM_NAME ubuntu-14.04-lts-amd64)

#first comes the short description, then a "\n<white space>", then the long description
SET(CPACK_DEBIAN_PACKAGE_DESCRIPTION "A Column-oriented GPU-accelerated DBMS\n CoGaDB is a column-oriented GPU-accelerated database management system developed at TU Dortmund University and University of Magdeburg. Its purpose is to investigate advanced coprocessing techniques for effective GPU utilization during database query processing. It utilizes our hybrid query processing engine (HyPE) for the physical optimization process.\n Website: http://cogadb.cs.tu-dortmund.de/wordpress/")

SET(CPACK_DEBIAN_PACKAGE_VERSION ${CPACK_PACKAGE_VERSION})
#SET(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6, libgcc1,"\ 
#"libboost-chrono1.54-dev, libboost-chrono1.54.0,"\
#"libboost-date-time1.54-dev,libboost-date-time1.54.0,"\
#"libboost-filesystem1.54-dev, libboost-filesystem1.54.0,"\
#"libboost-iostreams1.54-dev, libboost-iostreams1.54.0,"\
#"libboost-program-options1.54-dev, libboost-program-options1.54.0,"\
#"libboost-random1.54-dev, libboost-random1.54.0,"\
#"libboost-regex1.54-dev, libboost-regex1.54.0,"\
#"libboost-serialization1.54-dev, libboost-serialization1.54.0,"\
#"libboost-system1.54-dev, libboost-system1.54.0,"\
#"libboost-thread1.54-dev, libboost-thread1.54.0,"\
#"libtbb2, zlib1g, libreadline6, libgomp1")
SET(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6, libgcc1, libboost-chrono1.54-dev, libboost-chrono1.54.0, libboost-date-time1.54-dev,libboost-date-time1.54.0, libboost-filesystem1.54-dev, libboost-filesystem1.54.0, libboost-iostreams1.54-dev, libboost-iostreams1.54.0, libboost-program-options1.54-dev, libboost-program-options1.54.0, libboost-random1.54-dev, libboost-random1.54.0, libboost-regex1.54-dev, libboost-regex1.54.0, libboost-serialization1.54-dev, libboost-serialization1.54.0, libboost-system1.54-dev, libboost-system1.54.0, libboost-thread1.54-dev, libboost-thread1.54.0, libtbb2, zlib1g, libreadline6, libgomp1")

INCLUDE(CPack)

###
### Test programs and benchmarks
### FIXME: should probably be tests defined in
### benchmarks/CMakeLists.txt and run by CTest
###

add_executable(sql_test_ssb_diagrams src/sql/server/test_ssb_diagrams.cpp)
target_link_libraries(sql_test_ssb_diagrams cogadb)
# put executable in bin/
set_target_properties(sql_test_ssb_diagrams PROPERTIES
		      RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

add_executable(sql_benchmark_parser src/sql/server/benchmark_parser.cpp)
target_link_libraries(sql_benchmark_parser cogadb)
# put executable in bin/
set_target_properties(sql_benchmark_parser PROPERTIES
		      RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

if(ENABLE_GPU_ACCELERATION)
    add_executable(aggregation_benchmark benchmarks/aggregation_benchmark.cpp) 
    target_link_libraries(aggregation_benchmark cogadb)
    # put executable in bin/
    set_target_properties(aggregation_benchmark PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

    add_executable(selection_benchmark benchmarks/selection_benchmark.cpp) 
    target_link_libraries(selection_benchmark cogadb)
    # put executable in bin/
    set_target_properties(selection_benchmark PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

    add_executable(sort_benchmark benchmarks/sort_benchmark.cpp) 
    target_link_libraries(sort_benchmark cogadb)
    # put executable in bin/
    set_target_properties(sort_benchmark PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

    add_executable(join_benchmark benchmarks/join_benchmark.cpp) 
    target_link_libraries(join_benchmark cogadb)
    # put executable in bin/
    set_target_properties(join_benchmark PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

    add_executable(generic_selection_benchmark benchmarks/generic_selection_benchmark.cpp) 
    target_link_libraries(generic_selection_benchmark cogadb)
    # put executable in bin/
    set_target_properties(generic_selection_benchmark PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")

    cuda_add_executable(gpu_memory_allocator benchmarks/gpu_memory_allocator.cu) 
    #set_target_properties(gpu_memory_allocator PROPERTIES LINKER_LANGUAGE CXX)
    # put executable in bin/
    set_target_properties(gpu_memory_allocator PROPERTIES
                          RUNTIME_OUTPUT_DIRECTORY "${PROJECT_BINARY_DIR}/bin")
else ()
	message(WARNING "GPU acceleration is disabled, I will not build the hybrid CPU/GPU operator benchmarks!")
endif()

# Output directory required by libhype
file(MAKE_DIRECTORY "${PROJECT_BINARY_DIR}/output")

# Documentation
add_subdirectory(doc)

#unittests
add_subdirectory(test)
include_directories(${GOOGLE_GTEST_INCLUDE_DIRS})
add_definitions(-DGTEST_HAS_TR1_TUPLE=0)

enable_testing()
add_executable(unittest_compression EXCLUDE_FROM_ALL
	       src/unittests/compression_techniques_unittests.cpp)
target_link_libraries(unittest_compression cogadb)
add_test(NAME compression_test COMMAND unittest_compression)
set(UNITTESTS ${UNITTESTS} unittest_compression)
add_custom_target(cogadb_tests COMMAND ${CMAKE_CTEST_COMMAND}
                  DEPENDS ${UNITTESTS})
