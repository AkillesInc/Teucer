project(CoGaDB)

#google testing framework
#add_definitions(-DGTEST_USE_OWN_TR1_TUPLE=0)
add_definitions(-DGTEST_HAS_TR1_TUPLE=0)
add_subdirectory(gtest-1.7.0)
enable_testing(true)

include_directories(src ${gtest_SOURCE_DIR}/include ${gtest_SOURCE_DIR})
#add_executable(unit-tests firstTestCase.cpp)
#target_link_libraries(unit-tests gtest gtest_main cogadb)
#add_test(unit-tests unit-tests)

set(GOOGLE_GTEST_INCLUDE_DIRS "${gtest_SOURCE_DIR}/include"
    PARENT_SCOPE)

add_executable(regression-tests "regression test/regression_tests.cpp")
target_link_libraries(regression-tests gtest gtest_main cogadb)
add_test(regression-tests regression-tests)

add_executable(generic_regression-tests "regression test/generic_regression_tests.cpp")
target_link_libraries(generic_regression-tests gtest gtest_main cogadb)
add_test(generic_regression-tests generic_regression-tests)

file(COPY testdata/regression\ test DESTINATION testdata)
file(COPY testdata/unittests DESTINATION testdata)

add_executable(wah_bitmap_column-tests "compression_tests/wah_bitmap_column_tests.cpp")
target_link_libraries(wah_bitmap_column-tests gtest gtest_main cogadb)
add_test(wah_bitmap_column-tests wah_bitmap_column-tests)

add_executable(dictionary_compressed_column-tests "compression_tests/dictionary_compressed_column_tests.cpp")
target_link_libraries(dictionary_compressed_column-tests gtest gtest_main cogadb)
add_test(dictionary_compressed_column-tests dictionary_compressed_column-tests)

file(COPY testdata/compression_tests DESTINATION testdata)
add_executable(cpp-code-generation-tests "unittests/cpp_code_generation.cpp")
target_link_libraries(cpp-code-generation-tests gtest gtest_main cogadb ${CMAKE_DL_LIBS})
add_test(cpp-code-generation-tests cpp-code-generation-tests)


add_executable(string_column_tests "unittests/storage/string_column_tests.cpp")
target_link_libraries(string_column_tests gtest gtest_main cogadb ${CMAKE_DL_LIBS})
add_test(string_column_tests string_column_tests)

add_executable(hash-table-performance-tests "experiments/hash_table_performance.cpp")
target_link_libraries(hash-table-performance-tests gtest gtest_main cogadb ${CMAKE_DL_LIBS})
add_test(hash-table-performance-tests hash-table-performance-tests)
