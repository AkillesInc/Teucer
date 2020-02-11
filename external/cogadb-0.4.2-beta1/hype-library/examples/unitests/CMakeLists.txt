###
### Unit tests using CTest
### Run with "check" target to build and run tests
###

# Output directory required by libhype
file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/output")

get_target_property(HyPE_TYPE hype TYPE)
if (${HyPE_TYPE} STREQUAL SHARED_LIBRARY)
	# will only apply to files in this directory and below
	add_definitions(-DHYPE_USE_SHARED)
endif ()

# add_executable(unittest_learning EXCLUDE_FROM_ALL
# 	       unittest_learning.cpp)
# target_link_libraries(unittest_learning hype)
# add_test(NAME learning COMMAND unittest_learning)
# set(UNITTESTS ${UNITTESTS} unittest_learning)

# add_executable(unittest_cpp_interface EXCLUDE_FROM_ALL
# 	       unittest_cpp_interface.cpp)
# target_link_libraries(unittest_cpp_interface hype)
# add_test(NAME cpp_interface COMMAND unittest_cpp_interface)
# set(UNITTESTS ${UNITTESTS} unittest_cpp_interface)

add_executable(unittest_c_interface EXCLUDE_FROM_ALL
	       unittest_c_interface.c)
target_link_libraries(unittest_c_interface hype)
add_test(NAME c_interface COMMAND unittest_c_interface)
set(UNITTESTS ${UNITTESTS} unittest_c_interface)

add_executable(unittest_c_qep EXCLUDE_FROM_ALL
	       unittest_c_qep.c)
target_link_libraries(unittest_c_qep hype)
add_test(NAME unittest_c_qep COMMAND unittest_c_qep)
set(UNITTESTS ${UNITTESTS} unittest_c_qep)

add_custom_target(hype-unit-tests DEPENDS ${UNITTESTS})
