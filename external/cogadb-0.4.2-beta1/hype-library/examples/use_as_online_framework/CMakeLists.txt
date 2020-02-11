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

add_executable(online_learning EXCLUDE_FROM_ALL
	       online_learning.cpp)
target_link_libraries(online_learning hype)
add_test(NAME learning COMMAND online_learning)
set(UNITTESTS ${UNITTESTS} online_learning)

add_executable(online_learning_manual_time_measurement EXCLUDE_FROM_ALL
	       online_learning_manual_time_measurement.cpp)
target_link_libraries(online_learning_manual_time_measurement hype)
add_test(NAME learning_manual_time_measurement COMMAND online_learning_manual_time_measurement)
set(UNITTESTS ${UNITTESTS} online_learning_manual_time_measurement)

add_executable(simulator EXCLUDE_FROM_ALL
	       simulator.cpp)
target_link_libraries(simulator hype)
add_test(NAME simulator COMMAND simulator)

add_executable(sequence_optimization EXCLUDE_FROM_ALL
	       sequence_optimization.cpp)
target_link_libraries(sequence_optimization hype)
add_test(NAME sequence_optimization COMMAND sequence_optimization)
set(UNITTESTS ${UNITTESTS} sequence_optimization)

add_executable(qep_optimization EXCLUDE_FROM_ALL
	       qep_optimization.cpp)
target_link_libraries(qep_optimization hype)
add_test(NAME qep_optimization COMMAND qep_optimization)
set(UNITTESTS ${UNITTESTS} qep_optimization)


add_custom_target(hype-online-framework DEPENDS ${UNITTESTS})
