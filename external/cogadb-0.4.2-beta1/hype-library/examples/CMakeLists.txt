###
### Unit tests using CTest
### Run with "check" target to build and run tests
###

# NOTE: unit tests are currently broken
add_subdirectory(unitests)
add_subdirectory(use_as_online_framework)

# use correct CTest configuration for Make and non-Make
# based build systems
if (CMAKE_CONFIGURATION_TYPES)
	set(CTEST_BUILD_CONFIGURATION ${CMAKE_CFG_INTDIR})
else ()
	set(CTEST_BUILD_CONFIGURATION ${CMAKE_BUILD_TYPE})
endif ()

# Unified "check" target to compile and run all
# unit tests/examples
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} -C ${CTEST_BUILD_CONFIGURATION}
		  DEPENDS hype-unit-tests hype-online-framework)
