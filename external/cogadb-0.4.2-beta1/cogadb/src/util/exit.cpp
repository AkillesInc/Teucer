
#include <core/global_definitions.hpp>
#include <core/processor_data_cache.hpp>

namespace CoGaDB {

    void callGlobalCleanupRoutines() {
        OnExitCloseDataPlacementThread();
    }

    void exit(int status) {
        callGlobalCleanupRoutines();
        if (status != EXIT_SUCCESS) {
            quick_exit(status);
        }
        std::exit(status);
    }
};
