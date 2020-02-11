//
// Created by Sebastian Dorok on 06.07.15.
//

#include <boost/chrono.hpp>
#include <util/time_measurement.hpp>
#include <iomanip>

#include <cmath>

#ifndef GPUDBMS_GENOMICS_DEFINITIONS_HPP
#define GPUDBMS_GENOMICS_DEFINITIONS_HPP

namespace CoGaDB {

    using namespace std;
    using namespace boost::chrono;

    const string GENOME_SCHEMA_TYPE_PARAMETER = "genome_schema_type";
    const string GENOME_IMPORTER_VERBOSE_PARAMETER = "genome_importer_verbose";
    const string BASE_CENTRIC_SCHEMA_TYPE_PARAMETER_VALUE = "base_centric";
    const string STORAGE_EXPERIMENTS_SCHEMA_TYPE_PARAMETER_VALUE = "storage_experiments";
    const string SEQUENCE_CENTRIC_SCHEMA_TYPE_PARAMETER_VALUE = "sequence_centric";
    const string SEQUENCE_CENTRIC_SCHEMA_WITH_STASH_TYPE_PARAMETER_VALUE = "sequence_centric_with_stashing";

    const string GENOME_SCHEMA_COMPRESSION_PARAMETER = "genome_schema_compression";

    const string GENOTYPE_FREQUENCY_MIN_PARAMETER = "genotype_frequency_min";
    const string GENOTYPE_FREQUENCY_MAX_PARAMETER = "genotype_frequency_max";

    // Helper methods

    /*  */
    inline void _printProgress(int cur, float prog, int max, ostream &out, bool final) {
        out << fixed << setprecision(2)
        << "\r   [" << string(cur, '#')
        << string(max - cur, ' ') << "] " << 100 * prog << "%";

        if (prog == 1 && final) {
            out << endl;
        } else {
            out.flush();
        }
    }

    /* Prints a progress bar
     *
     * Params:
     * lengthOverall    .. maximum possible value
     * length           .. current value
     * maximumIndicatos .. maximum number of indicator signs '#'
     */
    inline void _drawProgress(uint64_t maximumValue, uint64_t currentValue,
                              int maximumIndicator, ostream &out, bool final = false) {
        if (currentValue == -1) {
            _printProgress(maximumIndicator + 1, 1, maximumIndicator, out, final);
            return;
        }

        float progress((float) currentValue / (float) maximumValue); // percentage of infile already read

        // Number of #'s as function of current progress
        int cur(std::ceil(progress * maximumIndicator));
        //if (_last != cur) _last = cur,
        _printProgress(cur, progress, maximumIndicator, out, final);

    }

    /**
     * Returns 0 if suffix is the suffix of string.
     */
    inline int _endsWithFoo(const char *string, const char *suffix) {
        string = strrchr(string, '.');

        if (string != NULL && suffix != NULL)
            return (strcmp(string, suffix));

        return (-1);
    }

}


#endif //GPUDBMS_GENOMICS_DEFINITIONS_HPP
