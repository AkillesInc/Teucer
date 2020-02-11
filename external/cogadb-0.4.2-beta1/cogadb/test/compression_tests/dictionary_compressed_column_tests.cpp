#include <core/global_definitions.hpp>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"
#include "compression/dictionary_compressed_column.hpp"

namespace CoGaDB {

    const std::string TESTDATA_PATH = std::string(PATH_TO_COGADB_EXECUTABLE) +
                                      "/test/testdata/compression_tests/";

    class DictionaryCompressedColumnTest : public testing::Test {

    public:
        typedef boost::shared_ptr<DictionaryCompressedColumn<std::string> > StringTypedDictionaryCompressedColumnPtr;

        DictionaryCompressedColumnTest() {
            region_column = StringTypedDictionaryCompressedColumnPtr(
                    new DictionaryCompressedColumn<std::string>("C_REGION", VARCHAR));
            region_column->load(TESTDATA_PATH + "dictionary_compressed_column");
        }

        virtual ~DictionaryCompressedColumnTest() {

        }

        virtual void SetUp() {
            // Code here will be called immediately after the constructor (right
            // before each test).
        }

        virtual void TearDown() {
            // Code here will be called immediately after each test (right
            // before the destructor).
        }

    protected:

        uint32_t *getCompressedData(StringTypedDictionaryCompressedColumnPtr column) {
            return column->ids_->data();
        }

        std::map<std::string, uint32_t> getDictionary(StringTypedDictionaryCompressedColumnPtr column) {
            return column->dictionary_;
        }

        std::vector<std::string> getReverseLookupVector(StringTypedDictionaryCompressedColumnPtr column) {
            return column->reverse_lookup_vector_;
        }

        StringTypedDictionaryCompressedColumnPtr region_column;

    };

    //@formatter:off
    TEST_F(DictionaryCompressedColumnTest, NUMBER_OF_ROWS_SSB_SF1) {
        ASSERT_EQ(30000, region_column->getNumberOfRows());
    };

    TEST_F(DictionaryCompressedColumnTest, DICTIONARY_SIZE) {
        std::map<std::string, uint32_t> dictionary = getDictionary(region_column);
        std::vector<std::string> reverse_lookup_vector = getReverseLookupVector(region_column);
        ASSERT_EQ(5, dictionary.size());
        ASSERT_EQ(5, reverse_lookup_vector.size());
    };

    //@formatter:on

} // end namespace



