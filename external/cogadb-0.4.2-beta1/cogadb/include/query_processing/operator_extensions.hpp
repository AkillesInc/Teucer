#pragma once

#include <core/bitmap.hpp>
#include <core/base_column.hpp>

namespace CoGaDB {
    namespace query_processing {
        
        class BitmapOperator{
        public:
            bool hasResultBitmap();
            BitmapPtr getResultBitmap();
            void releaseResultData();
        protected:
            BitmapPtr cpu_bitmap_;
        };
        
        
        class PositionListOperator{
        public:
            bool hasResultPositionList();
            PositionListPtr getResultPositionList();
            void releaseResultData();
        protected:
//            PositionListPtr cpu_tids_;
            PositionListPtr tids_;
        };
        
        
    
    }; //end namespace CoGaDB
}; //end namespace CoGaDB


