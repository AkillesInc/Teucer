/* 
 * File:   is_equals.h
 * Author: florian
 *
 * Created on 27. Mai 2015, 14:58
 */

#ifndef IS_EQUALS_H
#define	IS_EQUALS_H

#include "gtest/internal/gtest-internal.h"

namespace CoGaDB {

template<typename T> bool is_equal(const T& lhs, const T& rhs) {
    return lhs == rhs;
}

template<> bool is_equal(const float& lhs, const float& rhs) {
    testing::internal::FloatingPoint<float> lhs_fp(lhs);
    testing::internal::FloatingPoint<float> rhs_fp(rhs);
    
    return lhs_fp.AlmostEquals(rhs_fp);
}

template<> bool is_equal(const double& lhs, const double& rhs) {
    testing::internal::FloatingPoint<double> lhs_fp(lhs);
    testing::internal::FloatingPoint<double> rhs_fp(rhs);
    
    return lhs_fp.AlmostEquals(rhs_fp);
}

}

#endif	/* IS_EQUALS_H */

