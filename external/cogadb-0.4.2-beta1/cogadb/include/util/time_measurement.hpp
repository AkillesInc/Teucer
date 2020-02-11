#pragma once


#include <stdint.h>
#include <vector>

#include <iostream>
#include <string>

// needs -lrt (real-time lib)
// 1970-01-01 epoch UTC time, 1 mcs resolution (divide by 1M to get time_t)

namespace CoGaDB{

typedef uint64_t Timestamp;

Timestamp getTimestamp();

void fatalError(const std::string& message);

int Factorial(int n); 

}; //end namespace CogaDB


