
#include <util/time_measurement.hpp>
#include <boost/chrono.hpp>
#include <cstdlib>

using namespace boost::chrono;

namespace CoGaDB{

Timestamp getTimestamp()
{
	high_resolution_clock::time_point tp = high_resolution_clock::now();
	nanoseconds dur = tp.time_since_epoch();

	return (Timestamp)dur.count();
}

void fatalError(const std::string& message){
	std::cout << "FATAL ERROR: " << message << std::endl;
	std::cout << "Aborting..." << std::endl;	
	exit(-1);
}

int Factorial(int n) {

  if(n <= 1) {
    return 1;
  } 
  
  return n * Factorial(n-1); 

}

}; //end namespace CogaDB


