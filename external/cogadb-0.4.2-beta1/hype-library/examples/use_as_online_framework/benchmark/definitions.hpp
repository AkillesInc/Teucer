
#define __STDC_LIMIT_MACROS //nessessary to define and then include the header file, so UINTPTR_MAX is defined (used to get the architecture 32 or 64 bit)
#include <stdint.h>

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>



//g++ compiler workaround for boost thread!
#ifdef __GNUC__
	#pragma GCC visibility push(default) 
#endif
	#include <boost/smart_ptr.hpp> 
	#include <boost/thread.hpp> 
	#include <boost/program_options.hpp> 
//g++ compiler workaround for boost thread!
#ifdef __GNUC__
#pragma GCC visibility pop 
#endif

using namespace std;



//enum Architecture{Architecture_32Bit,Architecture_64Bit};

//const Architecture getArchitecture(){
//
//#if UINTPTR_MAX == 0xffffffff
///* 32-bit */
//return Architecture_32Bit;
//#elif UINTPTR_MAX == 0xffffffffffffffff
///* 64-bit */
//return Architecture_64Bit;
//#else
///* wtf */
//   #error "Could not determine Architecture!"
//#endif
//}



