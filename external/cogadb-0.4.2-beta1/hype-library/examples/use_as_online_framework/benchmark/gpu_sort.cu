
#include <thrust/host_vector.h>
#include <thrust/device_vector.h>
#include <thrust/generate.h>
#include <thrust/sort.h>
#include <thrust/copy.h>

#include "definitions.hpp"

void GPU_Sort(VecPtr dataset){
	assert(dataset!=NULL);
	// transfer data to the device
  thrust::device_vector<ElementType> d_vec (dataset->begin(),dataset->end());

  // sort data on the device (846M keys per second on GeForce GTX 480)
  thrust::sort(d_vec.begin(), d_vec.end());

  // transfer data back to host
  thrust::copy(d_vec.begin(), d_vec.end(), dataset->begin());
}

