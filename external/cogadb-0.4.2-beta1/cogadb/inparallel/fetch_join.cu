#include <moderngpu.cuh>
#include <iostream>
#include <vector>
#include <algorithm>
#include <thrust/host_vector.h>
#include <thrust/device_vector.h>
#include <thrust/binary_search.h>
#include <thrust/execution_policy.h>
#include <stdlib.h>     /* srand, rand */
#include <time.h>       /* time */

#define gpuErrchk(ans) { gpuAssert((ans), __FILE__, __LINE__); }
inline void gpuAssert(cudaError_t code, char *file, int line, bool abort=true)
{
   if (code != cudaSuccess)
   {
      fprintf(stderr,"GPUassert: %s %s %dn", cudaGetErrorString(code), file, line);
      if (abort) exit(code);
   }
}

using namespace mgpu;
using namespace std;




typedef struct fetch_join_data {
    std::vector<unsigned int> matching_tids;
    std::vector<unsigned int> primary_keys;
    std::vector<unsigned int> foreign_keys;
} fetch_join_data;


void GenerateFetchJoinData(fetch_join_data &data, int join_index_size) {

    //ordered, unique
    std::vector<unsigned int> matching_tids;
    unsigned int num_matching_tids = join_index_size*0.5f;
    //ordered, non-unique
    std::vector<unsigned int> primary_keys;
    float key_repeat_probability = 0.2f;
    //non-ordered unique
    std::vector<unsigned int> foreign_keys;

    int pk = 0;
    for(int i=0; i < join_index_size; i++) {
	foreign_keys.push_back(i);
	primary_keys.push_back(pk);
	float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
	if(r > key_repeat_probability)
	    pk++;
    }
    std::random_shuffle(foreign_keys.begin(), foreign_keys.end());
    
    float prop_use_tid_from_pk_range = (float)num_matching_tids / (float)pk;
    if(prop_use_tid_from_pk_range > 1.0f) {
	std::cout << "Error: More matching tids than different primary keys. Aborting" << endl;
	return;
    }
    
    for(int i=0; i < pk; i++) {
	float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
	if(r < prop_use_tid_from_pk_range)
	    matching_tids.push_back(i);
    }
    num_matching_tids = matching_tids.size();
    cout << "[generated join index (" << join_index_size << " entries) and query tids (" << num_matching_tids << " entries)]" << endl;
  
    data.foreign_keys = foreign_keys;
    data.primary_keys = primary_keys;
    data.matching_tids = matching_tids;
}




__global__ void scatter_fk_bitmap(unsigned int join_index_size, char* flags_in, unsigned int* fk_column_tids, char* flags_out){

    //threads align to join index entries
    int tid = threadIdx.x + blockIdx.x * blockDim.x;
    
    while(tid<join_index_size){

        //get foreign key for this thread
	int fk = fk_column_tids[tid];
	
	//scatter to flag array at foreign key index
	if(flags_in[tid])
	    flags_out[fk] = 1;
	
	tid+=blockDim.x * gridDim.x;
    }
}


std::vector<char> DoFetchJoinModernGPU(CudaContext& context, fetch_join_data& data, bool print, bool measure) {
    
    //---- Get sizes ----
  
    unsigned int num_tids = data.matching_tids.size();
    unsigned int join_index_size = data.primary_keys.size();

    //--- Transfer data to device ----
    
    thrust::device_vector<unsigned int> matching_tids(data.matching_tids.begin(), data.matching_tids.end());
    thrust::device_vector<unsigned int> primary_keys(data.primary_keys.begin(), data.primary_keys.end());
    thrust::device_vector<unsigned int> foreign_keys(data.foreign_keys.begin(), data.foreign_keys.end());

    unsigned int *devIn1 = thrust::raw_pointer_cast(&primary_keys[0]);
    unsigned int* devIn2 = thrust::raw_pointer_cast(&matching_tids[0]);
    unsigned int *devIn3 = thrust::raw_pointer_cast(&foreign_keys[0]);
    
    MGPU_MEM(char) out = context.Malloc<char>(primary_keys.size());

    unsigned int number_of_blocks = 512;
    unsigned int number_of_threads = 1024;
    
    thrust::device_ptr<char> flag_array;
    unsigned int number_of_flags = ((join_index_size+7)/8)*8;
    try{
        flag_array = thrust::device_malloc<char>(number_of_flags);
    }catch(std::bad_alloc &e){
        std::cerr << "Ran out of memory during flag array allocation!" << std::endl;
        return std::vector<char>();
    }
    gpuErrchk(cudaMemsetAsync(flag_array.get(), 0, number_of_flags));
    cudaDeviceSynchronize();

    //--- Perform fetch join
    
    context.Start();
    
    SortedSearch<MgpuBoundsLower, MgpuSearchTypeMatch, MgpuSearchTypeNone>
	(devIn1, primary_keys.size(), devIn2, matching_tids.size(), out->get(), (int*)0, context);
	
    scatter_fk_bitmap<<<number_of_blocks, number_of_threads>>>(join_index_size, out->get(), devIn3, flag_array.get());
    cudaDeviceSynchronize();	
	
    double time = context.Split();
    if(measure)
	cout << "ModernGPU SortedSearch + scatter-kernel took " << time << endl << endl;
    
    //---- Transfer results ----

    thrust::host_vector<char> thrust_result(flag_array, flag_array+number_of_flags);
    std::vector<char> result(thrust_result.begin(), thrust_result.end());
    
    //---- Print results
    
    if(print) {
	cout << "--- Results from ModernGPU implementation ---" << endl;
	cout << "join-index (" << join_index_size << ")" << endl;
	for (int i = 0; i < join_index_size; i++)
	    cout << "\t" << data.primary_keys[i];
	cout << endl;
	for (int i = 0; i < join_index_size; i++)
	    cout << "\t" << data.foreign_keys[i];
	cout << endl << "matching-tids (" << num_tids << ")" << endl << "\t";
	for (int i = 0; i < num_tids; i++)
	    cout << ' ' << data.matching_tids[i];
	cout << endl << "flag-array of matching foreign keys (" << number_of_flags << ")" << endl << "\t";
	for (int i = 0; i < number_of_flags; i++)
	    cout << ' ' << (int)flag_array[i];
	cout << endl << endl;
    }
    
    return result;
}


std::vector<char> DoFetchJoinThrust(CudaContext& context, fetch_join_data& data, bool print, bool measure) {

    //---- Get sizes ----
    unsigned int num_tids = data.matching_tids.size();
    unsigned int join_index_size = data.primary_keys.size();

    //--- Transfer data to device ----

    thrust::device_vector<unsigned int> matching_tids(data.matching_tids.begin(), data.matching_tids.end());
    thrust::device_vector<unsigned int> primary_keys(data.primary_keys.begin(), data.primary_keys.end());
    thrust::device_vector<unsigned int> foreign_keys(data.foreign_keys.begin(), data.foreign_keys.end());
    thrust::device_vector<bool> search_output(join_index_size);
    thrust::device_vector<char> bitmap_output(join_index_size);
    thrust::device_vector<char> bits(join_index_size, 1);
    
    //--- Perform fetch join
    
    context.Start();
    
    thrust::binary_search(thrust::device, matching_tids.begin(), matching_tids.end(), primary_keys.begin(), primary_keys.end(), search_output.begin());
    
    thrust::scatter_if(bits.begin(), bits.end(), foreign_keys.begin(), search_output.begin(), bitmap_output.begin());
    
    double time = context.Split();
    if(measure)
	cout << "Thrust implementation took " << time << endl << endl;
    
    //---- Transfer results ----

    thrust::host_vector<char> thrust_result(bitmap_output.begin(), bitmap_output.end());
    std::vector<char> result(thrust_result.begin(), thrust_result.end());
    
    return result;
}




//assumes that matching tids is unique!
__device__ int binary_search(unsigned int* matching_tids, unsigned int number_of_matching_tids, unsigned int search_val){


   int low = 0;
   int high = number_of_matching_tids - 1;
   int mid = low + ((high - low) / 2);

   while (low <= high ){ //&& !(matching_tids[mid - 1] <= search_val && matching_tids[mid] > search_val)) 
       if(mid<0 || mid >= number_of_matching_tids) return number_of_matching_tids;
       //unsigned int mid = low + ((high - low) / 2);

       if (matching_tids[mid] > search_val){
           high = mid - 1;
       }else if (matching_tids[mid] < search_val){
           low = mid + 1;
       }else{
           return (unsigned int) mid; // found
       }
       mid = low + ((high - low) / 2);
   }
 
   return number_of_matching_tids; // not found
}

__global__ void fetch_join_bitmap_kernel(unsigned int* matching_tids, unsigned int number_of_matching_tids, unsigned int* pk_column_tids, unsigned int* fk_column_tids, unsigned int join_index_size, char* flags){

    //threads align to join index entries
    int tid = threadIdx.x + blockIdx.x * blockDim.x;
    
    while(tid<join_index_size){

	//try to find primary key for this thread in matching tids
        int index = binary_search(matching_tids, number_of_matching_tids, pk_column_tids[tid]);
	
        //get foreign key for this thread
	int fk = fk_column_tids[tid];
	
	//scatter to flag array at foreign key index
	if(index < number_of_matching_tids)
	    flags[fk] = 1;
	  
	//flags[fk] = index < number_of_matching_tids;
	
	tid+=blockDim.x * gridDim.x;
    }
}

std::vector<char> DoFetchJoinGPU(CudaContext& context, fetch_join_data& data, bool print, bool measure) {

    //---- Get sizes ----
  
    unsigned int num_tids = data.matching_tids.size();
    unsigned int join_index_size = data.primary_keys.size();

    //--- Transfer data to device ----

    thrust::device_vector<unsigned int> device_matching_tids(data.matching_tids.begin(), data.matching_tids.end());
    thrust::device_vector<unsigned int> device_primary_keys(data.primary_keys.begin(), data.primary_keys.end());
    thrust::device_vector<unsigned int> device_foreign_keys(data.foreign_keys.begin(), data.foreign_keys.end());
    
    //---- Perform fetch join ----
    
    unsigned int number_of_blocks = 512;
    unsigned int number_of_threads_per_block = 1024;
    
    thrust::device_ptr<char> flag_array;
    unsigned int number_of_flags = ((join_index_size+7)/8)*8;
    try{
        flag_array = thrust::device_malloc<char>(number_of_flags);
    }catch(std::bad_alloc &e){
        std::cerr << "Ran out of memory during flag array allocation!" << std::endl;
        return std::vector<char>();
    }
    gpuErrchk(cudaMemsetAsync(flag_array.get(), 0, number_of_flags));
    cudaDeviceSynchronize();
    
    unsigned int* devPtr1 = thrust::raw_pointer_cast(&device_matching_tids[0]);
    unsigned int *devPtr2 = thrust::raw_pointer_cast(&device_primary_keys[0]);
    unsigned int *devPtr3 = thrust::raw_pointer_cast(&device_foreign_keys[0]);
    
    context.Start();
    
    fetch_join_bitmap_kernel<<<number_of_blocks,number_of_threads_per_block>>>(
	devPtr1, num_tids, devPtr2, devPtr3, join_index_size, flag_array.get());
    cudaDeviceSynchronize();
    
    double time = context.Split();
    if(measure)
	cout << "GPU Kernel fetch_join_bitmap_kernel took " << time << endl << endl;
    
    //---- Transfer results ----

    thrust::host_vector<char> thrust_result(flag_array, flag_array+number_of_flags);
    std::vector<char> result(thrust_result.begin(), thrust_result.end());
    
    //---- Print results ----
    
    if(print) {
	cout << "--- Results from original GPU kernel ---" << endl;
	cout << "join-index (" << join_index_size << ")" << endl;
	for (int i = 0; i < join_index_size; i++)
	    cout << "\t" << data.primary_keys[i];
	cout << endl;
	for (int i = 0; i < join_index_size; i++)
	    cout << "\t" << data.foreign_keys[i];
	cout << endl << "matching-tids (" << num_tids << ")" << endl << "\t";
	for (int i = 0; i < num_tids; i++)
	    cout << ' ' << data.matching_tids[i];
	cout << endl << "flag-array of matching foreign keys (" << number_of_flags << ")" << endl << "\t";
	for (int i = 0; i < number_of_flags; i++)
	    cout << ' ' << (int)flag_array[i];
	cout << endl << endl;
    }
    
    return result;
}

void TestFetchJoin(CudaContext& context) {
  
    std::vector<char> res1, res2, res3;
  
    int join_index_size = 2 << 20;
    fetch_join_data data;
    GenerateFetchJoinData(data, join_index_size);
    
    res1 = DoFetchJoinGPU(context, data, false, false);
    
    res2 = DoFetchJoinThrust(context, data, false, false);

    res3 = DoFetchJoinModernGPU(context, data, false, false);
    
    bool correct=true;
    for(int i=0; i < join_index_size; i++) {
        char v1,v2,v3;
	v1 = res1.at(i);
	v2 = res2.at(i);
	v3 = res3.at(i);

	if(v1 != v2 | v2 != v3 | v1 != v3) {
	    correct=false;
	    cout << "error at " << i << " aborting test" << endl;
	    return;
	}
    }
    
    if(correct) 
      cout << "Tests were successful. All modules give the same results." << endl << endl;

}



void DoFetchJoin(CudaContext& context) {

    int join_index_size = 16;
    fetch_join_data data;
    GenerateFetchJoinData(data, join_index_size);

    DoFetchJoinGPU(context, data, true, false);

    DoFetchJoinModernGPU(context, data, true, false);
  
}


void DoMeasureFetchJoin(CudaContext& context) {

    int join_index_size = 2 << 26;
    fetch_join_data data;
    GenerateFetchJoinData(data, join_index_size);

    DoFetchJoinGPU(context, data, false, true);
    
    DoFetchJoinThrust(context, data, false, true);

    DoFetchJoinModernGPU(context, data, false, true);
  
}



int main(int argc, char** argv) {
	// Initialize a CUDA device on the default stream.
	ContextPtr context = CreateCudaDevice(argc, argv, true);
	srand (time(NULL));
	
	TestFetchJoin(*context);
	
	DoFetchJoin(*context);

	DoMeasureFetchJoin(*context);
	
	return 0;
}
