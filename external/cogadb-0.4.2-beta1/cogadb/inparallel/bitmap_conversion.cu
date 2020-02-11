#include <moderngpu.cuh>
#include <thrust/scatter.h>
#include <thrust/unique.h>
#include <thrust/host_vector.h>
#include <thrust/device_vector.h>
#include <thrust/iterator/constant_iterator.h>
#include <iostream>
#include <vector>

using namespace mgpu;
using namespace std;


void PositionListToBitmapCPU(unsigned int* positionlist, unsigned int length, char *flagarray) {
  
    for(int i = 0; i < length; i++) {
	flagarray[positionlist[i]] = 1;
    }

}



vector<char> DoPositionListToBitmapCPU(CudaContext& context, unsigned int positionlist_size, unsigned int table_size, bool print, bool measure) {
	
	std::vector<unsigned int> positionlist;
	std::vector<char> flagarray(table_size);
	
	for(int i = 0; i < positionlist_size; i++)
	    positionlist.push_back(rand() % table_size);
	
	
	context.Start();
	
	PositionListToBitmapCPU(&positionlist[0], positionlist_size, &flagarray[0]);
	
	double time = context.Split();
	if(measure)
	    cout << "CPU PositionlistToBitmap took " << time << endl;
	
	
	if(print) {
	    for (std::vector<unsigned int>::iterator it=positionlist.begin(); it!=positionlist.end(); ++it)
		std::cout << ' ' << *it;
	    cout << endl;
	    for (std::vector<char>::iterator it=flagarray.begin(); it!=flagarray.end(); ++it)
		std::cout << ' ' << (int)*it;
	    cout << endl << endl;
	  
	}
	
	
	return flagarray;
}


__global__ void convertPositionListToBitmap_create_flagarray_kernel(unsigned int* tids, size_t num_tids, size_t num_rows_base_table, char* flag_array){
    
    const int VT=1;
    
    unsigned int tids_start = threadIdx.x*VT + blockIdx.x * blockDim.x;
    unsigned int tids_end = tids_start+VT;
    
    unsigned int thread_tids[VT];
    
    while(tids_end < num_tids) {
        
            for(int i=0; i < VT; i++) {
                thread_tids[i] = tids[tids_start + i];
            }

            for(int i=0; i < VT; i++) {
                flag_array[thread_tids[i]] = 1;
            }
        
        
        tids_start += VT*blockDim.x*gridDim.x;
        tids_end = (tids_start + VT) < num_tids ? tids_start + VT : num_tids;
    }

}


__global__ void convertPositionListToBitmap_create_int_flagarray_kernel(unsigned int* tids, size_t num_tids, size_t num_rows_base_table, int* flag_array){
    
    const int VT=1;
    
    unsigned int tids_start = threadIdx.x*VT + blockIdx.x * blockDim.x;
    unsigned int tids_end = tids_start+VT;
    
    unsigned int thread_tids[VT];
    
    while(tids_end < num_tids) {
        
            for(int i=0; i < VT; i++) {
                thread_tids[i] = tids[tids_start + i];
            }

            for(int i=0; i < VT; i++) {
                flag_array[thread_tids[i]] = 1;
            }
        
        
        tids_start += VT*blockDim.x*gridDim.x;
        tids_end = (tids_start + VT) < num_tids ? tids_start + VT : num_tids;
    }

}


void DoPositionlistToBitmapGPU(CudaContext& context, unsigned int positionlist_size, unsigned int table_size, bool print, bool measure) {
    
    std::vector<unsigned int> positionlist;
    std::vector<char> flagarray(table_size);
    
    for(int i = 0; i < positionlist_size; i++)
	positionlist.push_back(rand() % table_size);
    
    thrust::device_vector<int> device_flagarray(flagarray.begin(), flagarray.end());
    thrust::device_vector<unsigned int> device_positionlist(positionlist.begin(), positionlist.end());
    
    const int number_of_blocks=64;
    const int number_of_threads_per_block=128;
    
    
    unsigned int* devPtr1 = thrust::raw_pointer_cast(&device_positionlist[0]);
    int *devPtr2 = thrust::raw_pointer_cast(&device_flagarray[0]);
    
    context.Start();
    
    convertPositionListToBitmap_create_int_flagarray_kernel<<<number_of_blocks,number_of_threads_per_block>>>(
	    devPtr1, positionlist_size, table_size, devPtr2);
    cudaDeviceSynchronize();
    
    double time = context.Split();
    if(measure)
	cout << "GPU Kernel PositionlistToBitmap took " << time << endl;
    
    if(print) {
	for (int i = 0; i < device_positionlist.size(); i++)
	    std::cout << ' ' << device_positionlist[i];
	cout << endl;
	for (int i = 0; i < device_flagarray.size(); i++)
	    std::cout << ' ' << (int)device_flagarray[i];
	cout << endl << endl;
    }



}


void PositionListToBitmapThrust(thrust::device_vector<unsigned int> &positionlist, thrust::device_vector<char> &flag_array) {

    thrust::device_vector<char> bits(positionlist.size(), 1);
  
    thrust::scatter(bits.begin(), bits.end(), positionlist.begin(), flag_array.begin());

}


void DoPositionlistToBitmapThrust(CudaContext& context, unsigned int positionlist_size, unsigned int table_size, bool print, bool measure) {

    std::vector<unsigned int> positionlist;
    std::vector<char> flagarray(table_size);
    
    for(int i = 0; i < positionlist_size; i++)
	positionlist.push_back(rand() % table_size);
    
    thrust::device_vector<char> device_flagarray(flagarray.begin(), flagarray.end());
    thrust::device_vector<unsigned int> device_positionlist(positionlist.begin(), positionlist.end());
    
    context.Start();
    
    PositionListToBitmapThrust(device_positionlist, device_flagarray);
	
    double time = context.Split();
    if(measure)
	cout << "Thrust PositionlistToBitmap took " << time << endl;
    
    if(print) {
	for (int i = 0; i < device_positionlist.size(); i++)
	    std::cout << ' ' << device_positionlist[i];
	cout << endl;
	for (int i = 0; i < device_flagarray.size(); i++)
	    std::cout << ' ' << (int)device_flagarray[i];
	cout << endl << endl;
    }
}



void PositionListToBitmapModernGPU(CudaContext& context, thrust::device_vector<unsigned int> &positionlist, thrust::device_vector<char> &flag_array) {

      unsigned int *devIn1 = thrust::raw_pointer_cast(&positionlist[0]);

      MergesortKeys(devIn1, positionlist.size(), mgpu::less<int>(), context);



}


void DoPositionlistToBitmapModernGPU(CudaContext& context, unsigned int positionlist_size, unsigned int table_size, bool print, bool measure) {

    std::vector<unsigned int> positionlist;
    std::vector<char> flagarray(table_size);
    
    for(int i = 0; i < positionlist_size; i++)
	positionlist.push_back(rand() % table_size);
    
    thrust::device_vector<char> device_flagarray(flagarray.begin(), flagarray.end());
    thrust::device_vector<unsigned int> device_positionlist(positionlist.begin(), positionlist.end());
    
    context.Start();
    
    PositionListToBitmapModernGPU(context, device_positionlist, device_flagarray);
	
    double time = context.Split();
    if(measure)
	cout << "ModernGPU PositionlistToBitmap took " << time << endl;
    
    if(print) {
	for (int i = 0; i < device_positionlist.size(); i++)
	    std::cout << ' ' << device_positionlist[i];
	cout << endl;
	for (int i = 0; i < device_flagarray.size(); i++)
	    std::cout << ' ' << (int)device_flagarray[i];
	cout << endl << endl;
    }
}



void DoPositionlistToBitmap(CudaContext& context) {

    int positionlist_size = 2 << 4;
    int table_size = 2 << 5;

    DoPositionListToBitmapCPU(context, positionlist_size, table_size, true, false);
	
    DoPositionlistToBitmapThrust(context, positionlist_size, table_size, true, false);

    DoPositionlistToBitmapGPU(context, positionlist_size, table_size, true, false);
}


void DoMeasurePositionlistToBitmap(CudaContext& context) {

    int positionlist_size = 2 << 25;
    int table_size = 2 << 26;

    DoPositionListToBitmapCPU(context, positionlist_size, table_size, false, true);
	
    DoPositionlistToBitmapThrust(context, positionlist_size, table_size, false, true);
  
    DoPositionlistToBitmapGPU(context, positionlist_size, table_size, false, true);
    
    DoPositionlistToBitmapModernGPU(context, positionlist_size, table_size, false, true);
}





template<int NT, typename InputIt1, typename InputIt2, typename OutputIt,
    typename Comp>
__global__ void ParallelMergeA(InputIt1 a_global, int aCount, InputIt2 b_global,
    int bCount, OutputIt dest_global, Comp comp) {
 
    typedef typename std::iterator_traits<InputIt1>::value_type T;
 
    int gid = threadIdx.x + NT * blockIdx.x;
    if(gid < aCount) {
        T aKey = a_global[gid];
        int lb = BinarySearch<MgpuBoundsLower>(b_global, bCount, aKey, comp);
        dest_global[gid + lb] = aKey;
    }
}
 
template<int NT, typename InputIt1, typename InputIt2, typename OutputIt,
    typename Comp>
__global__ void ParallelMergeB(InputIt1 a_global, int aCount, InputIt2 b_global,
    int bCount, OutputIt dest_global, Comp comp) {
 
    typedef typename std::iterator_traits<InputIt2>::value_type T;
 
    int gid = threadIdx.x + NT * blockIdx.x;
    if(gid < bCount) {
        T bKey = b_global[gid];
        int ub = BinarySearch<MgpuBoundsUpper>(a_global, aCount, bKey, comp);
        dest_global[gid + ub] = bKey;
    }
}


void DoMerge(CudaContext& context) {
	printf("\n\nMERGE DEMONSTRATION\n\n");

	int ACount = 30;
	int BCount = 30;

	MGPU_MEM(int) aKeysDevice = context.SortRandom<int>(ACount, 100, 130);
	MGPU_MEM(int) bKeysDevice = context.SortRandom<int>(BCount, 100, 130);
	MGPU_MEM(int) cKeysDevice = context.Malloc<int>(ACount+BCount);

	printf("A keys:\n");
	PrintArray(*aKeysDevice, "%4d", 10);

	printf("\nB keys:\n");
	PrintArray(*bKeysDevice, "%4d", 10);
	
	const int NT = 512;
	int ablocks = MGPU_DIV_UP(ACount, NT);
	int bblocks = MGPU_DIV_UP(BCount, NT);
	ParallelMergeA<NT><<<ablocks, NT>>>(aKeysDevice->get(), ACount, bKeysDevice->get(), BCount, cKeysDevice->get(), mgpu::less<int>());
	ParallelMergeB<NT><<<bblocks, NT>>>(aKeysDevice->get(), ACount, bKeysDevice->get(), BCount, cKeysDevice->get(), mgpu::less<int>());

	printf("\nC keys:\n");
	PrintArray(*cKeysDevice, "%4d", 10);
}


void DoJoin(CudaContext& context) {
	printf("\n\nRELATIONAL JOINS DEMONSTRATION\n\n");

	int ACount = 30;
	int BCount = 30;

	MGPU_MEM(int) aKeysDevice = context.SortRandom<int>(ACount, 100, 130);
	MGPU_MEM(int) bKeysDevice = context.SortRandom<int>(BCount, 100, 130);
	std::vector<int> aKeysHost, bKeysHost;
	aKeysDevice->ToHost(aKeysHost);
	bKeysDevice->ToHost(bKeysHost);

	printf("A keys:\n");
	PrintArray(*aKeysDevice, "%4d", 10);

	printf("\nB keys:\n");
	PrintArray(*bKeysDevice, "%4d", 10);

	MGPU_MEM(int) aIndices, bIndices;
	int innerCount = RelationalJoin<MgpuJoinKindInner>(aKeysDevice->get(),
		ACount, bKeysDevice->get(), BCount, &aIndices, &bIndices, context);

	std::vector<int> aHost, bHost;
	aIndices->ToHost(aHost);
	bIndices->ToHost(bHost);

	printf("\nInner-join (%d items):\n", innerCount);
	printf("output   (aIndex, bIndex) : (aKey, bKey)\n");
	printf("----------------------------------------\n");
	for(int i = 0; i < innerCount; ++i)
		printf("%3d      (%6d, %6d) : (%4d, %4d)\n", i, aHost[i], bHost[i],
			aKeysHost[aHost[i]], bKeysHost[bHost[i]]);

	int outerCount = RelationalJoin<MgpuJoinKindOuter>(aKeysDevice->get(),
		ACount, bKeysDevice->get(), BCount, &aIndices, &bIndices, context);

	aIndices->ToHost(aHost);
	bIndices->ToHost(bHost);
	printf("\nOuter-join (%d items):\n", outerCount);
	printf("output   (aIndex, bIndex) : (aKey, bKey)\n");
	printf("----------------------------------------\n");
	for(int i = 0; i < outerCount; ++i) {
		std::string aKey, bKey;
		if(-1 != aHost[i]) aKey = stringprintf("%4d", aKeysHost[aHost[i]]);
		if(-1 != bHost[i]) bKey = stringprintf("%4d", bKeysHost[bHost[i]]);
		printf("%3d      (%6d, %6d) : (%4s, %4s)\n", i, aHost[i], bHost[i],
			(-1 != aHost[i]) ? aKey.c_str() : "---", 
			(-1 != bHost[i]) ? bKey.c_str() : "---");
	}
}





int main(int argc, char** argv) {
	// Initialize a CUDA device on the default stream.
	ContextPtr context = CreateCudaDevice(argc, argv, true);
	
	DoPositionlistToBitmap(*context);

	DoMeasurePositionlistToBitmap(*context);
	
	return 0;
}
