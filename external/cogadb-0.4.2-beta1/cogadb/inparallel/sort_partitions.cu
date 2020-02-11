#include <iostream>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <thrust/host_vector.h>
#include <thrust/device_vector.h>
#include <thrust/sort.h>
#include <moderngpu.cuh>

using namespace std;
using namespace thrust;
using namespace mgpu;

vector<int> generate_unique_data(int num_elements) {

	vector<int> data(num_elements);
        for(unsigned long int j=0; j<num_elements; j++){
	    data[j] = j;
	}
	std::random_shuffle( data.begin(), data.end() );
	return data;
}

vector<int> generate_data(int num_elements) {

	vector<int> data(num_elements);
        for(unsigned long int j=0; j<num_elements; j++){
	    data[j] = std::rand()%num_elements;
	}

	return data;
}


template <int NT, int VT> float blockSortMGPU(vector<int>& host_data, CudaContext &context, int input_size, int numExecutions) {

//template<typename T, typename Comp>
//MGPU_HOST void MergesortKeys(T* data_global, int count, Comp comp,
//    CudaContext& context) {

    device_vector<int> data = host_data;
    int* devptr_input = thrust::raw_pointer_cast(&data[0]);
    
    device_vector<int> output(input_size);;
    int* devptr_output = thrust::raw_pointer_cast(&output[0]); 

    typedef LaunchBoxVT<NT, VT> Tuning;
    int2 launch = Tuning::GetLaunchParams(context);
     
    const int NV = NT * VT;
    
    //cout << "Block length NV: " << NV << endl;
    
    int numBlocks = MGPU_DIV_UP(input_size, NV);
    //int numPasses = FindLog2(numBlocks, true);
 
    //MGPU_MEM(T) destDevice = context.Malloc<T>(count);
    //T* source = data_global;
    //T* dest = destDevice->get();
 
    context.Start();
    
    KernelBlocksort<Tuning, false><<<numBlocks, NT, 0, context.Stream()>>>(devptr_input, (const int*)0, input_size, devptr_output, (int*)0, mgpu::less<int>());
    
    //if(1 & numPasses) std::swap(source, dest);


    float time = context.Split();
    
    cout << input_size / NV << ", " << time << endl;
    
    return time;
}


float sortPartitionsMGPU(vector<int>& host_data, CudaContext &context, int input_size, int num_partitions, int numExecutions) {
    
    
    float time_sum = 0.0f;
    
    for(int i=0; i<numExecutions; i++) {
        device_vector<int> data = host_data;
        int num_elements = input_size / num_partitions;
        context.Start();
        int start = 0;

        while(start < input_size) {
            //end = std::min(input_size, start + num_elements);
            

            int* devptr_input = thrust::raw_pointer_cast(&data[start]);
            mgpu::MergesortKeys(devptr_input, num_elements, mgpu::less<int>(), context);

            
            start += num_elements;
        }
        time_sum += context.Split();
    }
    return time_sum / numExecutions;

}

float sortPartitionsThrust(vector<int>& host_data, CudaContext &context, int input_size, int num_partitions, int numExecutions) {
    
    float time_sum = 0.0f;
    
    for(int i=0; i<numExecutions; i++) {
        device_vector<int> data = host_data;
        int num_elements = input_size / num_partitions;
        context.Start();
        int start = 0;
        int end;
        while(start < input_size) {
            end = std::min(input_size, start + num_elements);
            thrust::sort(data.begin()+start, data.begin()+end);
            start += num_elements;
        }
        time_sum += context.Split();
    }
    return time_sum / numExecutions;

}

template<int NT> void test_partitioned_sort(vector<int>& host_data, CudaContext &context, int size, int numExecutions) {
    cout << "blocksize " << NT << endl;
    
    float time3 = 0;

//    time3 = blockSortMGPU<NT,1>(host_data, context, size, numExecutions);
//
//    time3 = blockSortMGPU<NT,3>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,5>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,7>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,9>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,11>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,13>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,15>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,17>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,19>(host_data, context, size, numExecutions);
//
//    time3 = blockSortMGPU<NT,21>(host_data, context, size, numExecutions);
//
//    time3 = blockSortMGPU<NT,23>(host_data, context, size, numExecutions);
//    
//    time3 = blockSortMGPU<NT,25>(host_data, context, size, numExecutions);
//        
//    time3 = blockSortMGPU<NT,27>(host_data, context, size, numExecutions);

    //time3 = blockSortMGPU<NT,29>(host_data, context, size, numExecutions);
                
    //time3 = blockSortMGPU<NT,31>(host_data, context, size, numExecutions);

    time3 = blockSortMGPU<NT,32>(host_data, context, size, numExecutions);
    
    time3 = blockSortMGPU<NT,33>(host_data, context, size, numExecutions);
    
    time3 = blockSortMGPU<NT,34>(host_data, context, size, numExecutions);
    
    time3 = blockSortMGPU<NT,35>(host_data, context, size, numExecutions);
    
    time3 = blockSortMGPU<NT,36>(host_data, context, size, numExecutions);
    
    time3 = blockSortMGPU<NT,43>(host_data, context, size, numExecutions);
}

int main(int argc, char** argv) {
	
    cout << "Performance test: Sorting partitions" << endl << endl;
  
    ContextPtr context = CreateCudaDevice(0, 0, true);
    

    
    int size = 2 << 26;
    int numPartitions = 1;
    int numExecutions = 1;
    float time1 = 0;
    float time2 = 0;
    
    vector<int> host_data = generate_data(size);
    
    cout << "Data size is " << (sizeof(int) * size / (2 << 20)) << "MB" << endl;
    
    for(int i = 1; i < 32; i++) {
        
        cout << numPartitions << ", ";

        if(time1 < 6) {
            time1 = sortPartitionsThrust(host_data, *context, size, numPartitions, numExecutions);
            cout << time1;
        }
        cout << ", ";
        
        if(time2 < 6) {
            time2 = sortPartitionsMGPU(host_data, *context, size, numPartitions, numExecutions);
            cout << time2;
        }
        cout << endl;
        
        numPartitions *= 2;
    }
    
    
    test_partitioned_sort<128>(host_data, *context, size, numExecutions);
    
    test_partitioned_sort<256>(host_data, *context, size, numExecutions);
    
    //test_partitioned_sort<386>(host_data, *context, size, numExecutions);
    
    //test_partitioned_sort<512>(host_data, *context, size, numExecutions);
    
    //impossible, too much shared memory
    //test_partitioned_sort<640>(host_data, *context, size, numExecutions);
 
    //test_partitioned_sort<768>(host_data, *context, size, numExecutions);
    
    //test_partitioned_sort<896>(host_data, *context, size, numExecutions);
    
    //test_partitioned_sort<1024>(host_data, *context, size, numExecutions);
    
    return 0;
}

