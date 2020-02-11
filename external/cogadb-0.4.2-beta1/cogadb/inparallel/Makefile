all: bitmap-conversion fetch-join sort-partitions

bitmap-conversion: bitmap_conversion.cu
	nvcc -arch=sm_30 -I moderngpu/include/ -o bitmap-conversion moderngpu/src/mgpucontext.cu moderngpu/src/mgpuutil.cpp bitmap_conversion.cu

fetch-join: fetch_join.cu
	nvcc -arch=sm_30 -I moderngpu/include/ -o fetch-join moderngpu/src/mgpucontext.cu moderngpu/src/mgpuutil.cpp fetch_join.cu
	
sort-partitions: sort_partitions.cu
	nvcc -arch=sm_30 -I moderngpu/include/ -o sort-partitions moderngpu/src/mgpucontext.cu moderngpu/src/mgpuutil.cpp sort_partitions.cu
