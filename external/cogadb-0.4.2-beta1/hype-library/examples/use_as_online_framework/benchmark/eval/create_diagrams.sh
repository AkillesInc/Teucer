#!/bin/bash

./eval.sh "Results/$HOSTNAME-varying_dataset_size_benchmark_results.data" varying_dataset_size
./eval.sh "Results/$HOSTNAME-varying_number_of_operations_benchmark_results.data" varying_number_of_operations
./eval.sh "Results/$HOSTNAME-varying_number_of_datasets_benchmark_results.data" varying_number_of_datasets
./eval.sh "Results/$HOSTNAME-varying_operator_queue_length_benchmark_results.data" varying_operator_queue_length


for i in varying_dataset_size varying_number_of_operations varying_number_of_datasets varying_operator_queue_length; do
	cd $i
	gnuplot execution_times.plt
	cd ..
done

exit 0
