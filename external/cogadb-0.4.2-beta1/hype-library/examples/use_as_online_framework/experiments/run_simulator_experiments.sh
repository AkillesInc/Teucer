#!/bin/bash

#1MB=1073741824Byte
trap "Error! Experiments did not successfully complete!" SIGINT SIGTERM ERR SIGKILL

#NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT=10
NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT=5

#default parameters for benchmark
DEFAULT_NUMBER_OF_DATASETS=100 #100 #100
DEFAULT_MAX_DATASET_SIZE=10 #10 #10 #MB #50 #MB #53687091200 #50MB
DEFAULT_NUMBER_OF_OPERATIONS=10000 #2000 #2000
DEFAULT_RANDOM_SEED=10

DEFAULT_RELATIVE_SPEED_VECTOR="1,2" #two processing devices, where the second is twice as fast as the first
#"1,2,0.5" #three processing devices, where the second is twice as fast as the first and the third is 2 times slower
DEFAULT_CACHE_HITRATE=0.0 
DEFAULT_AVG_OPERATOR_SELECTIVITY=1.0 
#DEFAULT_RELATIVE_BUS_SPEED=1.0

#average measured Main Memory Bandwidth using mbw benchmark: 23436.635 MiB/s
#PCIe Express Bus Bandwith: 8GB/s
#relative Bus Speed: PCIe Express Bus Bandwith/Main Memory Bandwidth
#0.341345931<1 -> Bus is slower than main memory!
DEFAULT_RELATIVE_BUS_SPEED=0.341345931


#default values for HYPE!
export HYPE_LENGTH_OF_TRAININGPHASE=10
export HYPE_HISTORY_LENGTH=1000
export HYPE_RECOMPUTATION_PERIOD=100
export HYPE_ALGORITHM_MAXIMAL_IDLE_TIME=2
export HYPE_RETRAINING_LENGTH=1
export HYPE_MAXIMAL_SLOWDOWN_OF_NON_OPTIMAL_ALGORITHM_IN_PERCENT=50
export HYPE_READY_QUEUE_LENGTH=100

#if any program instance fails, the entire script failes and returns with an error code
set -e
#print each command before executing
set -o xtrace
#enable core dumps in case simulator crashes
ulimit -c unlimited

CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_DATASETS=false
CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_OPERATIONS=false
CONDUCT_EXPERIMENT_VARYING_MAXIMAL_DATASET_SIZE=false
CONDUCT_EXPERIMENT_VARYING_OPERATOR_QUEUE_LENGTH=false
CONDUCT_EXPERIMENT_VARYING_INITIAL_TRAINING_PHASE=false
#new for simulator
CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_PROCESSING_DEVICES=false
CONDUCT_EXPERIMENT_VARYING_RELATIVE_SPEED_OF_PROCESSING_DEVICES=false
CONDUCT_EXPERIMENT_VARYING_NUMBER_AND_RELATIVE_SPEED_OF_PROCESSING_DEVICES=false
CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_NUMBER_OF_PROCESSING_DEVICES=false
CONDUCT_EXPERIMENT_VARYING_AVERAGE_SELECTIVITY_AND_NUMBER_OF_PROCESSING_DEVICES=false
CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_AVERAGE_OPERATOR_SELECTIVITY=false

if [ $# -lt 1 ]; then
	echo 'Missing parameter!'
	echo "Usage: $0 [test|full|varying_number_datasets|varying_number_operations|varying_number_dataset_size|varying_operator_queue_length|varying_training_length|varying_number_of_processing_devices|varying_relative_speed_of_processing_devices|varying_number_and_relative_speed_of_processing_devices|varying_hitrate_and_number_of_processing_devices|varying_operator_selectivity_and_number_of_processing_devices|varying_hitrate_and_operator_selectivity]"
	exit -1
fi

if [ "$1" = "varying_number_datasets" ]; then
	CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_DATASETS=true
elif [ "$1" = "varying_number_operations" ]; then
	CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_OPERATIONS=true
elif [ "$1" = "varying_number_dataset_size" ]; then
	CONDUCT_EXPERIMENT_VARYING_MAXIMAL_DATASET_SIZE=true
elif [ "$1" = "varying_operator_queue_length" ]; then
	CONDUCT_EXPERIMENT_VARYING_OPERATOR_QUEUE_LENGTH=true
elif [ "$1" = "varying_training_length" ]; then
	CONDUCT_EXPERIMENT_VARYING_INITIAL_TRAINING_PHASE=true
elif [ "$1" = "varying_number_of_processing_devices" ]; then
        CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_PROCESSING_DEVICES=true
elif [ "$1" = "varying_relative_speed_of_processing_devices" ]; then
        CONDUCT_EXPERIMENT_VARYING_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true
elif [ "$1" = "varying_number_and_relative_speed_of_processing_devices" ]; then
        CONDUCT_EXPERIMENT_VARYING_NUMBER_AND_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true
elif [ "$1" = "varying_hitrate_and_number_of_processing_devices" ]; then
        CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_NUMBER_OF_PROCESSING_DEVICES=true
elif [ "$1" = "varying_operator_selectivity_and_number_of_processing_devices" ]; then
        CONDUCT_EXPERIMENT_VARYING_AVERAGE_SELECTIVITY_AND_NUMBER_OF_PROCESSING_DEVICES=true
elif [ "$1" = "varying_hitrate_and_operator_selectivity" ]; then
        CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_AVERAGE_OPERATOR_SELECTIVITY=true
elif [ "$1" = "full" ]; then
	CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_DATASETS=true
	CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_OPERATIONS=true
	CONDUCT_EXPERIMENT_VARYING_MAXIMAL_DATASET_SIZE=true
	CONDUCT_EXPERIMENT_VARYING_OPERATOR_QUEUE_LENGTH=true
	#CONDUCT_EXPERIMENT_VARYING_INITIAL_TRAINING_PHASE=1

        CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_PROCESSING_DEVICES=true
        CONDUCT_EXPERIMENT_VARYING_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true
        CONDUCT_EXPERIMENT_VARYING_NUMBER_AND_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true
        CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_NUMBER_OF_PROCESSING_DEVICES=true
        CONDUCT_EXPERIMENT_VARYING_AVERAGE_SELECTIVITY_AND_NUMBER_OF_PROCESSING_DEVICES=true
        CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_AVERAGE_OPERATOR_SELECTIVITY=true
fi

OPERATION_NAME="$2"

##if [ "$OPERATION_NAME" != "SORT" ] && [ "$OPERATION_NAME" != "SELECTION" ] && [ "$OPERATION_NAME" != "AGGREGATION" ]; then
#if [[ "$OPERATION_NAME" != "SORT" && "$OPERATION_NAME" != "SELECTION" && "$OPERATION_NAME" != "AGGREGATION" ]]; then
#	echo "Second parameter has to be a valid Operation: [AGGREGATION|SELECTION|SORT]"
#	echo "Your Input: $OPERATION_NAME"
#	echo "Aborting..."
#	exit -1
#fi

mkdir -p "eval/Results/$OPERATION_NAME"

#BENCHMARK_COMMAND=../bin/cogadb
#BENCHMARK_COMMAND=echo

#if [ "$OPERATION_NAME" == "SORT" ]; then
#	BENCHMARK_COMMAND=../bin/sort_benchmark 
#elif [ "$OPERATION_NAME" == "SELECTION" ]; then
#	BENCHMARK_COMMAND=../bin/selection_benchmark 
#elif [ "$OPERATION_NAME" == "AGGREGATION" ]; then
#	BENCHMARK_COMMAND=../bin/aggregation_benchmark 
#fi

BENCHMARK_COMMAND=../simulator 
#BENCHMARK_COMMAND="echo ../simulator"

rm -f benchmark_results.log
rm -f simulator.log

echo -n "Start Time of Experiments: " 
date

if [ "$1" = "test" ]; then
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 
fi







if $CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_DATASETS; then
for number_of_datasets in {50..200..50} {300..500..100} #values 50 to 200 in steps of 50 and values from 300 to 500 in steps of 100
do
	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --speed_vector="1" --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$number_of_datasets --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 
	done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_of_datasets_benchmark_results.data"  
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_of_datasets_simulator_results.data"  
fi


#--stemod_optimization_criterion="Simple Round Robin"

if $CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_OPERATIONS; then
for number_of_operations in {500..3000..500} {4000..8000..1000} #values 100 to 3000 in steps of 300
do
	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --speed_vector="1" --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$number_of_operations --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_of_operations_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_of_operations_simulator_results.data"
fi


##we devide with 4, because the benchmark expects the maximal data size in number of elements (an elements are form typ int, which is 4 byte on 32 bit platform)

if $CONDUCT_EXPERIMENT_VARYING_MAXIMAL_DATASET_SIZE; then
for max_dataset_size in {10..40..10} {50..150..50}
do
	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --speed_vector="1" --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
done
mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_dataset_size_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_dataset_size_simulator_results.data"
fi

if $CONDUCT_EXPERIMENT_VARYING_OPERATOR_QUEUE_LENGTH; then
for operator_queue_length in {10..40..10} {50..150..50}
do
	export HYPE_READY_QUEUE_LENGTH=$operator_queue_length
	echo "Ready Queue Length: $HYPE_READY_QUEUE_LENGTH"
	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --speed_vector="1" --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
done
mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_operator_queue_length_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_operator_queue_length_simulator_results.data"
fi

if $CONDUCT_EXPERIMENT_VARYING_INITIAL_TRAINING_PHASE; then
	echo "Error! NOT YET IMPLEMENTED VARYING TRANINGPHASE EXPERIMENTS SCRIPT!"
	exit -1;
fi


        #CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_PROCESSING_DEVICES=true
        #CONDUCT_EXPERIMENT_VARYING_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true
        #CONDUCT_EXPERIMENT_VARYING_NUMBER_AND_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true


if $CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_PROCESSING_DEVICES; then
for i in {2..20} # "1,1,1","1,1,1,1","1,1,1,1,1","1,1,1,1,1,1"} #{10..40..10} {50..150..50}
do
        speed_vector="1"
        for (( j=1; j<$i; j++ ))
        do
            speed_vector="$speed_vector,1"
        done

	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --speed_vector="1" --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_of_processing_devices_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_of_processing_devices_simulator_results.data"
fi

#elif [ "$1" = "varying_number_of_processing_devices" ]; then
#        CONDUCT_EXPERIMENT_VARYING_NUMBER_OF_PROCESSING_DEVICES=true
#elif [ "$1" = "varying_relative_speed_of_processing_devices" ]; then
#        CONDUCT_EXPERIMENT_VARYING_RELATIVE_SPEED_OF_PROCESSING_DEVICES=true
#elif [ "$1" = "varying_number_and_relative_speed_of_processing_devices" ]; then


if $CONDUCT_EXPERIMENT_VARYING_RELATIVE_SPEED_OF_PROCESSING_DEVICES; then
for i in {2..20} # "1,1,1","1,1,1,1","1,1,1,1,1","1,1,1,1,1,1"} #{10..40..10} {50..150..50}
do
        speed_vector="1"
 #       #half=i/2;
 #       #for (( j=1; j<6; j++ ))
 #       for (( j=1; j<2; j++ ))
 #       do
 #           value=$(echo "scale=8; 1/$i" | bc -q)
 #           speed_vector="$speed_vector,0$value"
 #       done
 #       #for (( j=6; j<11; j++ ))
        for (( j=2; j<3; j++ ))
        do
            speed_vector="$speed_vector,$i"
        done

	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --speed_vector="1" --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED  --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
done
mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_relative_speed_of_processing_devices_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_relative_speed_of_processing_devices_simulator_results.data"
fi

#combination of prior experiments
if $CONDUCT_EXPERIMENT_VARYING_NUMBER_AND_RELATIVE_SPEED_OF_PROCESSING_DEVICES; then

#generate relative speeds to consider in experiment
relative_speed_list=""

#first for cases where co-processors are to 2 to 10 times slower
for i in 10 #{10..2..2}; 
do relative_speed_list+=" 0"$(echo "scale=8; 1/$i" | bc -q); done 

#second for cases where co-processors are to 1 to 10 times faster
for i in 1 10 #{2..10..2}; 
do relative_speed_list+=" $i"; done 

echo "Relative Speeds to consider in EXPERIMENT_VARYING_NUMBER_AND_RELATIVE_SPEED_OF_PROCESSING_DEVICES: $relative_speed_list"

for number_of_processing_devices in {1..3} {4..20..4} #vary number of processing devices
do
    for relative_speed in $relative_speed_list #{2..10..2} #vary relative speeds
    do
        speed_vector="1"
        for (( j=1; j<$number_of_processing_devices; j++ ))
        do
            speed_vector="$speed_vector,$relative_speed"
            #value=$(echo "scale=8; 1/$relative_speed" | bc -q)
            #speed_vector="$speed_vector,0$value"
        done

	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --comment="relative_speed=$relative_speed" --speed_vector="1" --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED  --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
#		 $BENCHMARK_COMMAND --comment="relative_speed=$relative_speed" --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --comment="relative_speed=$relative_speed" --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
#		 $BENCHMARK_COMMAND --comment="relative_speed=$relative_speed" --speed_vector=$speed_vector --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 #$BENCHMARK_COMMAND --comment=$relative_speed --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
    done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_and_relative_speed_of_processing_devices_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_and_relative_speed_of_processing_devices_simulator_results.data"
fi

#combination of prior experiments
if $CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_NUMBER_OF_PROCESSING_DEVICES; then

echo "EXPERIMENT_VARYING_HITRATE_AND_NUMBER_OF_PROCESSING_DEVICES:"

for number_of_processing_devices in {1..3} {4..20..4} #vary number of processing devices
do
    for hitrate in {0..100..10} #vary average hitrate
    do
		  hitrate_float=$(echo "scale=2; $hitrate/100" | bc -q)
        speed_vector="1"
        for (( j=1; j<$number_of_processing_devices; j++ ))
        do
				#assume a Co-Processor is twice as fast as a CPU on Average (considering processsing power and memory bandwidth)
            speed_vector="$speed_vector,2"
        done

	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float" --speed_vector="1" --cache_hitrate=$hitrate_float --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED  --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
#		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float" --speed_vector=$speed_vector --cache_hitrate=$hitrate_float --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float" --speed_vector=$speed_vector --cache_hitrate=$hitrate_float --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
#		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float" --speed_vector=$speed_vector --cache_hitrate=$hitrate_float --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 #$BENCHMARK_COMMAND --comment=$relative_speed --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
    done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_hitrate_and_number_of_processing_devices_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_hitrate_and_number_of_processing_devices_simulator_results.data"
fi




#combination of prior experiments
if $CONDUCT_EXPERIMENT_VARYING_AVERAGE_SELECTIVITY_AND_NUMBER_OF_PROCESSING_DEVICES; then

echo "EXPERIMENT_VARYING_AVERAGE_SELECTIVITY_AND_NUMBER_OF_PROCESSING_DEVICES:"

for number_of_processing_devices in 1 2 3 4 5 6 7 8 #vary number of processing devices
do
    for average_selectivity in 0 10 20 100 #{0..100..10} #vary average average_selectivity
    do
		  average_selectivity_float=$(echo "scale=2; $average_selectivity/100" | bc -q)
        speed_vector="1"
        for (( j=1; j<$number_of_processing_devices; j++ ))
        do
				#assume a Co-Processor is twice as fast as a CPU on Average (considering processsing power and memory bandwidth)
            speed_vector="$speed_vector,2"
        done

	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --comment="average_operator_selectivity=$average_selectivity_float" --speed_vector="1" --cache_hitrate=1 --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED  --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
#		 $BENCHMARK_COMMAND --comment="average_operator_selectivity=$average_selectivity_float" --speed_vector=$speed_vector --cache_hitrate=1 --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --comment="average_operator_selectivity=$average_selectivity_float" --speed_vector=$speed_vector --cache_hitrate=1 --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
#		 $BENCHMARK_COMMAND --comment="average_operator_selectivity=$average_selectivity_float" --speed_vector=$speed_vector --cache_hitrate=1 --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 #$BENCHMARK_COMMAND --comment=$relative_speed --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
    done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_operator_selectivity_and_number_of_processing_devices_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_operator_selectivity_and_number_of_processing_devices_simulator_results.data"
fi


#combination of prior experiments
if $CONDUCT_EXPERIMENT_VARYING_HITRATE_AND_AVERAGE_OPERATOR_SELECTIVITY; then

echo "EXPERIMENT_VARYING_HITRATE_AND_AVERAGE_OPERATOR_SELECTIVITY:"
#set #PD to maximum
number_of_processing_devices=20
#compute speed vector
speed_vector="1"
for (( j=1; j<$number_of_processing_devices; j++ ))
do
	#assume a Co-Processor is twice as fast as a CPU on Average (considering processsing power and memory bandwidth)
   speed_vector="$speed_vector,2"
done

for average_selectivity in 0 10 20 50 100 #{0..100..10} #vary average average_selectivity
do
	 average_selectivity_float=$(echo "scale=2; $average_selectivity/100" | bc -q)
    for hitrate in {0..100..10} #vary average hitrate
    do
		  hitrate_float=$(echo "scale=2; $hitrate/100" | bc -q)
  

	for (( c=0; c<$NUMBER_OF_RUNS_FOR_EACH_EXPERIMENT; c++ ))
	do
		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float;average_selectivity=$average_selectivity_float" --speed_vector="1" --cache_hitrate=$hitrate_float --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED  --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
#		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float;average_selectivity=$average_selectivity_float" --speed_vector=$speed_vector --cache_hitrate=$hitrate_float --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float;average_selectivity=$average_selectivity_float" --speed_vector=$speed_vector --cache_hitrate=$hitrate_float --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
#		 $BENCHMARK_COMMAND --comment="hitrate=$hitrate_float;average_selectivity=$average_selectivity_float" --speed_vector=$speed_vector --cache_hitrate=$hitrate_float --average_operator_selectivity=$average_selectivity_float --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 #$BENCHMARK_COMMAND --comment=$relative_speed --speed_vector=$speed_vector --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

	done
    done
done

mv benchmark_results.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_hitrate_and_operator_selectivity_benchmark_results.data"
mv simulator.log "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_hitrate_and_operator_selectivity_simulator_results.data"
fi

echo -n "End Time of Experiments: " 
date

#for number_of_datasets in {100..3000..100} #values 50 to 300 in steps of 10
#do
#	for (( c=0; c<=10; c++ ))
#	do
#		$BENCHMARK_COMMAND --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
#		$BENCHMARK_COMMAND --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
#		$BENCHMARK_COMMAND --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$DEFAULT_MAX_DATASET_SIZE --scheduling_method="HYBRID" --random_seed=$DEFAULT_RANDOM_SEED
#	done
#done

echo "Experiments have sucessfully finished!"

exit 0

		 $BENCHMARK_COMMAND --speed_vector="1" --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="CPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 #$BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="GPU_ONLY" --random_seed=$DEFAULT_RANDOM_SEED
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Simple Round Robin" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Response Time" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="WaitingTimeAwareResponseTime" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Throughput" --random_seed=$DEFAULT_RANDOM_SEED 
		 $BENCHMARK_COMMAND --speed_vector=$DEFAULT_RELATIVE_SPEED_VECTOR --cache_hitrate=$DEFAULT_CACHE_HITRATE --average_operator_selectivity=$DEFAULT_AVG_OPERATOR_SELECTIVITY --relative_bus_speed=$DEFAULT_RELATIVE_BUS_SPEED --number_of_datasets=$DEFAULT_NUMBER_OF_DATASETS --number_of_operations=$DEFAULT_NUMBER_OF_OPERATIONS --max_dataset_size_in_MB=$max_dataset_size --scheduling_method="HYBRID" --optimization_criterion="Throughput2" --random_seed=$DEFAULT_RANDOM_SEED 

#varying number of processing devices + varying hitrate
#varying number of processing devices + bus speed
#varying hitrate and varying selectivity
#varying number of processing devices and relative speed of processing devices

