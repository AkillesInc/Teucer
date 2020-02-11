#!/bin/bash

OPERATION_NAME="SIMULATION_OPERATION"
#EVALUATION_MACHINE=$HOSTNAME
#EVALUATION_MACHINE=sebastian-ESPRIMO-P700
EVALUATION_MACHINE=$HOSTNAME #dell-laptop


#paste "eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_and_relative_speed_of_processing_devices_benchmark_results.data"

#cat eval/Results/$OPERATION_NAME/$HOSTNAME-varying_number_and_relative_speed_of_processing_devices_simulator_results.data | awk 'BEGIN{FS=' ';} {print $4}'

################################################################################################################################################################
#EXTRACT DATA FOR EXPERIMENT: RELATIVE SPEED AND NUMBER OF PROCESSING DEVICES
#extract the relative speed value from simulator logfile
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_number_and_relative_speed_of_processing_devices_simulator_results.data  | awk 'BEGIN{FS=" ";} {print $5}' | sed -e "s/comment=relative_speed=//g" > relative_speed_values.data

#extract relevant data from benchmark logfile
#result has schema: sched_config(0 CPU_ONLY, 2 HYBRID);Optimization heuristic;Workload execution time; training time; #processing devices
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_number_and_relative_speed_of_processing_devices_benchmark_results.data | awk 'BEGIN{FS="\t";} {print $4"\t"$7"\t"$16"\t"$17"\t"$18}' > benchmark.data

paste relative_speed_values.data benchmark.data > varying_number_and_relative_speed_of_processing_devices_evaluation.data
rm relative_speed_values.data benchmark.data

################################################################################################################################################################
#EXTRACT DATA FOR EXPERIMENT: CACHE HITRATE AND NUMBER OF PROCESSING DEVICES
#extract the relative speed value from simulator logfile
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_hitrate_and_number_of_processing_devices_simulator_results.data | awk 'BEGIN{FS=" ";} {print $2}' | sed -e "s/cache_hitrate=//g" > cache_hitrate_values.data

#extract relevant data from benchmark logfile
#result has schema: sched_config(0 CPU_ONLY, 2 HYBRID);Optimization heuristic;Workload execution time; training time; #processing devices
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_hitrate_and_number_of_processing_devices_benchmark_results.data | awk 'BEGIN{FS="\t";} {print $4"\t"$7"\t"$16"\t"$17"\t"$18}' > benchmark.data

paste cache_hitrate_values.data benchmark.data > hitrate_and_number_of_processing_devices_evaluation.data
rm cache_hitrate_values.data benchmark.data




################################################################################################################################################################
#EXTRACT DATA FOR EXPERIMENT: AVERAGE OPERATOR SELECTIVITY AND NUMBER OF PROCESSING DEVICES
#extract the relative speed value from simulator logfile
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_operator_selectivity_and_number_of_processing_devices_simulator_results.data | awk 'BEGIN{FS=" ";} {print $3}' | sed -e "s/average_operator_selectivity=//g" > operator_selectivity_values.data

#extract relevant data from benchmark logfile
#result has schema: sched_config(0 CPU_ONLY, 2 HYBRID);Optimization heuristic;Workload execution time; training time; #processing devices
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_operator_selectivity_and_number_of_processing_devices_benchmark_results.data | awk 'BEGIN{FS="\t";} {print $4"\t"$7"\t"$16"\t"$17"\t"$18}' > benchmark.data

paste operator_selectivity_values.data benchmark.data > operator_selectivity_and_number_of_processing_devices_evaluation.data
rm operator_selectivity_values.data benchmark.data

################################################################################################################################################################
#EXTRACT DATA FOR EXPERIMENT: HITRATE and AVERAGE OPERATOR SELECTIVITY
#extract the hitrate value from simulator logfile
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_hitrate_and_operator_selectivity_simulator_results.data | awk 'BEGIN{FS=" ";} {print $2}' | sed -e "s/cache_hitrate=//g" > hitrate_values.data

cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_hitrate_and_operator_selectivity_simulator_results.data | awk 'BEGIN{FS=" ";} {print $3}' | sed -e "s/average_operator_selectivity=//g" > operator_selectivity_values.data

#extract relevant data from benchmark logfile
#result has schema: sched_config(0 CPU_ONLY, 2 HYBRID);Optimization heuristic;Workload execution time; training time; #processing devices
cat Results/$OPERATION_NAME/$EVALUATION_MACHINE-varying_hitrate_and_operator_selectivity_benchmark_results.data | awk 'BEGIN{FS="\t";} {print $4"\t"$7"\t"$16"\t"$17"\t"$18}' > benchmark.data

paste hitrate_values.data benchmark.data operator_selectivity_values.data > hitrate_and_operator_selectivity_evaluation.data
rm hitrate_values.data operator_selectivity_values.data benchmark.data

################################################################################################################################################################

#cat varying_number_and_relative_speed_of_processing_devices_evaluation.data

export LC_ALL=C
#sort first after column 1 (relative speed), then after column 6 (# processing devices)
sort --stable -k1,6 -g varying_number_and_relative_speed_of_processing_devices_evaluation.data > varying_number_and_relative_speed_of_processing_devices_evaluation_sorted.data
#sort first after column 1 (cache hitrate), then after column 6 (# processing devices)
sort --stable -k1,6 -g hitrate_and_number_of_processing_devices_evaluation.data > varying_hitrate_and_number_of_processing_devices_evaluation_sorted.data
#sort first after column 1 (average operator selectivity), then after column 6 (# processing devices)
sort --stable -k1,6 -g operator_selectivity_and_number_of_processing_devices_evaluation.data > varying_operator_selectivity_and_number_of_processing_devices_evaluation_sorted.data
#sort first after column 1 (cache hitrate), then after column 7 (average operator selectivity)
sort --stable -k1,7 -g hitrate_and_operator_selectivity_evaluation.data > varying_hitrate_and_operator_selectivity_evaluation_sorted.data


#rm varying_number_and_relative_speed_of_processing_devices_evaluation.data

mkdir -p $OPERATION_NAME-varying_number_and_relative_speed_of_processing_devices
mkdir -p $OPERATION_NAME-varying_hitrate_and_number_of_processing_devices
mkdir -p $OPERATION_NAME-varying_operator_selectivity_and_number_of_processing_devices
mkdir -p $OPERATION_NAME-varying_hitrate_and_operator_selectivity

mv varying_number_and_relative_speed_of_processing_devices_evaluation_sorted.data $OPERATION_NAME-varying_number_and_relative_speed_of_processing_devices/
mv varying_hitrate_and_number_of_processing_devices_evaluation_sorted.data $OPERATION_NAME-varying_hitrate_and_number_of_processing_devices/
mv varying_operator_selectivity_and_number_of_processing_devices_evaluation_sorted.data $OPERATION_NAME-varying_operator_selectivity_and_number_of_processing_devices/
mv varying_hitrate_and_operator_selectivity_evaluation_sorted.data $OPERATION_NAME-varying_hitrate_and_operator_selectivity/

for EXPERIMENT_NAME in varying_number_and_relative_speed_of_processing_devices varying_hitrate_and_number_of_processing_devices varying_operator_selectivity_and_number_of_processing_devices varying_hitrate_and_operator_selectivity; do
cd $OPERATION_NAME-$EXPERIMENT_NAME/

	DATAFILE_CONTAINING_RESULTS="$EXPERIMENT_NAME"_evaluation_sorted.data

	cat $DATAFILE_CONTAINING_RESULTS | awk '{if($2==0){print}}' | tee cpu_only.data
	cat $DATAFILE_CONTAINING_RESULTS | awk '{if($2==1){print}}' | tee gpu_only.data

	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Response Time"){print}}' | tee response_time.data
	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Simple Round Robin"){print}}' | tee simple_round_robin.data
	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="WaitingTimeAwareResponseTime"){print}}' | tee waiting_time_aware_response_time.data
	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Throughput"){print}}' | tee throughput.data
	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Throughput2"){print}}' | tee throughput2.data

#aggregate each 10 lines by computing average of it
#ATTENTION: the 10 corresponds to the number of rounds per experiment, if the number of roudn is modified, this script has to be modifed as well!
for i in cpu_only.data gpu_only.data response_time.data simple_round_robin.data waiting_time_aware_response_time.data throughput.data throughput2.data; do
	cat "$i" | awk '
BEGIN{FS="\t";
sum_execution_time=0;
sum_training_time=0;
number_of_rounds=5;
} 

{sum_execution_time+=$4; 
sum_training_time+=$5; 

if(NR%number_of_rounds==0){
exec_time_avg=sum_execution_time/number_of_rounds;
training_time_avg=sum_training_time/number_of_rounds;

print $0"\t"exec_time_avg"\t"training_time_avg
sum_execution_time=0;
sum_training_time=0;
next
}}' > averaged_$i
done


	#cp varying_number_and_relative_speed_of_processing_devices_evaluation_sorted.data results.csv

#fetch script template	
cp ../script_templates/speedup_$EXPERIMENT_NAME.gnuplot .
cat ../script_templates/speedups_$EXPERIMENT_NAME.plt | sed 's/%OPERATION_NAME%/'"$OPERATION_NAME"'/g' > speedups_$EXPERIMENT_NAME.plt

if [ "$EXPERIMENT_NAME" = "varying_hitrate_and_operator_selectivity" ]; then

	cat averaged_cpu_only.data | awk '{print $9}' > avg_cpu_execution_times.tmp
	paste averaged_waiting_time_aware_response_time.data avg_cpu_execution_times.tmp > wtar_results.data
	rm avg_cpu_execution_times.tmp

	cat wtar_results.data | awk '{if(NR%11==0){print; print ""}else{print}}' > tmp
	cp wtar_results.data wtar_results_2d_plot.data
	mv tmp wtar_results.data 

else

	cat averaged_cpu_only.data | awk '{print $8}' > avg_cpu_execution_times.tmp
	paste averaged_waiting_time_aware_response_time.data avg_cpu_execution_times.tmp > wtar_results.data
	rm avg_cpu_execution_times.tmp

	cat wtar_results.data | awk '{if(NR%8==0){print; print ""}else{print}}' > tmp
	cp wtar_results.data wtar_results_2d_plot.data
	mv tmp wtar_results.data 

fi

gnuplot -p speedup_$EXPERIMENT_NAME.gnuplot
gnuplot -p speedups_$EXPERIMENT_NAME.plt

	#bash -x prepareGnuplotCSV.sh

cd ..
done

exit

#cd $OPERATION_NAME-varying_number_and_relative_speed_of_processing_devices/

#	DATAFILE_CONTAINING_RESULTS=varying_number_and_relative_speed_of_processing_devices_evaluation_sorted.data

#	cat $DATAFILE_CONTAINING_RESULTS | awk '{if($2==0){print}}' | tee cpu_only.data
#	cat $DATAFILE_CONTAINING_RESULTS | awk '{if($2==1){print}}' | tee gpu_only.data

#	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Response Time"){print}}' | tee response_time.data
#	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Simple Round Robin"){print}}' | tee simple_round_robin.data
#	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="WaitingTimeAwareResponseTime"){print}}' | tee waiting_time_aware_response_time.data
#	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Throughput"){print}}' | tee throughput.data
#	cat $DATAFILE_CONTAINING_RESULTS | awk 'BEGIN{FS="\t";}{if($2==2 && $3=="Throughput2"){print}}' | tee throughput2.data

##aggregate each 10 lines by computing average of it
##ATTENTION: the 10 corresponds to the number of rounds per experiment, if the number of roudn is modified, this script has to be modifed as well!
#for i in cpu_only.data gpu_only.data response_time.data simple_round_robin.data waiting_time_aware_response_time.data throughput.data throughput2.data; do
#	cat "$i" | awk '
#BEGIN{FS="\t";
#sum_execution_time=0;
#sum_training_time=0;
#number_of_rounds=5;
#} 

#{sum_execution_time+=$4; 
#sum_training_time+=$5; 

#if(NR%number_of_rounds==0){
#exec_time_avg=sum_execution_time/number_of_rounds;
#training_time_avg=sum_training_time/number_of_rounds;

#print $0"\t"exec_time_avg"\t"training_time_avg
#sum_execution_time=0;
#sum_training_time=0;
#next
#}}' > averaged_$i
#done


#	#cp varying_number_and_relative_speed_of_processing_devices_evaluation_sorted.data results.csv
#	
#cat averaged_cpu_only.data | awk '{print $8}' > avg_cpu_execution_times.tmp
#paste averaged_waiting_time_aware_response_time.data avg_cpu_execution_times.tmp > wtar_results.data
#rm avg_cpu_execution_times.tmp

#cat wtar_results.data | awk '{if(NR%8==0){print; print ""}else{print}}' > tmp
#mv tmp wtar_results.data 
#gnuplot -p speedup.gnuplot


#	#bash -x prepareGnuplotCSV.sh

#cd ..
