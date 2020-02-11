#set title 'Execution time curves for Operation SORT'
set xtics nomirror
set ytics nomirror
#set xlabel 'Data size in number of Elements (int)'
set ylabel 'Average Execution Time in ns' offset 0,-2
#set key top left Left reverse samplen 1

set key top right Left reverse samplen 1
set key below
set key box



set datafile separator "\t"

set xlabel 'MAX_DATASET_SIZE_IN_MB' 

plot 'averaged_cpu_only.data' using 1:10 title "CPU_Only" with points, \
'averaged_gpu_only.data' using 1:10 title "GPU_Only" with points, \
'averaged_response_time.data' using 1:10 title "Hybrid: Response Time" with points, \
'averaged_waiting_time_aware_response_time.data' using 1:10 title "Hybrid: Waiting Time Aware Response Time" with points, \
'averaged_simple_round_robin.data' using 1:10 title "Hybrid: Simple Round Robin" with points, \
'averaged_throughput.data' using 1:10 title "Hybrid: Throughput" with points, \
'averaged_throughput2.data' using 1:10 title "Hybrid: Throughput2" with points    

set output "execution_times_MAX_DATASET_SIZE_IN_MB.pdf" 
set terminal pdfcairo font "Helvetica,9" size 5, 4
replot

set xlabel 'NUMBER_OF_DATASETS'

plot 'averaged_cpu_only.data' using 2:10 title "CPU_Only" with points, \
'averaged_gpu_only.data' using 2:10 title "GPU_Only" with points, \
'averaged_response_time.data' using 2:10 title "Hybrid: Response Time" with points, \
'averaged_waiting_time_aware_response_time.data' using 2:10 title "Hybrid: Waiting Time Aware Response Time" with points, \
'averaged_simple_round_robin.data' using 2:10 title "Hybrid: Simple Round Robin" with points, \
'averaged_throughput.data' using 2:10 title "Hybrid: Throughput" with points, \
'averaged_throughput2.data' using 2:10 title "Hybrid: Throughput2" with points   

set output "execution_times_NUMBER_OF_DATASETS.pdf"
set terminal pdfcairo font "Helvetica,9"
replot

set xlabel 'NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD'

plot 'averaged_cpu_only.data' using 3:10 title "CPU_Only" with points, \
'averaged_gpu_only.data' using 3:10 title "GPU_Only" with points, \
'averaged_response_time.data' using 3:10 title "Hybrid: Response Time" with points, \
'averaged_waiting_time_aware_response_time.data' using 3:10 title "Hybrid: Waiting Time Aware Response Time" with points, \
'averaged_simple_round_robin.data' using 3:10 title "Hybrid: Simple Round Robin" with points, \
'averaged_throughput.data' using 3:10 title "Hybrid: Throughput" with points, \
'averaged_throughput2.data' using 3:10 title "Hybrid: Throughput2" with points   

set output "execution_times_NUMBER_OF_SORT_OPERATIONS_IN_WORKLOAD.pdf"
set terminal pdfcairo font "Helvetica,9"
replot




