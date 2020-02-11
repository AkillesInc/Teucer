#set title 'Execution time curves for Operation SORT'
set xtics nomirror
set ytics nomirror
#set xlabel 'Data size in number of Elements (int)'
set ylabel 'Average Execution Time in ns' offset 0,-2
#set key top left Left reverse samplen 1

#set logscale y


set key top right Left reverse samplen 1
set key below
set key box



set datafile separator "\t"

set xlabel 'MAX_DATASET_SIZE_IN_MB' 

plot 'averaged_cpu_only.data' using 1:24 title "CPU_Only" w lp lw 4, \
'averaged_gpu_only.data' using 1:24 title "GPU_Only" w lp lw 4, \
'averaged_response_time.data' using 1:24 title "Hybrid: Response Time" w lp lw 4, \
'averaged_waiting_time_aware_response_time.data' using 1:24 title "Hybrid: Waiting Time Aware Response Time" w lp lw 4, \
'averaged_simple_round_robin.data' using 1:24 title "Hybrid: Simple Round Robin" w lp lw 4, \
'averaged_throughput.data' using 1:24 title "Hybrid: Throughput" w lp lw 4, \
'averaged_throughput2.data' using 1:24 title "Hybrid: Throughput2" w lp lw 4    

set output "execution_times_MAX_DATASET_SIZE_IN_MB.pdf" 
set terminal pdfcairo font "Helvetica,9" size 5, 4
replot




