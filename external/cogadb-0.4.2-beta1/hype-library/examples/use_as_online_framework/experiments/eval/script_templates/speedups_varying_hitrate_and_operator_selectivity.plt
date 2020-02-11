#set title 'Execution time curves for Operation SORT'
set xtics nomirror
set ytics mirror
#set xlabel 'Data size in number of Elements (int)'
#set ylabel 'Speedup w.r.t. CPU' offset 0,-2
set ylabel 'Speedup w.r.t. CPU' offset 2,-1
#set key top left Left reverse samplen 1

set logscale y


set key top left Left reverse samplen 1
#set key below
set key box

#set title '%OPERATION_NAME%: NUMBER_OF_OPERATIONS_IN_WORKLOAD: Execution Times'


set datafile separator "\t"

set xlabel 'Average Cache Hitrate'

 

#plot 'averaged_cpu_only.data' using 3:24 title "CPU_Only" w lp lw 4, \
#'averaged_gpu_only.data' using 3:24 title "GPU_Only" w lp lw 4, \
#'averaged_response_time.data' using 3:24 title "Hybrid: Response Time" w lp lw 4, \
#'averaged_waiting_time_aware_response_time.data' using 3:24 title "Hybrid: Waiting Time Aware Response Time" w lp lw 4, \
#'averaged_simple_round_robin.data' using 3:24 title "Hybrid: Simple Round Robin" w lp lw 4, \
#'averaged_throughput.data' using 3:24 title "Hybrid: Throughput" w lp lw 4, \
#'averaged_throughput2.data' using 3:24 title "Hybrid: Throughput2" w lp lw 4     

plot "wtar_results_2d_plot.data" using 1:($7==0?$10/$8:1/0) title "ASF=0" w lp lw 4, \
"wtar_results_2d_plot.data" using 1:($7==0.1?$10/$8:1/0) title "ASF=0.1" w lp lw 4, \
"wtar_results_2d_plot.data" using 1:($7==0.2?$10/$8:1/0) title "ASF=0.2" w lp lw 4, \
"wtar_results_2d_plot.data" using 1:($7==0.5?$10/$8:1/0) title "ASF=0.5" w lp lw 4, \
"wtar_results_2d_plot.data" using 1:($7==1.0?$10/$8:1/0) title "ASF=1.0" w lp lw 4


set output "%OPERATION_NAME%_execution_times_NUMBER_OF_OPERATIONS_IN_WORKLOAD.pdf" 
#set terminal pdfcairo font "Helvetica,9" size 5, 4
set terminal pdfcairo mono font "Helvetica,30" size 6.1, 4
replot





