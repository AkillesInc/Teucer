#set title 'Execution time curves for Operation SORT'
set xtics nomirror
set ytics mirror
#set xlabel 'Data size in number of Elements (int)'
#set ylabel 'Speedup w.r.t. CPU' offset 0,-2
set ylabel 'Speedup w.r.t. CPU' offset 2,-1
#set key top left Left reverse samplen 1

#set logscale y


set key top left Left reverse samplen 1
#set key below
set key box

#set title '%OPERATION_NAME%: NUMBER_OF_OPERATIONS_IN_WORKLOAD: Execution Times'


set datafile separator "\t"

set xlabel 'Number of Processing Devices'
set xrange [0:21]
 

set style arrow 1 nohead filled size screen 0.025,30,45 ls 1 lw 4 linecolor rgb "gray50"
#set arrow from graph(0,0),0.5 to graph(0,1),0.5 as 1
set arrow from 0,1 to 21,1 as 1

#set arrow from graph(0,0.73),0.90 to graph(0,0.83),0.90 as 1
#set label "speedup=1" at graph(0,0.85),0.90
set arrow from graph(0,0.43),0.90 to graph(0,0.53),0.90 as 1
set label "speedup=1" at graph(0,0.55),0.90

#plot 'averaged_cpu_only.data' using 3:24 title "CPU_Only" w lp lw 4, \
#'averaged_gpu_only.data' using 3:24 title "GPU_Only" w lp lw 4, \
#'averaged_response_time.data' using 3:24 title "Hybrid: Response Time" w lp lw 4, \
#'averaged_waiting_time_aware_response_time.data' using 3:24 title "Hybrid: Waiting Time Aware Response Time" w lp lw 4, \
#'averaged_simple_round_robin.data' using 3:24 title "Hybrid: Simple Round Robin" w lp lw 4, \
#'averaged_throughput.data' using 3:24 title "Hybrid: Throughput" w lp lw 4, \
#'averaged_throughput2.data' using 3:24 title "Hybrid: Throughput2" w lp lw 4     

#1:6:($9/$7)

plot "wtar_results_2d_plot.data" using 6:($1==0?$9/$7:1/0) title "ASF=0.0" w lp lw 4, \
"wtar_results_2d_plot.data" using 6:($1==0.1?$9/$7:1/0) title "ASF=0.1" w lp lw 4, \
"wtar_results_2d_plot.data" using 6:($1==0.2?$9/$7:1/0) title "ASF=0.2" w lp lw 4, \
"wtar_results_2d_plot.data" using 6:($1==1?$9/$7:1/0) title "ASF=1.0" w lp lw 4


set output "%OPERATION_NAME%_speedups_varying_operator_selectivity_and_number_pd.pdf" 
#set terminal pdfcairo font "Helvetica,9" size 5, 4
set terminal pdfcairo mono font "Helvetica,30" size 6.1, 4
replot





