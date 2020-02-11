#set title 'developement of hitrate over time' font "Helvetica,14"
set notitle

set xtics nomirror
set ytics nomirror
#set logscale x
#set logscale y
set ylabel 'execution time in ns'
#font "Helvetica,14"
set xlabel 'number of elements'
#set yrange [0.001:4]
#set xrange [500:1100000]



#font "Helvetica,14"
#set key left top reverse samplen 3
#set key left top reverse samplen 1
set key left top samplen 1




#SORTING USE CASE
#plot 'quicksort.data' using 1:2 with lines lw 4 title "CPU Quicksort",'mergesort.data' using 1:2 with lines lw 4 title "CPU Mergesort",'gpu_radixsort.data' using 1:2 with lines lw 4 title "GPU Radixsort",'decisions_sorted.data' using 1:2 with lines lw 4 title "Modellentscheidung"

#set style fill transparent pattern 4 border
#set style function filledcurves y1=0
#set clip two

#MERGING USE CASE
#plot 'cpu_algorithm.data' using 1:2 with lines lw 4 title "CPU Algorithm",'gpu_algorithm.data' using 1:2 with lines lw 4 title "GPU Algorithm"
#plot 'cpu_algorithm.data' using 1:2 with points title "CPU Algorithm estimated",'cpu_algorithm.data' using 1:3 with points title "CPU Algorithm measured",'gpu_algorithm.data' using 1:2 with points title "GPU Algorithm estimated",'gpu_algorithm.data' using 1:3 with points title "GPU Algorithm measured"
plot 'cpu_algorithm.data' using 1:3 with points title "CPU Algorithm measured",'cpu_algorithm.data' using 1:2 with points title "CPU Algorithm estimated",'gpu_algorithm.data' using 1:3 with points title "GPU Algorithm measured",'gpu_algorithm.data' using 1:2 with points title "GPU Algorithm estimated"





#plot 'estimationerrors.data' using 1:5 with points title "average error"

#, f(x) title "learned polynomial:"
set output "model_decision_visualisation_black_white.png"
set terminal png
replot
set output "model_decision_visualisation_black_white.pdf"
#set terminal pdfcairo font "Helvetica,10"
set terminal pdfcairo
replot
set output "model_decision_visualisation_black_white.ps"
#set terminal pdfcairo font "Helvetica,10"
set terminal postscript
replot
set output "model_decision_visualisation_black_white.eps"
#set terminal pdfcairo font "Helvetica,10"
set terminal postscript eps
replot
