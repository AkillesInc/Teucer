# change columns in splot command to plot speedup with/without GPU transfers
reset
#set terminal aqua
#set termoption dash
set view 55,340,1,1                                       

#set datafile separator ","

set palette gray                                          
#set hidden3d
#set pm3d

set key font "Helvetica, 30"
#set logscale x
#set logscale y 2
set logscale z
set logscale cb
set xlabel "operator selectivity" offset 1,-1.5 font "Helvetica, 30"
set ylabel "#PD" font "Helvetica, 30"
set zlabel "speedup w.r.t. CPU" offset 10,5 font "Helvetica, 30"

set cbrange [1:35]
set zrange [1:35]

#set xtics offset -0.5,-0.5 font "Helvetica, 30"
set xtics font "Helvetica, 30"
set ytics 5 offset -0.5,-0.25 font "Helvetica, 30"
set ztics 10 font "Helvetica, 30"
set cbtics offset -0.5,0 font "Helvetica,30"


#set contour
set style line 1 lt 1 linecolor rgb "#000000"
set style line 2 lt 2 linecolor rgb "#000000"
set style line 3 lt 3 linecolor rgb "#000000"
set style line 4 lt 4 linecolor rgb "#000000"
set style line 5 lt 5 linecolor rgb "#000000"
set style line 6 lt 6 linecolor rgb "#000000"
set style line 7 lt 7 linecolor rgb "#000000"
set style line 8 lt 8 linecolor rgb "#000000"
#set cntrparam levels discrete 1,2,3,4,5,8
set style increment user

# without data transfers
#splot  "sort-results.csv" using 1:2:10 notitle with pm3d nohidden3d nocontour, \
#	"sort-results.csv" using 1:2:10 title "speedup without transfers" with lines linecolor rgb "#3f3f3f" nohidden3d

# with data transfers
#splot  "sort-results.csv" using 1:2:11 notitle with pm3d nohidden3d nocontour, \
#	"sort-results.csv" using 1:2:11 title "speedup with transfers" with lines linecolor rgb "#3f3f3f" nohidden3d

# with model decisions
splot  "wtar_results.data" using 1:6:($9/$7) notitle with pm3d nohidden3d nocontour, \
	"wtar_results.data" using 1:6:($9/$7) notitle with lines linecolor rgb "#3f3f3f" nohidden3d

set output "SIMULATION_speedups_for_all_experiments.pdf" 
set terminal pdfcairo mono font "Helvetica,30" size 6.1, 4
replot


#splot  "results.csv" using 1:6:7 notitle with pm3d nohidden3d nocontour, \
#	"results.csv" using 1:6:7 title "speedup with transfers" with lines linecolor rgb "#3f3f3f" nohidden3d

