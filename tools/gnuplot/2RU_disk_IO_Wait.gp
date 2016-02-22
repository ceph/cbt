# expects to be called with gnuplot -e "data=filename;out_file=picture.png "
# the title should be passed in as well
set terminal pngcairo size 1920,1200 enhanced font 'Verdana,12'
set output out_file
set key autotitle columnhead
set xdata time
set timefmt "%s"
set format x "%H:%M:%S"
set ylabel "Average msec"
set xtics rotate
# This is an alternative form of the plot command where you can just give the column header
# super nice, but not really able to manipulate the data like we do in the actual plot command
# plot data using "UTC":"[NET:public]RxKB", data using "UTC":"[NET:public]TxKB"


set grid ytics

set multiplot layout 4,3 title "Average msec a Request has been waiting in the Queue" font 'Verdana,12'

set title "sda"
plot data using "UTC":"[DSK:sda]Wait" title columnheader with lines smooth bezier

set title "sdb"
plot data using "UTC":"[DSK:sdb]Wait" title columnheader with lines smooth bezier

set title "sdc"
plot data using "UTC":"[DSK:sdc]Wait" title columnheader with lines smooth bezier

set title "sdd"
plot data using "UTC":"[DSK:sdd]Wait" title columnheader with lines smooth bezier

set title "sde"
plot data using "UTC":"[DSK:sde]Wait" title columnheader with lines smooth bezier

set title "sdf"
plot data using "UTC":"[DSK:sdf]Wait" title columnheader with lines smooth bezier

set title "sdg"
plot data using "UTC":"[DSK:sdg]Wait" title columnheader with lines smooth bezier

set title "sdh"
plot data using "UTC":"[DSK:sdh]Wait" title columnheader with lines smooth bezier

set title "sdi"
plot data using "UTC":"[DSK:sdi]Wait" title columnheader with lines smooth bezier

set title "sdj"
plot data using "UTC":"[DSK:sdj]Wait" title columnheader with lines smooth bezier

set title "sdk"
plot data using "UTC":"[DSK:sdk]Wait" title columnheader with lines smooth bezier

set title "sdl"
plot data using "UTC":"[DSK:sdl]Wait" title columnheader with lines smooth bezier
unset multiplot



