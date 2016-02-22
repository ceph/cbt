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
set yrange [0:20]

set multiplot layout 4,3 title "Average msec before a request is serviced by drive" font 'Verdana,12'

set title "sda"
plot data using "UTC":"[DSK:sda]SvcTim" title columnheader with lines smooth bezier

set title "sdb"
plot data using "UTC":"[DSK:sdb]SvcTim" title columnheader with lines smooth bezier

set title "sdc"
plot data using "UTC":"[DSK:sdc]SvcTim" title columnheader with lines smooth bezier

set title "sdd"
plot data using "UTC":"[DSK:sdd]SvcTim" title columnheader with lines smooth bezier

set title "sde"
plot data using "UTC":"[DSK:sde]SvcTim" title columnheader with lines smooth bezier

set title "sdf"
plot data using "UTC":"[DSK:sdf]SvcTim" title columnheader with lines smooth bezier

set title "sdg"
plot data using "UTC":"[DSK:sdg]SvcTim" title columnheader with lines smooth bezier

set title "sdh"
plot data using "UTC":"[DSK:sdh]SvcTim" title columnheader with lines smooth bezier

set title "sdi"
plot data using "UTC":"[DSK:sdi]SvcTim" title columnheader with lines smooth bezier

set title "sdj"
plot data using "UTC":"[DSK:sdj]SvcTim" title columnheader with lines smooth bezier

set title "sdk"
plot data using "UTC":"[DSK:sdk]SvcTim" title columnheader with lines smooth bezier

set title "sdl"
plot data using "UTC":"[DSK:sdl]SvcTim" title columnheader with lines smooth bezier
unset multiplot



