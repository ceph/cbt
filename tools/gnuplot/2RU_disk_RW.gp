# expects to be called with gnuplot -e "data=filename;out_file=picture.png "
# the title should be passed in as well
set terminal pngcairo size 1920,1200 enhanced font 'Verdana,12'
set output out_file
set key autotitle columnhead
set xdata time
set timefmt "%s"
set format x "%H:%M:%S"
set ylabel "KiloBytes"
set xtics rotate
# This is an alternative form of the plot command where you can just give the column header
# super nice, but not really able to manipulate the data like we do in the actual plot command
# plot data using "UTC":"[NET:public]RxKB", data using "UTC":"[NET:public]TxKB"


set grid ytics

set multiplot layout 4,3 title "Read / Write KiB" font 'Verdana,12'

set title "sda"
plot data using "UTC":"[DSK:sda]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sda]WKBytes" title columnheader with lines smooth bezier

set title "sdb"
plot data using "UTC":"[DSK:sdb]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdb]WKBytes" title columnheader with lines smooth bezier

set title "sdc"
plot data using "UTC":"[DSK:sdc]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdc]WKBytes" title columnheader with lines smooth bezier

set title "sdd"
plot data using "UTC":"[DSK:sdd]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdc]WKBytes" title columnheader with lines smooth bezier

set title "sde"
plot data using "UTC":"[DSK:sde]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sde]WKBytes" title columnheader with lines smooth bezier

set title "sdf"
plot data using "UTC":"[DSK:sdf]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdf]WKBytes" title columnheader with lines smooth bezier

set title "sdg"
plot data using "UTC":"[DSK:sdg]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdg]WKBytes" title columnheader with lines smooth bezier

set title "sdh"
plot data using "UTC":"[DSK:sdh]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdh]WKBytes" title columnheader with lines smooth bezier

set title "sdi"
plot data using "UTC":"[DSK:sdi]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdi]WKBytes" title columnheader with lines smooth bezier

set title "sdj"
plot data using "UTC":"[DSK:sdj]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdj]WKBytes" title columnheader with lines smooth bezier

set title "sdk"
plot data using "UTC":"[DSK:sdk]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdk]WKBytes" title columnheader with lines smooth bezier

set title "sdl"
plot data using "UTC":"[DSK:sdl]RKBytes" title columnheader with lines smooth bezier, data using "UTC":"[DSK:sdl]WKBytes" title columnheader with lines smooth bezier
unset multiplot



