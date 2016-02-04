# expects to be called with gnuplot -e "data=filename"
# the title should be passed in as well
set terminal pngcairo size 1920,1080 enhanced font 'Verdana,12'
set output 'data_node_tab.png'
set title 'Summary'
set key autotitle columnhead
set xdata time
set timefmt "%s"
set format x "%H:%M:%S"
# This is an alternative form of the plot command where you can just give the column header
# super nice, but not really able to manipulate the data like we do in the actual plot command
# plot data using "UTC":"[NET:public]RxKB", data using "UTC":"[NET:public]TxKB"


# Setting y scale to be human readable
#set format y "%.0s%cbps"

set grid ytics

# 157 and 158 are the RxKB and TxKB for the public NIC
# sadly we have to hard code the value so we can operate on the data in the using statement
# the data is presented in KB/s, to translate to Gbps we multiply by 8192

set multiplot layout 4,1 title "Summary" font 'Verdana,12'

set title 'CPU Stats'
set yrange[0:100]
plot data using 'UTC':'[CPU]User%' title columnheader with lines smooth bezier, \
data using 'UTC':'[CPU]Sys%' title columnheader with lines smooth bezier, \
data using 'UTC':'[CPU]Wait%' title columnheader with lines smooth bezier, \
data using 'UTC':'[CPU]Idle%' title columnheader with lines smooth bezier, \
data using 'UTC':'[CPU]Totl%' title columnheader with lines smooth bezier
set yrange[*:*]

set title 'Disk IO KB/s'
plot data using 'UTC':'[DSK]ReadKBTot' title columnheader with lines smooth bezier, \
data using 'UTC':'[DSK]WriteKBTot' title columnheader with lines smooth bezier, \

set title 'Disk Operations'
plot data using 'UTC':'[DSK]OpsTot' title columnheader with lines smooth bezier

set title 'Network'
# setting the range to be in Gbps
set yrange[0:1e10]

# Setting y scale to be human readable
set format y "%.0s%cbps"

plot data using 1:($24*8192.0) title columnheader with lines smooth bezier, \
data using 1:($23*8192.0) title columnheader with lines smooth bezier, \

unset multiplot



