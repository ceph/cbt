# expects to be called with gnuplot -e "data=filename"
# the title should be passed in as well
set terminal pngcairo size 1280,720 enhanced font 'Verdana,12'
set output 'client_public_network.png'
set title 'Client Public NIC'
set key autotitle columnhead
set xdata time
set timefmt "%s"
set format x "%H:%M:%S"
# This is an alternative form of the plot command where you can just give the column header
# super nice, but not really able to manipulate the data like we do in the actual plot command
# plot data using "UTC":"[NET:public]RxKB", data using "UTC":"[NET:public]TxKB"

# setting the range to be in Gbps
set yrange[0:1e10]

# Setting y scale to be human readable
set format y "%.0s%cbps"

set grid ytics

# 157 and 158 are the RxKB and TxKB for the public NIC
# sadly we have to hard code the value so we can operate on the data in the using statement
# the data is presented in KB/s, to translate to Gbps we multiply by 8192
plot data using 1:($157*8192.0) title columnheader with lines smooth bezier, data using 1:($158*8192.0) title columnheader with lines smooth bezier



