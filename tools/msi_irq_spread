#! /usr/bin/python
import subprocess

# set the smp affinity via hexadecimal mask
# does not properly handle systems with more than 32 cores
# does not properly handle dual-socket systems, NUMA systems

# look for MSI-X interrupts
# save the IRQ number
irq_num_array=[]
with open('/proc/interrupts','r') as f:
    for line in f:
        if 'PCI-MSI-edge' in line:
            irq_num_array.append(line.split()[0].strip(':'))
        else:
            pass

#get the number of cpus, hyperthreading and all
cpu_count = 0
with open('/proc/cpuinfo','r') as f:
    for line in f:
        if 'processor' in line:
            cpu_count = cpu_count + 1
        else:
            pass

irq_path = '/proc/irq/{0}/smp_affinity'
for i in range(0,len(irq_num_array)):
    # formats number as hexadecimal 
    core_mask = format(1 << (i % cpu_count),'x')
    path = irq_path.format(irq_num_array[i])

    # example call is echo 400 > /proc/irq/84/smp_affinity
    subprocess.call("echo {0} > {1} ".format(core_mask,path),shell=True)

