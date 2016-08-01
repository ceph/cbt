import monitor_perf
import monitor_blktrace
import monitor_collectl
import monitoring

def factory(monitor_list_csv, run_dir):
    monitor_list = []
    if monitor_list_csv == '' or monitor_list_csv == None:
        return monitor_list
    monitor_names = [ m.strip() for m in monitor_list_csv.strip().split(',') ]
    for nm in monitor_names:
        if nm == 'perf':
            next_m = monitor_perf.monitor_perf(run_dir)
        elif nm == 'blktrace':
            next_m = monitor_blktrace.monitor_blktrace(run_dir)
        elif nm == 'collectl':
            next_m = monitor_collectl.monitor_collectl(run_dir)
        else:
            raise monitoring.MonitorException('unrecognized CBT monitor name %s' % nm)
        monitor_list.append(next_m)
    return monitor_list


