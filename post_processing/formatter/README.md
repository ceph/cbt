# Formatter

The formatter converts CBT output json files into the correct format for the rest of the post processing. It is
a json file of the format:

```
{
    <queue_depth>: {
                    bandwidth_bytes: <value>
                    blocksize: <value>
                    io_bytes: <value>
                    iops: <value>
                    latency: <value>
                    number_of_jobs: <value>
                    percentage_reads: <value>
                    percentage_writes: <value>
                    runtime_seconds: <value>
                    std_deviation: <value>
                    total_ios: <value>
    }
    ...
    <queue_depth_n> {

    }
    maximum_bandwidth: <value>
    latency_at_max_bandwidth: <value>
    maximum_iops: <value>
    latency_at_max_iops: <value>
}
```
A single file will be produced per block size used for the benchmark run.

## Standalone script
A wrapper script has been provided for the formatter
```
fio_common_output_wrapper.py --archive=<archive_directory>
                             --results_file_root=<file_root>
```
where
- `--archive` Required. the archive directory given to CBT for the benchmark run.
- `--results_file_root` Optional. the name of the results file to process, without the extension. This defaults to `json_output`,
which is the default for CBT runs, if not specified

Full help text is provided by using `--help` with the script

## Output
A directory called `visualisation` will be created in the directory specified by `--archive` that contains all the processed files.
There will be one file per blocksize used for the benchmark run.

## Example

```bash
PYTHONPATH=/cbt /cbt/tools/fio_common_output_wrapper.py --archive="/tmp/ch_cbt_run" --results_file_root="ch_json_result"
```