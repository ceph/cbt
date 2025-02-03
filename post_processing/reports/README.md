# Reports

Produces a report in github markdown, and optionally pdf format that includes a summary table and the relevant
plots from the CBT run.

## Output
A report in github markdown format with a plots directory containing the required plots. The report and plots directory
can be uploaded directly to github as-is and the links will be maintained.

Optionally a report in pdf format can also be created.

Due to the tools used there are only 6 unique colours available for the plot lines, so it is recommended to limit the
comparison to 6 or less files or directories. During testing we found that more than four directories can start rendering
the pdf report unreadable, so it is not recommended to create a pdf report to compare data from more than four 
benchmark runs.

## Standalone scripts
There are actually 2 scripts provided as wrappers for the report generation:
* generate_performance_report.py
* generate_comparison_performance_report.py

### generate_performance_report
Creates a performance report for a single benchmark run. The results must first have had the formatter run on them.

```
generate_performance_report.py  --archive=<full_path_to_results_directory>
                                --output_directory=<full_path_to_directory_to_store_report>
                                --create_pdf
```

where:
- `--archive` Required. The archive directory containing the files from the formatter
- `--output_directory` Required. The directory to store the markdown report file and relevant plots.
- `--create_pdf` Optional. Create a pdf report

Full help text is provided by using `--help` with the scripts

#### Example
```bash
PYTHONPATH=/cbt /cbt/tools/generate_performance_report.py --archive="/tmp/ch_cbt_main_run" --output_directory="/tmp/reports/main" --create_pdf
```

### generate_comparison_performance_report.py
Creates a report comparing 2 or more benchmark runs. The report will only include plots and results for formatted files
that are common in all the directories.

```
generate_comparison_performance_report.py --baseline=<full_path_to_archive_directory_to_use_as_baseline>
                                          --archives=<full_path_to_results_directories_to_compare>
                                          --output_directory=<full_path_to_directory_to_store_report>
                                          --create_pdf
```
where 
- `--baseline` Required. The full path to the baseline results for the comparison
- `--archives` Required. A comma-separated list of directories containing results to compare to the baseline
- `--output_directory` Required. The directory to store the markdown report file and relevant plots.
- `--create_pdf` Optional. Create a pdf report

#### Examples
```bash
PYTHONPATH=/cbt /cbt/tools/generate_comparison_performance_report.py --baseline="/tmp/ch_cbt_main_run" --archives="/tmp/ch_sandbox/" --output_directory="/tmp/reports/main" --create_pdf
```