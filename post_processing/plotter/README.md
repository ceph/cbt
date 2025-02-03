# Plotter
Draws the hockey stick plots for a benchmark run from the data produced by the formatter. These are png files, with one
plot produced per block size used.

There is also a python class that will produce comparison plots of two or more different CBT runs for one or more block 
sizes.
Due to the tools used there are only 6 unique colours available for the plot lines, so it is recommended to limit the
comparison to 6 or less files or directories.

## Standalone script
A wrapper script is only provided to produce comparison plots.
```
plot_comparison.py  --files=<comma_separated_list_of_files_to_compare>
                    --directories=<comma_separated_list_of_directories_to_compare>
                    --output_directory=<full_path_to_directory_to_store_plot>
                    --labels="<comma_separated_list_of_labels>
```
where
- `--output_directory` Required. The full path to a directory to store the plots. Will be created if it doesn't exist
- `--files` Optional. A comma separated list of files to plot on a single axis
- `--directories` Optional. A comma separated list of directories to plot. A single plot will be produced per blocksize
- `--labels` Optional. Comma separated list of labels to use for the lines on the comparison plot, in the same order as 
--file or --directories.

One of `--files` or `--directories` must be provided.

Full help text is provided by using `--help` with the script

## Example

```bash
PYTHONPATH=/cbt /cbt/tools/plot_comparison.py --directories="/tmp/ch_cbt_main_run,/tmp/ch_cbt_sandbox_run" --output_directory="/tmp/main_sb_comparisons"
```