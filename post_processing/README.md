# Post Processing of CBT results

## Description
A set of tools that can be used to post process the data from any run of CBT. It provides a report in github markdown,
and optionally pdf, format that contains a set of hockey-stick curves generated from the CBT run.
The tool set consists of three separate tools that can be run stand-alone. The eventual aim is to integrate the post
processing into CBT once more benchmark types are supported.

There are three components to the post processing which are:

* [formatter](formatter/README.md)
* [plotter](plotter/README.md)
* [reports](reports/README.md)


## Suppoted benchmark tools
This list will be added to as extra benchmark tools are supported.
* fio

## Dependencies
These post processing changes include some new dependencies to be run correctly

### python dependencies
The following python modules are dependencies for this work:
* matplotlib
* mdutils

Both have been added to the requirements.txt file in the CBT project.

### Dependencies for pdf report generation
To generate a report in pdf format there are 2 additional requirements

A working install of tex is required on the base operating system, which can be installed using the package manager.
For Red Hat based OSes this can be achieved by running `yum install texlive`

[Pandoc](https://pandoc.org/), which can be installed on most Linux distributions using the included package manager.
For Red Hat based OSes use `yum install pandoc`

The minimum pandoc level tested is `2.14.0.3` which is available for RHEL 9
