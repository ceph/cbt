# tools/serialise_benchmark.py -- Automatic Unit Test Generation

## Description:

This is a standalone tool to generate unit tests for CBT. 

The execution of the script produces as output:

1. a new baseline tools/baseline.json, this is a serialisation of each of the Benchmark class instances,
2. a new set of test/test_bm{benchmark}.py, each consisting on a set of sanity unit tests.

## Requirements:

The Python modules pytest and pytest-xdist should be installed on the machine that will run the tool, this can be the same as the one that drives CBT.

## Usage:

The following is an example of the execution of the script:

```bash
# python3 tools/serialise_benchmark.py
```
An example of the expected normal ouput is shown below.

![cbt_utests_gen](cbt_utest_gen.png)

This would have created (or updated if existing already) the set of unit tests for the supported benchmarks.

## Execution of unit tests:

The unit tests can be executed from the command line as follows:

```bash
# python3 -m pytest -p no:cacheprovider tests/
```
An example output showing a successful execution:

![cbt_utests_gen](cbt_utest_gen.png)

Note: the tests skipped above require an environment variable to be defined to identify the target nodes 
for exercising pdsh.

The following is an example to execute the pdsh tests:

```bash
# export CBT_TEST_NODES=root@ceph2,root@ceph4
# export PDSH_SSH_ARGS_APPEND="-p 8023 -o StrictHostKeyChecking=no -v -E /tmp/ssh.out"
```

## Generation of Unit tests

The main idea is the concept of **referencial transparency**, (see for example [ref_transparency](https://stackoverflow.com/questions/210835/what-is-referential-transparency)). Basically, in the functional programming
paradigm, it means that given a function and an input value, you will always receive the same output. The test
generator takes advantage of this since the constructors of the Benchmark classes should always produce instances
with the same initial state. The class Benchmark in CBT expects as an argument an object from a .yaml file (the test plan, which includes a Cluster type object). If we ensure to provide a fixed minimal cluster object to the
constructor of the Benchmark class, we can have an _invariant_ that we can use to test that each of the attributes
of the Benchmark classes have the same value across runs.

In other words, each class constructor of the CBT Benchmark class behaves like a function and always produces
object instances initialised with the same values, provided the same fixed cluster instance as argument.


* For each Benchmark class supported, the tool constructs a serialisation of the object instance, and saves them
in the tools/baseline.json.
* To prevent tampering, an md5sum of the contents of the .json file is calculated.
* For each Benchmark class suppported, the tool uses a boilerplate code template to produce unit tests. Each unit test verifies that a supported attribute of the benchmark class is initialised as recorded by the baseline.json.
* When executed, the unit tests perform a sanity check to ensure that the baseline.json has not changed since the creation of the unit tests, if so proceeds to verify each attribute of each Benchmark class. This is useful to detect
whether some attributes has been changed, replaced or deleted. This is especially useful to detect for regressions
during code refactoring.


## Workflow recommeded


* Before starting a code refactoring effort, run the unit tests: they should all pass as shown above.
* Make the intended code change -- for example, remove a benchmark.py class module, or refine with new attributes,
or delete some existing attributes.
* Run the unit tests: some should fail accordingly to indicate the missing attributes that existed in the past but no longer in the current benchmark class module. 
* Run the tool serialise_benchmark.py. This will regenerate the baseline.json and the unit tests.
* Run the unit tests: they should now all pass.
* Iterate if required.
