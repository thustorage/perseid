# Perseid

This repository contains the artifact for the USENIX ATC'23 paper: "[Revisiting Secondary Indexing in LSM-based Storage Systems with Persistent Memory](https://www.usenix.org/conference/atc23/presentation/wangjing)".

For USENIX ATC'23 Artifact Evaluation, please check out the `Comments for AEC` from the atc23ae hotcrp system.

## Repository contents

This prototype is built based on [PebblesDB](https://github.com/utsaslab/pebblesdb).

* `src/include/pebblesdb/db.h`: DB interface (with secondary indexing)
* `src/db/`: implementation of PebblesDB, Perseid, and LSM-based secondary index
* `src/db/sec_idx/`: PM-based secondary indexes
* `src/db/sec_idx/[clht & CCEH]`: the hybrid PM-DRAM validation
* `src/db/sec_idx_bench.cc`: benchmark for secondary indexes
* `scripts`: main evaluation scripts


## Dependencies

* libraries
  - Dependencies required by [PebblesDB](https://github.com/utsaslab/pebblesdb)
  - [PMDK](https://github.com/pmem/pmdk) (libpmem & libpmemobj)
  - google gflags

* utilities for experiments:
  - numactl


## System configuration for PM

```shell
1. set Optane DCPMM to AppDirect mode
$ sudo ipmctl create -f -goal persistentmemorytype=appdirect

2. configure PM device to fsdax mode
$ sudo ndctl create-namespace -m fsdax

3. create and mount a file system with DAX
$ sudo mkfs.ext4 -f /dev/pmem0
$ sudo mount -o dax /dev/pmem0 /mnt/pmem0
```


## Building the benchmarks

**We recommend that you use the scripts from the [scripts](scripts) directly. The scripts will build and run experiments automatically.**

Quick start:

```shell
$ mkdir -p build && cd build
$ cmake -DCMAKE_BUILD_TYPE=Release .. 
$ make sec_idx_bench -j
```
The [benchmark](src/db/sec_idx_bench.cc) receives arguments with gflags, examples:
```bash
./sec_idx_bench --db=path_to_db --preload=path_to_preload --trace=path_to_trace
```

To facilitate evaluation, we use cmake to set most configurations for systems. Please check out the [sec_idx_config.h.in](src/db/sec_idx/sec_idx_config.h.in) and the [CMakeLists.txt](CMakeLists.txt) for more details.


## Running experiments

```
scripts
|---- kick_the_tires.sh     # script of "Hello world"-sized examples
|---- eval_fig*.sh          # script for each figure in the paper
|---- sub/eval_fig*.sh      # scripts for subfigure (if you want to evaluate them separately)
```

### For USENIX ATC'23 artifact evaluation kick-the-tires
Run `kick_the_tires.sh` and this script will build and run several systems. You
should see `Kick-the-tires passed!` at the end if all procedures succeeded.
```
cd scripts
./kick_the_tires.sh
```

### Usage

To run experiments for Perseid:
```bash
# cd scripts  # in the scripts directory
./eval_fig*.sh
```

If you want to evaluate a subfigure separately, use the scripts in `sub` directory but stay in the `scripts` directory:
```bash
./sub/eval_fig*.sh
```

If you want to evaluate other systems, you can use the following commands:
```bash
./eval_fig*.sh others   # (just type 'others') for other systems (LSMSI, composite indexes, etc)
# or
./sub/eval_fig*.sh others
```


Results are shown in screen and saved in `results` directory with name like `fig*_sysname.txt`.
Results are printed per 1 Million ops by default, and final results are printed after each experiment.

Each line contains several numbers:
- Number of operations (in Million)
- avg latency of all operations
- avg latency of PUT
- avg latency of GET by primary key
- avg latency of GET by secondary key
- avg latency of lookup LSM primary table in one secondary GET
- avg latency of validation
- avg number of query results of one secondary GET
- avg number of primary table components probed in one secondary GET

You can use `grep_result.sh` to grep final results of latency and throughput:
```bash
./grep_result.sh path_to_result
```

**Note**:
- These scripts should be run inside the `scripts` directory.
- `eval_fig7.sh` will also get results of fig6, so you can skip `eval_fig6.sh` to save time.

###  Estimated time:
- fig6 (not recommend): 1 hour for Perseid (6 results) and 3 hour for others (18 results)
- fig7: 1 hour for Perseid (12 results) and 3.8 hours for others (36 results)
- fig8: 35 minutes for Perseid (3 results) and 1.7 hours for others (6 results)
- fig9: 4.3 hours for Perseid (12 results) and 7 hours for others (24 results)
- fig10: 15 minutes for Perseid (10 results) and 2 hours for others (20 results)
- fig11: 33 minutes for Perseid (3 results) and 2.6 hours for others (6 results)
