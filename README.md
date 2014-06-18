# Spark SQL Perf

## Quick start

1. Have TPC-DS tables ready. Follow the steps in [impala-tpcds-kit](https://github.com/cloudera/impala-tpcds-kit) to generate the data. (For local development use, set scale factor to 1.)
2. Set the location to the TPC-DS tables in `./run-bench`'s `TPCDS_DATA_DIR`.
3. Launch `./run-bench` with argument format :
```
<sparkMaster> [queries] [numIterPerQuery] [numWarmUpRuns] [dropOutlierPerc = 0.0]
``` 

## Example

This is an example benchmark:

```
./run-bench local[4] q19,q34,ss_max 10 2 0.2
```

A typical report near the end of the above job could be:

```
******** benchmark config
Number of warm-up runs (before all queries, not each): 2
Number of iterations per query: 10
Outlier drop ratio: 0.2

******** benchmark results
q19 runtime (millis): 10926
q34 runtime (millis): 9585
ss_max runtime (millis): 5460
```
