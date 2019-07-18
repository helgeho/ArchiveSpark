[< Table of Contents](README.md) | [ArchiveSpark Operations >](Operations.md)
:---|---:

# Configuration

ArchiveSpark provides two ways of configurations:

1. Parameters that are relevant for the driver, which is in charge of running a job and distributing your code to the executors, can be set directly on the global `ArchiveSpark` object.
2. Parameters that are relevant for each executor can be set through a distributed config, which can be accessed with `ArchiveSpark.conf`.

## Driver Options

Option| Description
:--------|:---
**parallelism** | Sets the extent of parallelism / number of partitions that are used by most distributed ArchiveSpark operations.  If this is not set, Spark's `defaultParallelism` will be used by ArchiveSpark instead, which can be set through the `spark.default.parallelism` property.
&nbsp; | *Example:* `ArchiveSpark.parallelism = 1000`

## Executor Options

Option| Description
:--------|:---
**catchExceptions** | Defines whether or not exceptions that are thrown when executing an Enrichment Function should be catched by ArchiveSpark. This is `true` by default. In this case, exceptions are sliently catched and accessible through `rdd.lastException` / `rdd.printLastException()` or filtered by `rdd.filterNoException()` (s. [Dataset Operations](Operations.md)). Otherwise, exceptions would cause the job to fail.
&nbsp; | *Example:* `ArchiveSpark.conf.catchExceptions = false`
**maxWarcDecompressionSize** | Limits the number of bytes to be read when extracting a (W)ARC.gz record. This can help to prevent failures due to out-of-memory exceptions. By defaults it is set to 0, which means there is no limit.  
&nbsp; | *Example:* `ArchiveSpark.conf.maxWarcDecompressionSize = 1024 * 1024 // 1 MB`

[< Table of Contents](README.md) | [ArchiveSpark Operations >](Operations.md)
:---|---: