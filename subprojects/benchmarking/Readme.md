### ArchiveSpark Benchmarking

In order to run the benchmarks of ArchiveSpark against HBase and pure Spark, both using the WarcBase library, please compile WarcBase yourself:

[`https://github.com/lintool/warcbase`](https://github.com/lintool/warcbase)

and put the resulting jar file into a `lib` folder under this subproject (`subprojects/benchmarking`) before compiling it with:

1. `sbt assemblyPackageDependency`
2. `sbt assembly`
