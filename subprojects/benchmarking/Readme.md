### ArchiveSpark Benchmarking

In order to run the benchmarks of ArchiveSpark against HBase and pure Spark, both using the WarcBase library, please compile WarcBase yourself:

[`https://github.com/lintool/warcbase`](https://github.com/lintool/warcbase)

and put the resulting jar file into a `lib` folder under this subproject (`subprojects/benchmarking`) before compiling it with:

1. `sbt assemblyPackageDependency`
2. `sbt assembly`

**Note:** WarcBase has evolved quite a bit since we ran these benchmarks. So to get the code working, please use the WarcBase version from back then:
https://github.com/lintool/warcbase/tree/1b6d83c4a7feb19258e61890060b27cedcec2ebb