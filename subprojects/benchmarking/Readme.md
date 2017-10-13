### ArchiveSpark Benchmarking

In order to run the benchmarks of ArchiveSpark against HBase and pure Spark, both using the WarcBase library, please compile *[WarcBase](https://github.com/lintool/warcbase) / [AUT](https://github.com/archivesunleashed/aut)* (see note below) and put the resulting jar file into a `lib` folder under this subproject (`subprojects/benchmarking`) before compiling it with:

1. `sbt assemblyPackageDependency`
2. `sbt assembly`

**Note:** The benchmarking codebase is not maintained anymore and we would like to keep it in the original state that we used for the benchmarks in our [[TPDL 2016]](https://github.com/helgeho/ArchiveSpark#cite) article. As all involved tools, i.e., *ArchiveSpark*, *HBase* as well as *WarcBase*, are under active development, it is quite likely that this code does not work anymore with the newest versions of these tools. Therefore, to rerun the benchmarks, we recommend to clone this repo and adapt the affected calls to the current APIs of the target versions.

*WarcBase* has been renamed to *Archives Unleashed Toolkit (AUT)* and is now available under https://github.com/archivesunleashed/aut.
Our benchmark codes use the version from the following commit: https://github.com/lintool/warcbase/tree/1b6d83c4a7feb19258e61890060b27cedcec2ebb