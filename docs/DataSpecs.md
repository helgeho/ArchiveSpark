[< Table of Contents](README.md) | [Enrichment Functions >](EnrichFuncs.md)
:---|---:

# Data Specifications (DataSpecs)

Data Specifications (DataSpecs) are abstractions of the load and read logic for metadata as well as data records.
Depending on your data source and type you need to select an appropriate one.
As part of core ArchiveSpark we provide DataSpecs for Web Archives (CDX/(W)ARC format) as well as some raw data types, such as plain text.
More DataSpecs for different data types and sources may be found in different projects, contributed by independent developers or ourselves (see below).

For more information on the usage of DataSpecs, please read [General Usage](General_Usage.md).

## Web Archive DataSpecs

The following DataSpecs are specific to web archive datasets from different sources. All of them are contained in `org.archive.archivespark.specific.warc.specs`, but access to all (W)ARC specs is provided through [`org.archive.archivespark.specific.warc.WarcSpec`](../src/main/scala/org/archive/archivespark/specific/warc/WarcSpec.scala).

DataSpec| Description
:-------|:---
**[CdxHdfsSpec](../src/main/scala/org/archive/archivespark/specific/warc/specs/CdxHdfsSpec.scala)**(*paths*) | Loads a collection of CDX records (meta data only) from a (distributed) filesystem, like HDFS. This is helpful to resolve *revisit records*, before loading the corresponding (W)ARC records, using `WarcHdfsCdxRddSpec`.
&nbsp; | *Example:* `val cdxRdd = ArchiveSpark.load(CdxHdfsSpec("/path/to/*.cdx.gz"))` 
**[WarcSpec.fromFiles](../src/main/scala/org/archive/archivespark/specific/warc/specs/WarcCdxHdfsSpec.scala)**(*cdxPaths*, *warcPath*) / **[WarcSpec.fromFilesWithCdx](../src/main/scala/org/archive/archivespark/specific/warc/specs/WarcCdxHdfsSpec.scala)**(*warcAndCdxPath*) | Loads a web archive collection that is available in CDX and (W)ARC format from a (distributed) filesystem, like HDFS.
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromFiles("/path/to/*.cdx.gz", "/path/to/warc_dir"))`
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromFilesWithCdx("/path/to/warc_dir_with_cdx"))`
**[WarcSpec.fromFiles](../src/main/scala/org/archive/archivespark/specific/warc/specs/WarcHdfsSpec.scala)**(*paths*) | Loads a web archive dataset from (W)ARC files without corresponding CDX records. Please note that this may be much slower for most operations except for batch processing that involve the whole collection. So it is highly recommended to use this DataSpec only to generate corresponding CDX records and reload it using along with CDX records in order to make use of ArchiveSpark's optimized two-step loading approach.
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromFiles("/path/to/*.*arc.gz"))`
**[WarcSpec.fromFiles](../src/main/scala/org/archive/archivespark/specific/warc/specs/WarcHdfsCdxPathRddSpec.scala)**(*cdxWithPathsRdd*) | Loads a web archive dataset from a (distributed) filesystem, like HDFS, given an RDD with tuples of the form `(CdxRecord, "/warc/path")`.
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromFiles(cdxWithPathsRdd))`
**[WarcSpec.fromFiles](../src/main/scala/org/archive/archivespark/specific/warc/specs/WarcHdfsCdxRddSpec.scala)**(*cdxRdd*, *warcPath*) | Loads a web archive dataset from a (distributed) filesystem, like HDFS, given an RDD of corresponding CDX records (e.g., loaded using `CdxHdfsSpec`).
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromFiles(cdxRdd, "/path/to/warc"))`
**[WarcSpec.fromWayback](../src/main/scala/org/archive/archivespark/specific/warc/specs/WaybackSpec.scala)**(*url*, [*matchPrefix*], [*from*], [*to*], [*blocksPerPage*], [*pages*]) | Loads a web archive dataset remotely from the [Internet Archive's Wayback Machine](http://web.archive.org) with the CDX metadata being fetched from CDX server. More details on the parameters for this DataSpec can be found on the [CDX server documentation](https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server).
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromWayback("helgeholzmann.de", matchPrefix = true, from = 201905, to = 201906, pages = 50))`
**[WarcSpec.fromWaybackByCdxQuery](../src/main/scala/org/archive/archivespark/specific/warc/specs/WaybackSpec.scala)**(*cdxServerUrl*, [*pages*]) | Loads a web archive dataset remotely from the [Internet Archive's Wayback Machine](http://web.archive.org) with the CDX metadata being fetched from the specified URL, e.g., [CDX server query](https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server).
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromWaybackByCdxQuery("http://web.archive.org/cdx/search/cdx?url=http://www.helgeholzmann.de&matchType=true", pages = 50))`
**[WarcSpec.fromWaybackWithLocalCdx](../src/main/scala/org/archive/archivespark/specific/warc/specs/WaybackCdxHdfsSpec.scala)**(*cdxPath*) | Loads a web archive collection from local CDX records with the corresponding data being fetched from the [Internet Archive's Wayback Machine](http://web.archive.org) remotely. 
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcSpec.fromWaybackWithLocalCdx("/path/to/*.cdx.gz"))`

## Additional DataSpecs for more data types 

In addition to the web archive specs we also provide some additional specs for raw files, available through `import org.archive.archivespark.specific.raw._`

DataSpec| Description
:-------|:---
**[HdfsFileSpec](../src/main/scala/org/archive/archivespark/specific/raw/HdfsFileSpec.scala)**(*path*, [*filePatterns*]) | Loads raw data records ([`FileStreamRecord`](../src/main/scala/org/archive/archivespark/specific/raw/FileStreamRecord.scala)) from the given path, which match the specified file patterns. This is an alternative to Spark's native loader methods, like `sc.textFile(...)`, but offers more flexibility as files accessed this way can be filtered by name before they are loaded and also provides raw data stream access.
&nbsp; | *Example:* `val textLines = ArchiveSpark.load(HdfsFileSpec("/path/to/data", Seq("*.txt.gz")).filter(_.get.contains("data")).flatMap(_.lineIterator)`

More DataSpecs for additional data types can be found in the following projects:

* Also for web archives, but starting from the temporal web archive search engine [Tempas](http://tempas.L3S.de/v2) to fetch metadata by keywords with data records loaded remotely from the Internet Archive's Wayback Machine: [Tempas2ArchiveSpark](https://github.com/helgeho/Tempas2ArchiveSpark)
* DataSpecs to analyze digitized books from the Internet Archive remotely with ArchiveSpark using local XML meta data. The main purpose of this project is to demonstrate how easily ArchiveSpark can be extended: [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark)
* The [Medical Heritage Library (MHL)](http://www.medicalheritage.org/) on ArchiveSpark project contains the required components for ArchiveSpark to work with MHL collections. It includes three DataSpecs to load data remotely through MHL's full-text search as well as from local files: [MHLonArchiveSpark](https://github.com/helgeho/MHLonArchiveSpark)

[< Table of Contents](README.md) | [Enrichment Functions >](EnrichFuncs.md)
:---|---: