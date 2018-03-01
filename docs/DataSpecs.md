[< Table of Contents](README.md) | [Enrich Functions >](EnrichFuncs.md)
:---|---:

# Data Specifications (DataSpecs)

Data Specifications (DataSpecs) are abstractions of the load and read logics for metadata as well as data records.
Depending on your data source and type you need to select an appropriate one.
As part of core ArchiveSpark we provide DataSpecs for Web Archives (CDX/(W)ARC format) as well as some raw data types, such as text.
More DataSpecs for different data types and sources may be found in different projects, contributed by independent developers or ourselves (see below).

For more information on the usage of DataSpecs, please read [General Usage](General_Usage.md).

## Web Archive DataSpecs

The following DataSpecs are specific to Web archive datasets. These become available by this import: `import de.l3s.archivespark.specific.warc.specs._`

DataSpec| Description
:-------|:---
**[WarcCdxHdfsSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/WarcCdxHdfsSpec.scala)**(*cdxPaths*, *warcPath*) | Loads a Web archive collection that is available in CDX and (W)ARC format from a (distributed) filesystem, like HDFS.
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcCdxHdfsSpec("/path/to/*.cdx.gz", "/path/to/warc_dir"))`
**[CdxHdfsSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/CdxHdfsSpec.scala)**(*paths*) | Loads a collection of CDX records (meta data only) from a (distributed) filesystem, like HDFS. This is helpful to resolve *revisit records*, before loading the corresponding (W)ARC records, using `WarcHdfsCdxRddSpec`.
&nbsp; | *Example:* `val cdxRdd = ArchiveSpark.load(CdxHdfsSpec("/path/to/*.cdx.gz"))` 
**[CdxHdfsWaybackSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/CdxHdfsWaybackSpec.scala)**(*cdxPath*) | Loads a Web archive collection from local CDX records with the corresponding data being fetched from the [Internet Archive's Wayback Machine](http://web.archive.org) remotely. 
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(CdxHdfsWaybackSpec("/path/to/*.cdx.gz"))`
**[WarcHdfsSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/WarcHdfsSpec.scala)**(*paths*) | Loads a Web archive dataset from (W)ARC files without corresponding CDX records. Please note that this may be much slower for most operations except for batch processing that involve the whole collection. So it is highly recommended to use this DataSpec only to generate corresponding CDX records and reload it using `WarcCdxHdfsSpec` in order to make use of ArchiveSpark's optimized two-step loading approach.
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcHdfsSpec("/path/to/*.*arc"))`
**[WarcGzHdfsSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/WarcGzHdfsSpec.scala)**(*cdxPath*, *warcPath*) | An optimized version of `WarcHdfsSpec` for dataset stored in WARC.gz with each record compressed individually, making use of the [*HadoopConcatGz*](https://github.com/helgeho/HadoopConcatGz) input format.
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcGzHdfsSpec("/path/to/warc.gz"))`
**[WarcHdfsCdxPathRddSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/WarcHdfsCdxPathRddSpec.scala)**(*cdxWithPathsRdd*) | Loads a Web archive dataset from a (distributed) filesystem, like HDFS, given an RDD with tuples of the form `(CdxRecord, WarcPath)`. After loading the CDX records using `CdxHdfsSpec`, an RDD of this form can be created using [`rdd.mapInfo(...)`](../src/main/scala/de/l3s/archivespark/specific/warc/implicits/ResolvableRDD.scala), given another RDD that maps metadata to corresponding (W)ARC paths.
&nbsp; | *Example:* `val cdxRdd = ArchiveSpark.load(CdxHdfsSpec("/path/to/*.cdx.gz")`<br>`val cdxWithPathsRdd = cdxRdd.mapInfo(_.digest, digestWarcPathRdd)`<br>`val rdd = ArchiveSpark.load(WarcHdfsCdxPathRddSpec(cdxWithPathsRdd))`
**[WarcHdfsCdxRddSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/WarcHdfsCdxRddSpec.scala)**(*cdxRdd*, *warcPath*) | Loads a Web archive dataset from a (distributed filesystem, like HDFS, given an RDD of corresponding CDX records (e.g., loaded using `CdxHdfsSpec`).
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WarcHdfsCdxRddSpec(cdxRdd, "/path/to/warc"))`
**[WaybackSpec](../src/main/scala/de/l3s/archivespark/specific/warc/specs/WaybackSpec.scala)**(*url*, [*matchPrefix*], [*from*], [*to*], [*blocksPerPage*], [*pages*]) | Loads a Web archive dataset completely remotely from the [Internet Archive's Wayback Machine](http://web.archive.org) with the CDX metadata being fetched from their CDX server. More details on the parameters for this DataSpec can be found on the [CDX server documentation](https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server).
&nbsp; | *Example:* `val rdd = ArchiveSpark.load(WaybackSpec("l3s.de", matchPrefix = true, from = 2010, to = 2012, pages = 100))`

## Additional DataSpecs for more data types 

In addition to the Web archive specs we also provide some additional specs for raw files, availble through `import de.l3s.archivespark.specific.raw._`

DataSpec| Description
:-------|:---
**[HdfsFileSpec](../src/main/scala/de/l3s/archivespark/specific/raw/HdfsFileSpec.scala)**(*path*, [*filePatterns*]) | Loads raw data records ([`FileStreamRecord`](../src/main/scala/de/l3s/archivespark/specific/raw/HdfsFileSpec.scala)) from the given path and matching the specified file patterns. This is an alternative to Spark's native `sc.textFile(...)`, but offers more flexibility as files can be filtered by name before they are loaded and also provides raw data stream access.
&nbsp; | *Example:* `val textLines = ArchiveSpark.load(HdfsFileSpec("/path/to/data", Seq("*.txt.gz")).flatMap(_.lineIterator)`

More DataSpecs for additional data types can be found in the following projects:

* Also for Web archives, but starting from the temporal Web archive search engine [Tempas](http://tempas.L3S.de/v2) to fetch metadata by keywords with data records loaded remotely from the Internet Archive's Wayback Machine: [Tempas2ArchiveSpark](https://github.com/helgeho/Tempas2ArchiveSpark)
* [Boris Smidt](https://github.com/borissmidt) has implemented a DataSpec to load Web archive collections that use the JSON-based CDX format *JCDX*: [ArchiveSparkJCDX](https://github.com/trafficdirect/ArchiveSparkJCDX)
* DataSpecs to analyze digitized books from the Internet Archive remotely with ArchiveSpark using local XML meta data. The main purpose of this project is to demonstrate how easily ArchiveSpark can be extended: [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark)
* The [Medical Heritage Library (MHL)](http://www.medicalheritage.org/) on ArchiveSpark project contains the required components for ArchiveSpark to work with MHL collections. It includes three DataSpecs to load data remotely through MHL's full-text search as well as from local files: [MHLonArchiveSpark](https://github.com/helgeho/MHLonArchiveSpark)

[< Table of Contents](README.md) | [Enrich Functions >](EnrichFuncs.md)
:---|---: