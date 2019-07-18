[< Table of Contents](README.md) | [How to Implement Enrichment Functions >](Dev_EnrichFuncs.md)
:---|---:

# How to Implement DataSpecs

ArchiveSpark comes with a base class for Data Specifications, called [`DataSpec`](../src/main/scala/org/archive/archivespark/dataspecs/DataSpec.scala). It accepts two types to be defined: The first is `Raw`, which is the raw type of metadata to be loaded from disk or a remote source by the `load` method, e.g., `String` for raw text. Each loaded metadata record is then passed to the `parse` method, which has to implement the logic that transforms the raw data into a record of your dataset. This can be any custom class derived from [`EnrichRoot`](../src/main/scala/org/archive/archivespark/enrich/EnrichRoot.scala). These records store and provide access to the metadata as well as include the logic to access the actual data records.

For examples, please have a look at the included DataSpecs, such as [`HdfsFileSpec`](../src/main/scala/org/archive/archivespark/specific/raw/HdfsFileSpec.scala) or the external [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark) project. For more information on how to deploy and share your DataSpecs, please read [Contribute](Contribute.md).

[< Table of Contents](README.md) | [How to Implement Enrichment Functions >](Dev_EnrichFuncs.md)
:---|---: