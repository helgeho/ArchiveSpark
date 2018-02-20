[< Table of Contents](README.md) | [How to Implement Enrich Functions >](Dev_EnrichFuncs.md)
:---|---:

# How to Implement DataSpecs

ArchiveSpark comes with a base class for Data Specifications, called [`DataSpec`](../src/main/scala/de/l3s/archivespark/dataspecs/DataSpec.scala). It accepts two types to be defined: The first is `Raw`, which is the type of metadata as loaded from disk or a remote source by the `load` method, e.g., `String` for raw text. Each loaded metadata record is then passed to the `parse` method, which has to be implemented with the logic to transform the raw metadata into a record of your dataset type `Record`. This can be any custom class derived from [`EnrichRoot`](../src/main/scala/de/l3s/archivespark/enrich/EnrichRoot.scala). These records store and provide access to the metadata as well as include the logics to access the actual data records.

For examples, please have a look at the included DataSpecs, such as [`HdfsFileSpec`](../src/main/scala/de/l3s/archivespark/specific/raw/HdfsFileSpec.scala) or the external [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark) project. For more information on how to deploy and share your DataSpecs, please read [Contribute](Contribute.md).

[< Table of Contents](README.md) | [How to Implement Enrich Functions >](Dev_EnrichFuncs.md)
:---|---: