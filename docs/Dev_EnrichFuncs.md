[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---:

# How to Implement Enrichment Functions

ArchiveSpark comes with multiple base classes to implement your custom Enrichment Functions. These can provide completely new derivation / extraction logics or expose an interface to your own libraries. In order to deploy and share your Enrichment Functions, please create a separate project or include an additional class, which serves as an API to your library, with your library's project. 

All Enrichment Functions need to be of type `EnrichFunc`, which is a generic class that accepts two types: The `Root` type, which defines what type of records it is applicable to by default, e.g., `WarcRecord` as well as the `Source` type, which defines its input type, e.g., `String`. In most most cases though, the easiest way is to `map` an existing enrich function (see [ArchiveSpark Operations](Operations.md)), for which we provide the `GlobalEnrichFunc` base class to publicly expose Enrichment Functions that are created that way as global objects.

For examples, please have a look at the included Enrichment Functions, such as [`LowerCase`](../src/main/scala/org/archive/archivespark/functions/LowerCase.scala) or the external [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark) project. For more information on how to deploy and share your DataSpecs, please read [Contribute](Contribute.md).

[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---: