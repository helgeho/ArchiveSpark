[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---:

# How to Implement Enrich Functions

ArchiveSpark comes with multiple base classes to implement your custom Enrich Functions. These can provide completely new derivation / extraction logics or expose an interface to your own libraries. In order to deploy and share your Enrich Functions, please create a separate project or include an additional class, which serves as an API to your library with your library's project. 

All Enrich Functions need to be of type `EnrichFunc`, which is a generic class that accepts two types: The `Root` type, which defines what type of records it is applicable to by default, as well as the `Source` type, which defines its input type. To define a default result field with a default output type, we provide the [`DefaultField`](../src/main/scala/de/l3s/archivespark/enrich/DefaultField.scala) trait as well as the [`SingleField`](../src/main/scala/de/l3s/archivespark/enrich/SingleField.scala) for Enrich Functions that produce only one result field. For the most common types of Enrich Functions, we provide simplified base classes, which are usually sufficient for the most common use cases: [BasicEnrichFunc](../src/main/scala/de/l3s/archivespark/enrich/BasicEnrichFunc.scala), [BasicDependentEnrichFunc](../src/main/scala/de/l3s/archivespark/enrich/BasicDependentEnrichFunc.scala),[BasicMultiValEnrichFunc](../src/main/scala/de/l3s/archivespark/enrich/BasicMultiValEnrichFunc.scala),[BasicMultiValDependentEnrichFunc](../src/main/scala/de/l3s/archivespark/enrich/BasicMultiValDependentEnrichFunc.scala).

For examples, please have a look at the included Enrich Functions, such as [`LowerCase`](../src/main/scala/de/l3s/archivespark/enrich/functions/LowerCase.scala) or the external [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark) project. For more information on how to deploy and share your DataSpecs, please read [Contribute](Contribute.md).

[< Table of Contents](README.md) | [Contribute >](Contribute.md)
:---|---: