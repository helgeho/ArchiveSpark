[< Table of Contents](README.md) | [Using ArchiveSpark as a Library (advanced) >](Using_Library.md)
:---|---:

# Building ArchiveSpark

To build the ArchiveSpark JAR files from source you need to have Scala 2.12 as well as SBT installed.

Sparkling...

Then simply run the following build command from within the ArchiveSpark folder:

`sbt assembly`

This commands will create a JAR files under `target/scala-2.12`.
Please include this file in your project that depends on ArchiveSpark or add it to your JVM classpath.

There are also pre-built versions available that you can add as dependencies to your projects.
Fore more information, please read [Using ArchiveSpark as a library (advanced)](Using_Library.md).

[< Table of Contents](README.md) | [Using ArchiveSpark as a Library (advanced) >](Using_Library.md)
:---|---: