[< Table of Contents](README.md) | [Use ArchiveSpark as a Library (advanced) >](Use_Library.md)
:---|---:

# Build ArchiveSpark

To build the ArchiveSpark JAR files from source you need to have Scala 2.11 as well as SBT installed.
Then simply run the following build commands from within the ArchiveSpark folder:

1. `sbt assembly`
2. `sbt assemblyPackageDependency`

These commands will create two JAR files under `target/scala-2.11`, one for ArchiveSpark and one for the required dependencies.
Please include these files in your project that depends on ArchiveSpark or add them to your JVM classpath.

There are also pre-built versions available that you can add as dependencies to your projects.
Fore more information, please read [Use ArchiveSpark as a library (advanced)](Use_Library.md).

[< Table of Contents](README.md) | [Use ArchiveSpark as a Library (advanced) >](Use_Library.md)
:---|---: