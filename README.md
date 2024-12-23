# ArchiveSpark

[![ArchiveSpark Logo](./logo.png)](https://github.com/helgeho/ArchiveSpark)

ArchiveSpark is a framework / toolkit / library / API to facilitate **efficient data processing, extraction as well as derivation for archival collections**.

While originally developed for the use with web archives, which is still its main focus, ArchiveSpark can be used with any (archival) data collections through its modular architecture and customizable _data specifications_.

### What can you do with it?

The main use case of ArchiveSpark is the **efficient access to archival data** with the goal to derive corpora by applying **filters and tools** in order to extract information from the original raw data, to be stored in a more **accessible format, like JSON**, while reflecting the data **lineage of each derived value**.

Examples of what you can do with it include: (see [**recipes**](docs/Recipes.md) for code examples)

* Selecting a **subset of your data** and **extracting desired properties** (e.g., title, entities, ...)
* Running a **(temporal) data analysis** on the filtered / extracted / derived data
* Generating **hyperlink or knowledge graphs** for downstream applications 
* **Processing archived webpages** and extracting embedded resources
* Downloading **remote WARC/CDX data from the Internet Archive's Wayback Machine**

### New in 3.0

* Namespace changed to `org.archive.webservices.archivespark`.
* Extensive overhaul to be based on _Sparkling_, Internet Archive's internal data processing library, which is now partially included under `org.archive.webservices.sparkling`.
* _ArchiveSpark_ will evolve as _Sparkling_ evolves and automatically benefit from new features and bugfixes.
* Streamlined with all unused / unnecessary / academic / experimental features being removed.
* Refactored / cleaned up / simplified ArchiveSpark's public APIs.

For more information and instructions, please [**read the docs**](docs/README.md):
 
 [![ArchiveSpark Documentation](./docs_button.png)](docs/README.md)

## License

The MIT License (MIT)

Copyright (c) 2015-2024 [Helge Holzmann](http://www.HelgeHolzmann.de) ([Internet Archive](http://www.archive.org)) <[helge@archive.org](mailto:helge@archive.org)>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
