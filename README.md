# ArchiveSpark

![ArchiveSpark Logo](./logo.png)

An Apache Spark framework (not only) for Web Archives that enables easy data processing, extraction as well as derivation.

To get you started, please find more information in our [wiki](https://github.com/helgeho/ArchiveSpark/wiki).

Dataspecs and basic enrich functions for Web Archives are included in this core project.
The [IABooksOnArchiveSpark](https://github.com/helgeho/IABooksOnArchiveSpark) project is an extension to work with books from the Internet Archive. 
It was created to demonstrate how easily [ArchiveSpark](https://github.com/helgeho/ArchiveSpark) can be extended and can be used as a template to add support for new data types to [ArchiveSpark](https://github.com/helgeho/ArchiveSpark) as well as to implement new enrich functions separated from the core project.

## Cite

ArchiveSpark has been published as a research paper at [JCDL 2016](http://www.jcdl2016.org), where it was nominated for the Best Paper Award.
If you use ArchiveSpark in your work, please cite:

*[Helge Holzmann, Vinay Goel, Avishek Anand. ArchiveSpark: Efficient Web Archive Access, Extraction and Derivation. In Proceedings of the Joint Conference on Digital Libraries, Newark, New Jersey, USA, 2016.](http://dl.acm.org/citation.cfm?id=2910902)*
**[Download PDF](http://l3s.de/~holzmann/papers/archivespark2016jcdl.pdf)**

More information can be found in the slides presented at the [WebSci`16 Hackathon](http://www.websci16.org/hackathon):

http://www.slideshare.net/HelgeHolzmann/archivespark-introduction-websci-2016-hackathon

## Related projects

__[ArchivePig](https://github.com/helgeho/ArchivePig)__

The original implementation of the ArchiveSpark concept was built on [Apache Pig](https://pig.apache.org) instead of Spark.
The project was the inspiration for this one and can be found under [ArchivePig](https://github.com/helgeho/ArchivePig).
However, it is not actively being developed anymore, but can be used if you prefer Pig over Spark.

__[Web2Warc](https://github.com/helgeho/Web2Warc)__

If you do not have Web archive data available to use it with ArchiveSpark, easily create your own from any collection of websites with [Web2Warc](https://github.com/helgeho/Web2Warc).

## License

The MIT License (MIT)

Copyright (c) 2015-2017 Helge Holzmann ([L3S](http://www.L3S.de)) and Vinay Goel ([Internet Archive](http://www.archive.org))

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
