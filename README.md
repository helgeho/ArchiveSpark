# ArchiveSpark

![ArchiveSpark Logo](./logo.png)

An Apache Spark framework for easy data processing, extraction as well as derivation for archival collections. Originally developed for the use with Web archives, it has now been extended to support any archival dataset through Data Specifications.

For more information and instructions, please read the **[ArchiveSpark Documentation](docs/README.md)** (in progress).

## Cite

ArchiveSpark has been published as a research paper at [JCDL 2016](http://www.jcdl2016.org), where it was nominated for the Best Paper Award.
If you use ArchiveSpark in your work, please cite:

[H. Holzmann, V. Goel and A. Anand. *ArchiveSpark: Efficient Web Archive Access, Extraction and Derivation.* 16th ACM/IEEE-CS Joint Conference on Digital Libraries (JCDL). Newark, New Jersey, USA. June 2016.](http://dl.acm.org/citation.cfm?id=2910902) [**Get full-text PDF**](http://www.helgeholzmann.de/papers/JCDL_2016_ArchiveSpark.pdf)

The extensions to make it a more universal / generic data processing platform for any archival collection were presented at IEEE BigData 2017 (Short Paper):

[H. Holzmann, Emily Novak Gustainis and Vinay Goel. *Universal Distant Reading through Metadata Proxies with ArchiveSpark*. 5th IEEE International Conference on Big Data (BigData). Boston, MA, USA. December 2017.](http://cci.drexel.edu/bigdata/bigdata2017/AcceptedPapers.html) [**Get full-text PDF**](http://www.helgeholzmann.de/papers/BIGDATA_2017.pdf)

## Related projects

__[ArchivePig](https://github.com/helgeho/ArchivePig)__

The original implementation of the ArchiveSpark concept was built on [Apache Pig](https://pig.apache.org) instead of Spark.
The project was the inspiration for this one and can be found under [ArchivePig](https://github.com/helgeho/ArchivePig).
However, it is not actively being developed anymore, but can be used if you prefer Pig over Spark.

__[Web2Warc](https://github.com/helgeho/Web2Warc)__

If you do not have Web archive data available to use it with ArchiveSpark, easily create your own from any collection of websites with [Web2Warc](https://github.com/helgeho/Web2Warc).

__[HadoopConcatGz](https://github.com/helgeho/HadoopConcatGz)__

A Splitable Hadoop InputFormat for Concatenated GZIP Files and *.(w)arc.gz, also used by ArchiveSpark.

## License

The MIT License (MIT)

Copyright (c) 2015-2018 [Helge Holzmann](http://www.HelgeHolzmann.de) ([L3S](http://www.L3S.de)) and Vinay Goel ([Internet Archive](http://www.archive.org))

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
