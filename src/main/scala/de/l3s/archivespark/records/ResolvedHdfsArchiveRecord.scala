/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Helge Holzmann (L3S) and Vinay Goel (Internet Archive)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 */

package de.l3s.archivespark.records

import java.io.InputStream

import de.l3s.archivespark.ResolvedArchiveRecord
import de.l3s.archivespark.cdx.ResolvedCdxRecord
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.deploy.SparkHadoopUtil

class ResolvedHdfsArchiveRecord(cdx: ResolvedCdxRecord, cdxPath: String = null) extends ResolvedArchiveRecord(cdx) {
  override def access[R >: Null](action: (String, InputStream) => R): R = {
    if (cdx.location.compressedSize < 0 || cdx.location.offset < 0) null
    else {
      val fs = FileSystem.get(SparkHadoopUtil.get.conf)
      findArchivePath(fs, cdx.location.fileLocation, cdx.location.filename) match {
        case None => fs.close(); null
        case Some(path) =>
          val stream = fs.open(path)
          try {
            stream.seek(cdx.location.offset)
            action(cdx.location.filename, new BoundedInputStream(stream, cdx.location.compressedSize))
          } catch {
            case e: Exception => null /* something went wrong, do nothing */
          } finally {
            stream.close()
            fs.close()
          }
      }
    }
  }

  private def findArchivePath(fs: FileSystem, base: String, filename: String): Option[Path] = {
    var cdxBase = if (cdxPath == null) null else new Path(cdxPath).getParent
    var dir = new Path(base)
    var path = new Path(dir, filename)
    while (!fs.exists(path)) {
      if (cdxBase == null || cdxBase.depth() == 0) return None
      dir = new Path(dir, cdxBase.getName)
      cdxBase = cdxBase.getParent
      path = new Path(dir, filename)
    }
    Some(path)
  }
}
