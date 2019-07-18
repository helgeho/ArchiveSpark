/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015-2019 Helge Holzmann (Internet Archive) <helge@archive.org>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.archive.archivespark.sparkling.util

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat, FileSplit}
import org.apache.hadoop.mapreduce.{InputSplit, JobContext, RecordReader, TaskAttemptContext}

class FileLocalityInputFormat extends FileInputFormat[NullWritable, Text] {
  class FileLocalityRecordReader extends RecordReader[NullWritable, Text] {
    private var filePath: Text = new Text()
    private var read: Boolean = true

    override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
      filePath.set(split.asInstanceOf[FileSplit].getPath.toString)
      read = false
    }

    override def nextKeyValue(): Boolean = {
      if (read) false
      else {
        read = true
        true
      }
    }

    override def getCurrentKey: NullWritable = NullWritable.get
    override def getCurrentValue: Text = filePath
    override def getProgress: Float = if (read) 1.0f else 0.0f
    override def close(): Unit = read = true
  }

  override def isSplitable(context: JobContext, filename: Path): Boolean = false
  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[NullWritable, Text] = new FileLocalityRecordReader
}
