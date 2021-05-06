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
