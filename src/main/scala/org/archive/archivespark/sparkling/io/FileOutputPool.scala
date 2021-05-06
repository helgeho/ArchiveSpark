package org.archive.archivespark.sparkling.io

import java.io.{File, OutputStream}

import org.archive.archivespark.sparkling.util.CacheMap

class FileOutputPool(minOpen: Int, maxOpen: Int) {
  private val cache = CacheMap[String, AppendingFileOutputStream](minOpen, maxOpen, (clear: Set[String]) => for (file <- clear) close(file)) { case (_, _) => 1L }

  def get(file: File): OutputStream = get(file.getCanonicalPath)

  def get(file: String): OutputStream = {
    cache.getOrElse(file, new AppendingFileOutputStream(new File(file), onOpen = stream => cache.cache(file, stream), onClose = _ => cache.remove(file)))
  }

  def close(file: File): Unit = close(file.getCanonicalPath)
  def close(file: String): Unit = for (out <- cache.get(file)) out.close()
}
