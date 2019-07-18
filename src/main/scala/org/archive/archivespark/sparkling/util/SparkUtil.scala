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

import org.apache.spark.TaskContext

object SparkUtil {
  private var cleanupObjects = collection.mutable.Map.empty[Any, Long]
  private var cleanups = collection.mutable.Map.empty[Long, collection.mutable.Map[Any, () => Unit]]

  def cleanupTask(owner: Any, cleanup: () => Unit): Unit = {
    val task = TaskContext.get
    if (task != null) {
      cleanupObjects.getOrElseUpdate(owner, {
        val attemptId = task.taskAttemptId
        val taskCleanups = cleanups.getOrElseUpdate(attemptId, {
          val taskCleanups = collection.mutable.Map.empty[Any, () => Unit]
          task.addTaskCompletionListener(ctx => {
            cleanups.remove(attemptId)
            for ((o, c) <- taskCleanups) {
              cleanupObjects.remove(o)
              c()
            }
            taskCleanups.clear()
          })
          taskCleanups
        })
        taskCleanups.update(owner, cleanup)
        attemptId
      })
    }
  }

  def removeTaskCleanup(owner: Any): Unit = for (attemptId <- cleanupObjects.remove(owner)) cleanups(attemptId).remove(owner)
}
