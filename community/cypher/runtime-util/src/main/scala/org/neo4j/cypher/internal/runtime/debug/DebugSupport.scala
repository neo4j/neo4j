/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.debug

object DebugSupport {

  /** DEBUG CONFIGURATION **/

  final val DEBUG_PHYSICAL_PLANNING = false
  final val DEBUG_TIMELINE = false
  final val DEBUG_WORKERS = false
  final val DEBUG_QUERIES = false
  final val DEBUG_TRACKER = false
  final val DEBUG_LOCKS = false
  final val DEBUG_ERROR_HANDLING = false
  final val DEBUG_CLEANUP = false
  final val DEBUG_CURSORS = false
  final val DEBUG_BUFFERS = false
  final val DEBUG_SCHEDULING = false
  final val DEBUG_ASM = false
  final val DEBUG_TRANSACTIONAL_CONTEXT = false
  final val DEBUG_PIPELINES = false
  final val DEBUG_PROGRESS = false
  final val DEBUG_WORKERS_ON_PROGRESS_STALL = false
  final val DEBUG_MEMORY_TRACKING = false
  final val DEBUG_GENERATED_SOURCE_CODE = false
  final val DEBUG_CONCURRENT_TRANSACTIONS = false

  /** LOGS */

  final val PHYSICAL_PLANNING = new DebugLog(DEBUG_PHYSICAL_PLANNING, "")
  final val TIMELINE = new DebugTimeline(DEBUG_TIMELINE)
  final val WORKERS = new DebugLog(DEBUG_WORKERS, BrightYellow)
  final val QUERIES = new DebugLog(DEBUG_QUERIES, "")
  final val TRACKER = new DebugLog(DEBUG_TRACKER, Yellow)
  final val LOCKS = new DebugLog(DEBUG_LOCKS, Blue)
  final val ERROR_HANDLING = new DebugLog(DEBUG_ERROR_HANDLING, Red)
  final val CLEANUP = new DebugLog(DEBUG_CLEANUP, Green)
  final val CURSORS = new DebugLog(DEBUG_CURSORS, "")
  final val BUFFERS = new DebugLog(DEBUG_BUFFERS, Magenta)
  final val SCHEDULING = new DebugLog(DEBUG_SCHEDULING, Cyan)
  final val ASM = new DebugLog(DEBUG_ASM, "")
  final val TRANSACTIONAL_CONTEXT = new DebugLog(DEBUG_TRANSACTIONAL_CONTEXT, Green)
  final val MEMORY_TRACKING = new DebugLog(DEBUG_MEMORY_TRACKING, Yellow)
  final val PROGRESS = new DebugLog(DEBUG_PROGRESS, Bold + Underline)
  final val CONCURRENT_TRANSACTIONS = new DebugLog(DEBUG_CONCURRENT_TRANSACTIONS, "")
  final val CONCURRENT_TRANSACTIONS_WORKER = new DebugLog(DEBUG_CONCURRENT_TRANSACTIONS, BrightBlue)

  /** COLORS AND FORMATTING **/

  final val Black = "\u001b[30m"
  final val Red = "\u001b[31m"
  final val Green = "\u001b[32m"
  final val Yellow = "\u001b[33m"
  final val BrightYellow = "\u001b[33;1m"
  final val Blue = "\u001b[34m"
  final val BrightBlue = "\u001b[34;1m"
  final val Magenta = "\u001b[35m"
  final val Cyan = "\u001b[36m"
  final val White = "\u001b[37m"

  final val Bold = "\u001b[1m"
  final val Underline = "\u001b[4m"
  final val Reversed = "\u001b[7m"

  final val Reset = "\u001b[0m"

  /** TOOLING **/

  def stackTraceOfCurrentThread(depth: Int = 0): String = {
    stackTraceSlice(Thread.currentThread().getStackTrace, skipTop = 2, depth)
  }

  def stackTraceOfThrowable(throwable: Throwable, depth: Int = 0): String = {
    stackTraceSlice(throwable.getStackTrace, skipTop = 0, depth)
  }

  private def stackTraceSlice(stackTrace: Array[StackTraceElement], skipTop: Int, depth: Int): String = {
    val stackTraceSlice = if (depth > 0) stackTrace.slice(skipTop, skipTop + depth) else stackTrace.drop(skipTop)
    val stackTraceSliceString = stackTraceSlice.map(e =>
      s"          ${e.getClassName}.${e.getMethodName} (${e.getFileName}:${e.getLineNumber()})"
    ).mkString("\n")
    stackTraceSliceString
  }

  def logPipelines(rows: => collection.Seq[String]): Unit = {
    if (DEBUG_PIPELINES) {
      for (row <- rows) {
        print(s"       || $row\n")
      }
    }
  }

  final class DebugLog(private[this] var enabled: Boolean, val color: String, val filter: String => Boolean = null) {

    def enable(): Unit = {
      enabled = true
    }

    def disable(): Unit = {
      enabled = false
    }

    // Not using println because that is synchronized and can hide
    // parallel problems.
    def log(str: String): Unit = {
      if (enabled) {
        val s = s"        $color$str$Reset\n"
        if (filter == null || filter(s)) {
          print(s)
        }
      }
    }

    def log(str: String, x: Any): Unit =
      if (enabled) {
        log(str.format(x))
      }

    def log(str: String, x1: Any, x2: Any): Unit =
      if (enabled) {
        log(str.format(x1, x2))
      }

    def log(str: String, x1: Any, x2: Any, x3: Any): Unit =
      if (enabled) {
        log(str.format(x1, x2, x3))
      }

    def log(str: String, x1: Any, x2: Any, x3: Any, x4: Any): Unit =
      if (enabled) {
        log(str.format(x1, x2, x3, x4))
      }

    def log(str: String, x1: Any, x2: Any, x3: Any, x4: Any, x5: Any): Unit =
      if (enabled) {
        log(str.format(x1, x2, x3, x4, x5))
      }

    def log(str: String, x1: Any, x2: Any, x3: Any, x4: Any, x5: Any, x6: Any): Unit =
      if (enabled) {
        log(str.format(x1, x2, x3, x4, x5, x6))
      }

    def log(throwable: Throwable): Unit = {
      if (enabled) {
        throwable.printStackTrace()
      }
    }
  }

  final class DebugTimeline(private[this] val enabled: Boolean) {

    private var t0: Long = 0L
    private var tn: Long = 0L

    def beginTime(): Unit =
      if (enabled) {
        println("")
        println("            ~= BEGINNING OF TIME =~")
        t0 = System.currentTimeMillis()
        log("")
      }

    def log(str: String): Unit =
      if (enabled) {
        tn = System.currentTimeMillis()
        println("[%6d ms] %s".format(tn - t0, str))
      }

    def log(str: String, x: Any): Unit =
      if (enabled) {
        tn = System.currentTimeMillis()
        println("[%6d ms] %s".format(tn - t0, str.format(x)))
      }

    def log(str: String, x1: Any, x2: Any): Unit =
      if (enabled) {
        tn = System.currentTimeMillis()
        println("[%6d ms] %s".format(tn - t0, str.format(x1, x2)))
      }

    def log(str: String, x1: Any, x2: Any, x3: Any): Unit =
      if (enabled) {
        tn = System.currentTimeMillis()
        println("[%6d ms] %s".format(tn - t0, str.format(x1, x2, x3)))
      }

    def logDiff(str: String): Unit =
      if (enabled) {
        val tPrev = tn
        tn = System.currentTimeMillis()
        println("     %+4d ms: %s".format(tn - tPrev, str))
      }
  }
}
