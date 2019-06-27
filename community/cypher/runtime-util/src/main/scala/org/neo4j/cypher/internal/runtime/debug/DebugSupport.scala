/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.debug

object DebugSupport {

  final val DEBUG_WORKERS = false
  final val DEBUG_QUERIES = false
  final val DEBUG_TRACKER = false
  final val DEBUG_LOCKS = false
  final val DEBUG_ERROR_HANDLING = false
  final val DEBUG_CURSORS = false
  final val DEBUG_PIPELINES = false
  final val DEBUG_BUFFERS = false
  final val DEBUG_SCHEDULING = false
  final val DEBUG_ASM = false

  final val Black   = "\u001b[30m"
  final val Red     = "\u001b[31m"
  final val Green   = "\u001b[32m"
  final val Yellow  = "\u001b[33m"
  final val Blue    = "\u001b[34m"
  final val Magenta = "\u001b[35m"
  final val Cyan    = "\u001b[36m"
  final val White   = "\u001b[37m"

  final val Bold      = "\u001b[1m"
  final val Underline = "\u001b[4m"
  final val Reversed  = "\u001b[7m"

  final val Reset   = "\u001b[0m"

  // Not using println because that is synchronized and can hide
  // parallel problems.

  def logWorker(str: => String): Unit = {
    if (DEBUG_WORKERS) {
      print(s"        $str\n")
    }
  }

  def logQueries(str: => String): Unit = {
    if (DEBUG_QUERIES) {
      println(s"        $str")
    }
  }

  def logTracker(str: => String): Unit = {
    if (DEBUG_TRACKER) {
      print(s"        $Yellow$str$Reset\n")
    }
  }

  def logLocks(str: => String): Unit = {
    if (DEBUG_LOCKS) {
      print(s"        $Blue$str$Reset\n")
    }
  }

  def logErrorHandling(str: => String): Unit = {
    if (DEBUG_ERROR_HANDLING) {
      print(s"        $Red$str$Reset\n")
    }
  }

  def logCursors(str: => String): Unit = {
    if (DEBUG_CURSORS) {
      print(s"        $str\n")
    }
  }

  def logBuffers(str: => String): Unit = {
    if (DEBUG_BUFFERS) {
      print(s"        $Magenta$str$Reset\n")
    }
  }

  def logAsm(str: => String): Unit = {
    if (DEBUG_ASM) {
      print(s"        $str\n")
    }
  }

  def logScheduling(str: => String): Unit = {
    if (DEBUG_SCHEDULING) {
      print(s"      $Cyan$str$Reset\n")
    }
  }

  def logPipelines(rows: => Seq[String]): Unit = {
    if (DEBUG_PIPELINES) {
      for (row <- rows) {
        print(s"       || $row\n")
      }
    }
  }
}
