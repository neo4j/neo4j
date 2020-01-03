/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

object DebugLog {

  final val ENABLED = false

  private var t0: Long = 0L
  private var tn: Long = 0L

  def beginTime(): Unit =
    if (ENABLED) {
      println("")
      println("            ~= BEGINNING OF TIME =~")
      t0 = System.currentTimeMillis()
      log("")
    }

  def log(str: String): Unit =
    if (ENABLED) {
      tn = System.currentTimeMillis()
      println("[%6d ms] %s".format(tn - t0, str))
    }

  def log(str: String, x: Any): Unit =
    if (ENABLED) {
      tn = System.currentTimeMillis()
      println("[%6d ms] %s".format(tn - t0, str.format(x)))
    }

  def log(str: String, x1: Any, x2: Any): Unit =
    if (ENABLED) {
      tn = System.currentTimeMillis()
      println("[%6d ms] %s".format(tn - t0, str.format(x1, x2)))
    }

  def log(str: String, x1: Any, x2: Any, x3: Any): Unit =
    if (ENABLED) {
      tn = System.currentTimeMillis()
      println("[%6d ms] %s".format(tn - t0, str.format(x1, x2, x3)))
    }

  def logDiff(str: String): Unit =
    if (ENABLED) {
      val tPrev = tn
      tn = System.currentTimeMillis()
      println("     %+4d ms: %s".format(tn - tPrev, str))
    }
}
