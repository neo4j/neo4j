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
package org.neo4j.cypher.internal.runtime.interpreted.load_csv

import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCsvIterator

class LoadCsvIteratorWithPeriodicCommit(loadCsvIterator: LoadCsvIterator)(onNext: => Unit) extends LoadCsvIterator {

  var lastCommitted: Long = -1L

  def lastProcessed: Long = loadCsvIterator.lastProcessed

  def readAll: Boolean = loadCsvIterator.readAll

  def next(): Array[String] = {
    val row = loadCsvIterator.next()
    onNext
    row
  }

  def hasNext: Boolean = loadCsvIterator.hasNext

  def notifyCommit() {
    lastCommitted = loadCsvIterator.lastProcessed - 1
  }
}
