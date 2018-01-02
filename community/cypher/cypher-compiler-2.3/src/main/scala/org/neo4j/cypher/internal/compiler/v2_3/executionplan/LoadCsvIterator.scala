/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import java.net.URL

class LoadCsvIterator(url: URL, inner: Iterator[Array[String]])(onNext: => Unit) extends Iterator[Array[String]] {
  var lastProcessed = 0L
  var lastCommitted = -1L
  var readAll = false

  def next() = {
    val row = inner.next()
    onNext
    lastProcessed += 1
    readAll = !hasNext
    row
  }

  def hasNext = inner.hasNext

  def notifyCommit() {
    lastCommitted = lastProcessed
  }

  def msg = {
    val committedAnything = lastCommitted >= 0
    (s"Failure when processing URL '$url' on line $lastProcessed") +
      (if (readAll) " (which is the last row in the file). " else ". ") +
      (if (committedAnything)
        s"Possibly the last row committed during import is line $lastCommitted. "
      else
        "No rows seem to have been committed. ") +
      "Note that this information might not be accurate."
  }
}
