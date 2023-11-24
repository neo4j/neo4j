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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.values.storable.Value

import java.net.URL

trait ExternalCSVResource {

  def getCsvIterator(
    url: URL,
    config: Config,
    fieldTerminator: Option[String],
    legacyCsvQuoteEscaping: Boolean,
    bufferSize: Int,
    headers: Boolean = false
  ): LoadCsvIterator
}

trait LoadCsvIterator extends ClosingIterator[Array[Value]] {
  def lastProcessed: Long
  def readAll: Boolean
}

object LoadCsvIterator {

  def empty: LoadCsvIterator = new LoadCsvIterator {
    override protected[this] def closeMore(): Unit = ()
    override def lastProcessed: Long = 0L
    override def readAll: Boolean = false
    override def innerHasNext: Boolean = false
    def next(): Nothing = throw new NoSuchElementException("next on empty iterator")
  }
}
