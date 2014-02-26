/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.neo4j.cypher.internal.compiler.v2_1.pipes.ExternalResource
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import java.net.URL
import org.neo4j.cypher.{CypherException, LoadCsvStatusWrapCypherException}

class LoadCsvPeriodicCommitObserver(batchSize: Long, resources: ExternalResource, queryContext: QueryContext)
  extends UpdateObserver with ExternalResource with ((CypherException) => CypherException) {

  var updates = 0L
  var first = true
  var it: Option[LoadCsvIterator] = None

  def notify(increment: Long) {
    assert(increment > 0, "increment must be positive")
    updates += increment
    maybeCommitAndRestartTx(batchSize * 2)
  }

  def getCsvIterator(url: URL): Iterator[Array[String]] =
    if (first) {
      first = false
      val iterator = new LoadCsvIterator(url, resources.getCsvIterator(url))(
        maybeCommitAndRestartTx(batchSize)
      )
      it = Some(iterator)
      iterator
    } else {
      resources.getCsvIterator(url)
    }

  private def maybeCommitAndRestartTx(max: Long) {
    if (updates >= max) {
      queryContext.commitAndRestartTx()
      updates = 0
      it.foreach(_.notifyCommit())
    }
  }

  def apply(e: CypherException): CypherException =
  it match {
    case Some(iterator) => new LoadCsvStatusWrapCypherException(iterator.msg, e)
    case _ => e
  }
}

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
    val maybeReadAllFileMsg: String = if (readAll) " (which is the last row in the file)" else ""

    s"Failure when processing url '${url}' on line ${lastProcessed}${maybeReadAllFileMsg}. " +
    s"Possibly the last row committed during import is line ${lastCommitted}. " +
    s"Note that this information might not be accurate."
  }
}
