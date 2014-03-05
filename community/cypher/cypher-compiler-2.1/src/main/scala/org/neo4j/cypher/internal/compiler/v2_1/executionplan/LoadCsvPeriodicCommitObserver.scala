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

  val updateCounter = new UpdateCounter
  var loadCsvIterator: Option[LoadCsvIterator] = None

  def notify(increment: Long) {
    updateCounter += increment
    onNotify()
  }

  def getCsvIterator(url: URL, fieldTerminator: Option[String] = None): Iterator[Array[String]] = {
    val innerIterator = resources.getCsvIterator(url, fieldTerminator)
    loadCsvIterator match {
      case Some(_) =>
        innerIterator
      case None =>
        val iterator = new LoadCsvIterator(url, innerIterator)(onNext())
        loadCsvIterator = Some(iterator)
        iterator
    }
  }

  private def onNotify() {
    updateCounter.resetIfPastLimit(batchSize * 2)(commitAndRestartTx())
  }

  private def onNext() {
    updateCounter.resetIfPastLimit(batchSize)(commitAndRestartTx())
  }

  private def commitAndRestartTx() {
      queryContext.commitAndRestartTx()
      loadCsvIterator.foreach(_.notifyCommit())
  }

  def apply(e: CypherException): CypherException = loadCsvIterator match {
    case Some(iterator) => new LoadCsvStatusWrapCypherException(iterator.msg, e)
    case _ => e
  }
}
