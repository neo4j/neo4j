/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.executionplan

import java.net.URL

import org.neo4j.cypher.internal.compiler.v3_0.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.compiler.v3_0.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_0.{CypherException, LoadCsvStatusWrapCypherException}

class LoadCsvPeriodicCommitObserver(batchRowCount: Long, resources: ExternalCSVResource, queryContext: QueryContext)
  extends ExternalCSVResource with ((CypherException) => CypherException) {

  val updateCounter = new UpdateCounter
  var outerLoadCSVIterator: Option[LoadCsvIterator] = None

  def getCsvIterator(url: URL, fieldTerminator: Option[String] = None): Iterator[Array[String]] = {
    val innerIterator = resources.getCsvIterator(url, fieldTerminator)
    if (outerLoadCSVIterator.isEmpty) {
      val iterator = new LoadCsvIterator(url, innerIterator)(onNext())
      outerLoadCSVIterator = Some(iterator)
      iterator
    } else {
      innerIterator
    }
  }

  private def onNext() {
    updateCounter += 1
    updateCounter.resetIfPastLimit(batchRowCount)(commitAndRestartTx())
  }

  private def commitAndRestartTx() {
    queryContext.transactionalContext.commitAndRestartTx()
    outerLoadCSVIterator.foreach(_.notifyCommit())
  }

  def apply(e: CypherException): CypherException = outerLoadCSVIterator match {
    case Some(iterator) => new LoadCsvStatusWrapCypherException(iterator.msg, e)
    case _ => e
  }
}
