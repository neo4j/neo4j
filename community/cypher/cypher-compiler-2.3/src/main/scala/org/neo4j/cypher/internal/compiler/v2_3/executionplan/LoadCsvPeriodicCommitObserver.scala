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

import org.neo4j.cypher.internal.compiler.v2_3.pipes.ExternalResource
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v2_3.{CypherException, LoadCsvStatusWrapCypherException}

class LoadCsvPeriodicCommitObserver(batchRowCount: Long, resources: ExternalResource, queryContext: QueryContext)
  extends ExternalResource with ((CypherException) => CypherException) {

  val updateCounter = new UpdateCounter
  var outerLoadCSVIterator: Option[LoadCsvIterator] = None

  def getCsvIterator(url: URL, fieldTerminator: Option[String] = None, headers:Boolean=false): Iterator[Array[String]] = {
    val innerIterator = resources.getCsvIterator(url, fieldTerminator, headers)
    if (outerLoadCSVIterator.isEmpty) {
      if (headers)
        updateCounter.offsetForHeaders()
      val iterator = new LoadCsvIterator(url, innerIterator)(onNext())
      outerLoadCSVIterator = Some(iterator)
      iterator
    } else {
      innerIterator
    }
  }

  private def onNext() {
    updateCounter.resetIfPastLimit(batchRowCount)(commitAndRestartTx())
    updateCounter += 1
  }

  private def commitAndRestartTx() {
    queryContext.commitAndRestartTx()
    outerLoadCSVIterator.foreach(_.notifyCommit())
  }

  def apply(e: CypherException): CypherException = outerLoadCSVIterator match {
    case Some(iterator) => new LoadCsvStatusWrapCypherException(iterator.msg, e)
    case _ => e
  }
}
