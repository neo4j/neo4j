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

class LoadCsvPeriodicCommitObserver(batchSize: Long, resources: ExternalResource, queryContext: QueryContext)
  extends UpdateObserver with ExternalResource {

  var updates: Long = 0
  var first = true

  def notify(increment: Long) {
    updates += increment
    maybeCommitAndRestartTx(batchSize * 2)
  }

  def getCsvIterator(url: URL): Iterator[Array[String]] = if (first) {
    first = false

    resources.getCsvIterator(url).map {
      csvRow =>
        maybeCommitAndRestartTx(batchSize)
        csvRow
    }

  } else resources.getCsvIterator(url)

  private def maybeCommitAndRestartTx(max: Long) {
    if (updates > max) {
      queryContext.commitAndRestartTx()
      updates = 0
    }
  }

}

