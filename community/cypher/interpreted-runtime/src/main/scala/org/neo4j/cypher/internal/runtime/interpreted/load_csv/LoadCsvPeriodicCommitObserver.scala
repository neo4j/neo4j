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

import java.net.URL

import org.neo4j.cypher.internal.runtime.ExpressionCursors
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCsvIterator
import org.neo4j.internal.kernel.api.AutoCloseablePlus
import org.neo4j.internal.kernel.api.Cursor

import scala.collection.mutable.ArrayBuffer

class LoadCsvPeriodicCommitObserver(batchRowCount: Long, resources: ExternalCSVResource, queryContext: QueryContext)
  extends ExternalCSVResource {

  val updateCounter = new UpdateCounter
  var outerLoadCSVIterator: Option[LoadCsvIteratorWithPeriodicCommit] = None

  override def getCsvIterator(url: URL, fieldTerminator: Option[String], legacyCsvQuoteEscaping: Boolean, bufferSize: Int,
                              headers: Boolean = false): LoadCsvIterator = {
    val innerIterator = resources.getCsvIterator(url, fieldTerminator, legacyCsvQuoteEscaping, bufferSize, headers)
    if (outerLoadCSVIterator.isEmpty) {
      if (headers)
        updateCounter.offsetForHeaders()
      val iterator = new LoadCsvIteratorWithPeriodicCommit(innerIterator)(onNext())
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
    //This is horrible, we need to close things such as expression cursors but we don't want to close
    //the URL that we are reading the CSV from until we are all done
    val trackedResources = new ArrayBuffer[AutoCloseablePlus]()
    queryContext.resources.allResources.foreach {
      // Cursors are tied to transaction and needs to be closed here.
      // We call closeInternal instead of close, so that the resources are not removed from the ResourceManager.
      // We want that, because they are still traced by the RuntimeResult and will be closed from there as well.
      case c: Cursor => c.closeInternal()
      case ec: ExpressionCursors => ec.closeInternal()
      case e =>
        //save so that we can remove and re-add them
        trackedResources += e
    }
    // Remove before we restart the tx
    trackedResources.foreach(queryContext.resources.untrace)
    // Restart TX
    queryContext.transactionalContext.commitAndRestartTx()
    // Add back
    trackedResources.foreach(queryContext.resources.trace)
    outerLoadCSVIterator.foreach(_.notifyCommit())
  }
}
