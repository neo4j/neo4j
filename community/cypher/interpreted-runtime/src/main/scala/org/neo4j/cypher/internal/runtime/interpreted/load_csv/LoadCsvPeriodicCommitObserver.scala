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

import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.CSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ExternalCSVResource, LoadCsvIterator}

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

    val csvResources = new ArrayBuffer[CSVResource]()
    queryContext.resources.allResources.foreach {
      case csvResource: CSVResource =>
        //save so that we can remove and re-add them
        csvResources += csvResource
      case e =>
        // We call closeInternal instead of close, so that the resources are not removed from the ResourceManager.
        // We want that, because they are still traced by the RuntimeResult and will be closed from there as well.
        e.closeInternal()
    }
    // Remove before we restart the tx
    csvResources.foreach(queryContext.resources.untrace)
    // Restart TX
    queryContext.transactionalContext.commitAndRestartTx()
    // Add back
    csvResources.foreach(queryContext.resources.trace)
    outerLoadCSVIterator.foreach(_.notifyCommit())
  }
}
