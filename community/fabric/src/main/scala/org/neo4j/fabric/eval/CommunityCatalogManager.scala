/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.fabric.eval

import org.neo4j.configuration.helpers.NormalizedGraphName
import org.neo4j.fabric.eval.Catalog.InternalGraph
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListenerAdapter
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners

class CommunityCatalogManager(databaseLookup: DatabaseLookup, txListeners: GlobalTransactionEventListeners) extends CatalogManager {

  private val invalidationLock = new Object()
  @volatile private var cachedCatalog: Catalog = _
  @volatile private var invalidationToken: Object = _

  registerCatalogInvalidateListeners()

  override def registerCatalogInvalidateListeners(): Unit = {
    txListeners.registerTransactionEventListener( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.name(), new TransactionEventListenerAdapter[AnyRef] {
      override def afterCommit(data: TransactionData, state: AnyRef, databaseService: GraphDatabaseService): Unit =
        invalidateCatalog()
    })
  }

  override final def currentCatalog(): Catalog = {
    val existingCatalog = cachedCatalog
    if (existingCatalog != null) {
      return existingCatalog
    }

    // There is a race between catalog construction and invalidation.
    // The 'dark' scenario is caching a stale catalog, which can happen
    // when another invalidation comes while a catalog is being constructed.
    // Therefore a newly constructed catalog is cached only
    // when the invalidation state represented by the invalidation token
    // is the same as when the catalog construction started.
    val invalidationTokenAtConstructionStart = invalidationToken
    val newCatalog = createCatalog()

    if (invalidationToken == invalidationTokenAtConstructionStart) {
      invalidationLock.synchronized {
        if (invalidationToken == invalidationTokenAtConstructionStart) {
          cachedCatalog = newCatalog
        }
      }
    }

    newCatalog
  }

  final def invalidateCatalog(): Unit = {
    invalidationLock.synchronized {
      invalidationToken = new Object()
      cachedCatalog = null
    }
  }
  
  protected def createCatalog(): Catalog = Catalog.create(asInternal().toSeq, Seq.empty, None)

  protected def asInternal(firstId: Long = 0) = for {
    ((databaseName, databaseId), idx) <- databaseLookup.databaseReferences.zip(Stream.iterate(firstId)(_ + 1))
    graphName = new NormalizedGraphName(databaseName.name)
  } yield InternalGraph(idx, databaseId.databaseId().uuid(), graphName, databaseName)

  override def locationOf(sessionDatabase: NamedDatabaseId, graph: Catalog.Graph, requireWritable: Boolean, canRoute: Boolean): Location = graph match {
    case Catalog.InternalGraph(id, uuid, _, databaseName) =>
      new Location.Local(id, uuid, databaseName.name())
    case _ => throw new IllegalArgumentException( s"Unexpected graph type $graph" )
  }
}
