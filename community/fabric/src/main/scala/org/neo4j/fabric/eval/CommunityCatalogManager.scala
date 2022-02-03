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
import org.neo4j.fabric.eval.Catalog.Alias
import org.neo4j.fabric.eval.Catalog.ExternalAlias
import org.neo4j.fabric.eval.Catalog.InternalAlias
import org.neo4j.fabric.eval.Catalog.InternalGraph
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListenerAdapter
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners

import java.util.UUID

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
  
  protected def createCatalog(): Catalog = {
    val internals = getInternals()
    val maxId = internals.map(_.id).reduceOption(_ max _).getOrElse(-1L)
    val aliases = getAliases(maxId+1)
    Catalog.create(internals.toSeq, Seq.empty, aliases.toSeq, None)
  }

  protected def getInternals(firstId: Long = 0) = for {
    (databaseId, idx) <- databaseLookup.databaseIds zip indicesFrom(firstId)
    graphName = new NormalizedGraphName(databaseId.name)
    databaseName = new NormalizedDatabaseName(databaseId.name)
  } yield InternalGraph(idx, databaseId.databaseId.uuid, graphName, databaseName)

  protected def getAliases(firstId: Long) = {
    val nonPrimaryRefs = databaseLookup.databaseReferences.toStream.filter(!_.isPrimary)
    val aliases = for {
      (ref, idx) <- nonPrimaryRefs zip indicesFrom(firstId)
      alias <- aliasFactory(ref, idx)
    } yield alias
    aliases.toSet
  }

  protected def aliasFactory(ref: DatabaseReference, idx: Long): Option[Alias] = ref match {
    case i: DatabaseReference.Internal if i.isPrimary => None //ignore primary aliases
    case i: DatabaseReference.Internal => Some(InternalAlias(idx, i.databaseId.databaseId.uuid, new NormalizedGraphName(i.alias.name), i.alias))
    case e: DatabaseReference.External => Some(ExternalAlias(idx, new UUID(idx,0), new NormalizedGraphName(e.alias.name), e.alias, e.remoteName, e.remoteUri))
    case other => None //ignore unexpected reference types
  }

  private def indicesFrom(firstId: Long) = Stream.iterate(firstId)(_ + 1)

  override def locationOf(sessionDatabase: DatabaseReference, graph: Catalog.Graph, requireWritable: Boolean, canRoute: Boolean): Location = graph match {
    case Catalog.InternalGraph(id, uuid, _, databaseName) =>
      new Location.Local(id, uuid, databaseName.name())
    case Catalog.InternalAlias(id, uuid, _, databaseName) =>
      new Location.Local(id, uuid, databaseName.name())
    case _ => throw new IllegalArgumentException( s"Unexpected graph type $graph" )
  }
}
