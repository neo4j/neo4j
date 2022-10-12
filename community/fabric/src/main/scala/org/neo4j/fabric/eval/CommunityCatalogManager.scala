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
import org.neo4j.fabric.eval.Catalog.Composite
import org.neo4j.fabric.eval.Catalog.ExternalAlias
import org.neo4j.fabric.eval.Catalog.Graph
import org.neo4j.fabric.eval.Catalog.InternalAlias
import org.neo4j.fabric.eval.Catalog.InternalGraph
import org.neo4j.fabric.eval.Catalog.NamespacedGraph
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListener
import org.neo4j.graphdb.event.TransactionEventListenerAdapter
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners

import scala.jdk.CollectionConverters.ListHasAsScala

class CommunityCatalogManager(databaseLookup: DatabaseLookup)
    extends CatalogManager {

  private val invalidationLock = new Object()
  @volatile private var cachedCatalog: Catalog = _
  @volatile private var invalidationToken: Object = _

  val catalogInvalidator: TransactionEventListener[AnyRef] = new TransactionEventListenerAdapter[AnyRef] {

    override def afterCommit(data: TransactionData, state: AnyRef, databaseService: GraphDatabaseService): Unit =
      invalidateCatalog()
  }

  final override def currentCatalog(): Catalog = {
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
    val idProvider = new IdProvider
    val internals = getInternals(idProvider)
    val aliases = getAliases(idProvider)
    val composites = getComposites(idProvider)
    Catalog.create(internals, Seq.empty, aliases, composites, None)
  }

  protected def getInternals(ids: IdProvider): Seq[InternalGraph] = {
    val references = databaseLookup.databaseReferences.toSeq
    val primaryRefs = references.collect {
      case int: DatabaseReference.Internal if int.isPrimary => int
    }
    for {
      (ref, idx) <- primaryRefs.zip(ids.sequence)
      databaseId = ref.databaseId
      graphName = new NormalizedGraphName(databaseId.name)
      databaseName = new NormalizedDatabaseName(databaseId.name)
    } yield InternalGraph(idx, databaseId.databaseId.uuid, graphName, databaseName)
  }

  protected def getAliases(ids: IdProvider): Seq[Graph] = {
    val references = databaseLookup.databaseReferences.toSeq
    val nonPrimaryRefs = references.collect {
      case int: DatabaseReference.Internal if !int.isPrimary => int
      case ext: DatabaseReference.External if !ext.isPrimary => ext
    }
    for {
      (ref, idx) <- nonPrimaryRefs.zip(ids.sequence)
      alias <- aliasFactory(ref, idx, None)
    } yield alias
  }

  protected def getComposites(ids: IdProvider): Seq[(Composite, Seq[Graph])] = {
    val references = databaseLookup.databaseReferences.toSeq
    val compositeRefs = references.collect {
      case comp: DatabaseReference.Composite => comp
    }
    for {
      (compositeRef, idx) <- compositeRefs.zip(ids.sequence)
      compositeAliases = for {
        (componentRef, idx) <- compositeRef.constituents.asScala.toSeq.zip(ids.sequence)
        alias <- aliasFactory(componentRef, idx, Some(compositeRef.databaseId().name()))
      } yield NamespacedGraph(compositeRef.alias().name(), alias)
    } yield (
      Composite(idx, compositeRef.databaseId.databaseId.uuid, databaseName(compositeRef.databaseId)),
      compositeAliases
    )

  }

  private def aliasFactory(ref: DatabaseReference, idx: Long, namespace: Option[String]): Option[Alias] = ref match {
    case i: DatabaseReference.Internal if i.isPrimary =>
      None // ignore primary aliases
    case i: DatabaseReference.Internal =>
      Some(InternalAlias(
        idx,
        i.databaseId.databaseId.uuid,
        graphName(i.alias()),
        graphName(namespace),
        databaseName(i.databaseId())
      ))
    case e: DatabaseReference.External =>
      Some(ExternalAlias(idx, e.id, graphName(e.alias), graphName(namespace), e.alias, e.targetAlias, e.externalUri))
    case other =>
      None // ignore unexpected reference types
  }

  private def graphName(databaseName: Option[String]): Option[NormalizedGraphName] =
    databaseName.map(new NormalizedGraphName(_))

  private def graphName(databaseName: NormalizedDatabaseName): NormalizedGraphName =
    new NormalizedGraphName(databaseName.name)

  private def databaseName(databaseId: NamedDatabaseId): NormalizedDatabaseName =
    new NormalizedDatabaseName(databaseId.name)

  override def locationOf(
    sessionDatabase: DatabaseReference,
    graph: Catalog.Graph,
    requireWritable: Boolean,
    canRoute: Boolean
  ): Location = graph match {
    case Catalog.InternalGraph(id, uuid, _, databaseName) =>
      new Location.Local(id, uuid, databaseName.name())
    case Catalog.InternalAlias(id, uuid, _, _, databaseName) =>
      new Location.Local(id, uuid, databaseName.name())
    case _ => throw new IllegalArgumentException(s"Unexpected graph type $graph")
  }
}

class IdProvider(startingFrom: Long = 0) {
  private var next: Long = startingFrom

  def getAndIncrement(): Long = {
    val value = next
    next = next + 1L
    value
  }

  def sequence: IterableOnce[Long] = new IterableOnce[Long] {
    override def iterator: Iterator[Long] = Iterator.continually(getAndIncrement())
  }
}
