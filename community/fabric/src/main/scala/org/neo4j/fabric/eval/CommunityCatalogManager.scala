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
import org.neo4j.fabric.executor.Location
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.event.TransactionData
import org.neo4j.graphdb.event.TransactionEventListener
import org.neo4j.graphdb.event.TransactionEventListenerAdapter
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName

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
    val aliases = getAliases(idProvider)
    val composites = getComposites(idProvider)
    Catalog.create(aliases, composites)
  }

  protected def getAliases(ids: IdProvider): Seq[Graph] = {
    val references = databaseLookup.databaseReferences.toSeq.filter {
      case _: DatabaseReference.Composite => false
      case _                              => true
    }
    // Give low ids to primary aliases
    val (primary, nonPrimary) = references.partition(_.isPrimary)
    for {
      (ref, idx) <- (primary ++ nonPrimary).zip(ids.sequence)
      alias <- aliasFactory(ref, idx)
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
        alias <- aliasFactory(componentRef, idx)
      } yield alias
    } yield (
      Composite(idx, compositeRef),
      compositeAliases
    )
  }

  private def aliasFactory(ref: DatabaseReference, idx: Long): Option[Alias] = ref match {
    case ref: DatabaseReference.Internal =>
      Some(InternalAlias(idx, ref))
    case ref: DatabaseReference.External =>
      Some(ExternalAlias(idx, ref))
    case other =>
      None // ignore unexpected reference types
  }

  private def databaseName(databaseId: NamedDatabaseId): NormalizedDatabaseName =
    new NormalizedDatabaseName(databaseId.name)

  override def locationOf(
    sessionDatabase: DatabaseReference,
    graph: Catalog.Graph,
    requireWritable: Boolean,
    canRoute: Boolean
  ): Location = graph match {
    case i: Catalog.InternalAlias =>
      new Location.Local(i.id, i.reference)
    case _ => throw new IllegalArgumentException(s"Unexpected graph type $graph")
  }

  override def isVirtualDatabase(databaseId: NamedDatabaseId): Boolean = databaseLookup.isVirtualDatabase(databaseId)
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
