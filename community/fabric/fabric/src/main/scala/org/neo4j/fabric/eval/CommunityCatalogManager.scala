/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.eval

import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext
import org.neo4j.fabric.eval.Catalog.Alias
import org.neo4j.fabric.eval.Catalog.Composite
import org.neo4j.fabric.eval.Catalog.ExternalAlias
import org.neo4j.fabric.eval.Catalog.InternalAlias
import org.neo4j.fabric.executor.Location
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.storageengine.api.TransactionIdStore

import java.util.function.Supplier

import scala.jdk.CollectionConverters.ListHasAsScala

class CommunityCatalogManager(
  databaseLookup: DatabaseLookup,
  systemDbTransactionIdStoreSupplier: Supplier[TransactionIdStore]
) extends CatalogManager {

  private val cachedCatalogLock = new Object()
  @volatile private var cachedCatalog: Catalog = _
  @volatile private var cachedCatalogTxId: Long = 0

  @volatile private var systemDbTransactionIdStore: TransactionIdStore = _

  final override def currentCatalog(): Catalog = {
    val lastTxId = systemDbTransactionIdStore.getLastClosedTransactionId
    if (cachedCatalogTxId < lastTxId) {
      val newCatalog = createCatalog()
      cachedCatalogLock.synchronized {
        if (cachedCatalogTxId < lastTxId) {
          cachedCatalog = newCatalog
          cachedCatalogTxId = lastTxId
        }
      }
    }

    cachedCatalog
  }

  protected def createCatalog(): Catalog = {
    val idProvider = new IdProvider
    val aliases = getAliases(idProvider)
    val composites = getComposites(idProvider)
    Catalog.create(aliases, composites)
  }

  protected def getAliases(ids: IdProvider): Seq[Alias] = {
    val references = databaseLookup.databaseReferences.toSeq.filter {
      case _: DatabaseReferenceImpl.Composite => false
      case _                                  => true
    }
    // Give low ids to primary aliases
    val (primary, nonPrimary) = references.partition(_.isPrimary)
    for {
      (ref, idx) <- (primary ++ nonPrimary).zip(ids.sequence)
      alias <- aliasFactory(ref, idx)
    } yield alias
  }

  protected def getComposites(ids: IdProvider): Seq[(Composite, Seq[Alias])] = {
    val references = databaseLookup.databaseReferences.toSeq
    val compositeRefs = references.collect {
      case comp: DatabaseReferenceImpl.Composite => comp
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
    case ref: DatabaseReferenceImpl.Internal =>
      Some(InternalAlias(idx, ref))
    case ref: DatabaseReferenceImpl.External =>
      Some(ExternalAlias(idx, ref))
    case other =>
      None // ignore unexpected reference types
  }

  override def locationOf(
    sessionDatabase: DatabaseReference,
    graph: Catalog.Graph,
    requireWritable: Boolean,
    routingContext: RoutingContext
  ): Location = graph match {
    case i: Catalog.InternalAlias =>
      new Location.Local(i.id, i.reference)
    case _ => throw new IllegalArgumentException(s"Unexpected graph type $graph")
  }

  override def isVirtualDatabase(databaseId: NamedDatabaseId): Boolean =
    databaseLookup.databaseClassifier.isVirtualDatabase(databaseId)

  override def start(): Unit = {
    systemDbTransactionIdStore = systemDbTransactionIdStoreSupplier.get()
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
