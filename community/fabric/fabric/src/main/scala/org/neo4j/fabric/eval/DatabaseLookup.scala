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

import org.neo4j.fabric.FabricDatabaseManager
import org.neo4j.fabric.eval.DatabaseLookup.DatabaseClassifier
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName

import scala.collection.SortedSet
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.jdk.OptionConverters.RichOptional

trait DatabaseLookup {

  /**
    * Returns all known databases references.
    *
    * Note: returned set is sorted lexicographically by the reference's alias, to provide stable iteration order.
    */
  def databaseReferences: SortedSet[DatabaseReference]

  def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId]

  def databaseClassifier: DatabaseClassifier
}

object DatabaseLookup {

  implicit val databaseNameOrdering: Ordering[NormalizedDatabaseName] = Ordering.by(_.name)
  implicit val databaseIdOrdering: Ordering[NamedDatabaseId] = Ordering.by(_.name)

  trait DatabaseClassifier {
    def isVirtualDatabase(databaseId: NamedDatabaseId): Boolean
  }

  private def defaultDatabaseClassifier(databaseReferenceRepo: DatabaseReferenceRepository): DatabaseClassifier =
    (namedDatabaseId: NamedDatabaseId) =>
      databaseReferenceRepo.getCompositeDatabaseReferences.asScala
        .map(_.databaseId())
        .contains(namedDatabaseId)

  class Default(
    databaseReferenceRepo: DatabaseReferenceRepository,
    classifier: DatabaseClassifier
  ) extends DatabaseLookup {

    def this(databaseReferenceRepo: DatabaseReferenceRepository) =
      this(databaseReferenceRepo, defaultDatabaseClassifier(databaseReferenceRepo))

    def this(fabricDatabaseManager: FabricDatabaseManager) =
      this(
        fabricDatabaseManager.databaseReferenceRepository(),
        (databaseId: NamedDatabaseId) => fabricDatabaseManager.isFabricDatabase(databaseId)
      )

    def databaseReferences: SortedSet[DatabaseReference] = {
      val unsortedSet = databaseReferenceRepo.getAllDatabaseReferences.asScala
      SortedSet.empty[DatabaseReference] ++ unsortedSet
    }

    def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] = {
      databaseReferenceRepo.getByAlias(databaseName).toScala.collect {
        case ref: DatabaseReferenceImpl.Internal => ref.databaseId
      }
    }

    def databaseClassifier: DatabaseClassifier = classifier
  }
}
