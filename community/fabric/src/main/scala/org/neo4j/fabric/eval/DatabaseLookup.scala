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

import org.neo4j.cypher.internal.administration.BaseDatabaseInfoMapper.RichOptional
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName

import scala.collection.JavaConverters.asScalaSetConverter
import scala.collection.SortedSet

trait DatabaseLookup {

  /**
   * Returns all known database references.
   *
   * Note: returned map is sorted lexicographically by reference's alias, to provide stable iteration order.
   */
  def databaseReferences: SortedSet[DatabaseReference]

  /**
   * Returns all known database Ids for this DBMS.
   *
   * Note: returned set is sorted lexicographically by `NamedDatabaseId#name`, to provide stable iteration order.
   */
  def databaseIds: SortedSet[NamedDatabaseId]

  def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId]
}

object DatabaseLookup {

  implicit val databaseNameOrdering: Ordering[NormalizedDatabaseName] = Ordering.by(_.name)
  implicit val databaseIdOrdering: Ordering[NamedDatabaseId] = Ordering.by(_.name)

  class Default(databaseRef: DatabaseReferenceRepository) extends DatabaseLookup {

    def databaseReferences: SortedSet[DatabaseReference] = {
      val unsortedSet = databaseRef.getAllDatabaseReferences.asScala
      SortedSet.empty[DatabaseReference] ++ unsortedSet
    }

    def databaseIds: SortedSet[NamedDatabaseId] = {
      val unsortedSet = databaseRef.getInternalDatabaseReferences.asScala.map(_.databaseId)
      SortedSet.empty[NamedDatabaseId] ++ unsortedSet
    }

    def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] =
      databaseRef.getByName(databaseName).asScala.collect {
        case ref: DatabaseReference.Internal => ref.databaseId
      }
  }
}
