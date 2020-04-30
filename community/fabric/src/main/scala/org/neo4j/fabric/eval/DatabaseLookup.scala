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
package org.neo4j.fabric.eval

import java.util.function.Supplier

import org.neo4j.configuration.helpers.NormalizedDatabaseName
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseManager
import org.neo4j.kernel.database.NamedDatabaseId

import scala.collection.JavaConverters.asScalaSetConverter

trait DatabaseLookup {

  def databaseIds: Set[NamedDatabaseId]

  def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId]
}

object DatabaseLookup {

  class Default(
    databaseManager: Supplier[DatabaseManager[DatabaseContext]],
  ) extends DatabaseLookup {

    def databaseIds: Set[NamedDatabaseId] =
      databaseManager.get().registeredDatabases().keySet().asScala.toSet

    def databaseId(databaseName: NormalizedDatabaseName): Option[NamedDatabaseId] = {
      val maybeDatabaseId = databaseManager.get().databaseIdRepository().getByName(databaseName)
      if (maybeDatabaseId.isPresent) Some(maybeDatabaseId.get) else None
    }
  }
}
