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
package cypher.features

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.mutable
import scala.jdk.CollectionConverters.MapHasAsJava

class TestDatabaseProvider(dbBuilder: () => TestDatabaseManagementServiceBuilder) {

  private val managementServices: ThreadLocal[mutable.Map[Map[Setting[_], Object], DatabaseManagementService]] =
    ThreadLocal.withInitial(() => mutable.Map.empty)

  private val testDbms: CopyOnWriteArrayList[DatabaseManagementService] =
    new CopyOnWriteArrayList[DatabaseManagementService]

  def get(config: Map[Setting[_], Object]): DatabaseManagementService = {
    this.synchronized {
      val servicesByConfig = managementServices.get()
      servicesByConfig.getOrElseUpdate(config, createManagementService(config, dbBuilder))
    }
  }

  private def createManagementService(
    config: collection.Map[Setting[_], Object],
    graphDatabaseFactory: () => TestDatabaseManagementServiceBuilder
  ) = {
    val dbms = graphDatabaseFactory.apply().impermanent().setConfig(config.asJava).build()
    testDbms.add(dbms)
    dbms
  }

  def close(): Unit = {
    testDbms.parallelStream().forEach(dbms => dbms.shutdown())
  }
}
