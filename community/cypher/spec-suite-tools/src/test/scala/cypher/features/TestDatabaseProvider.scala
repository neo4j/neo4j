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
package cypher.features

import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters.MapHasAsJava

class TestDatabaseProvider(
  dbBuilder: () => TestDatabaseManagementServiceBuilder,
  onDbmsCreatedCallback: DatabaseManagementService => Unit
) {

  private val managementServices: ThreadLocal[Option[(Map[Setting[_], Object], DatabaseManagementService)]] =
    ThreadLocal.withInitial(() => None)

  private val testDbms: CopyOnWriteArrayList[DatabaseManagementService] =
    new CopyOnWriteArrayList[DatabaseManagementService]

  def get(config: Map[Setting[_], Object]): DatabaseManagementService = {
    managementServices.get() match {
      case Some((cachedConfig, cachedDbms)) if config == cachedConfig => cachedDbms
      case Some((_, oldDbms)) =>
        oldDbms.shutdown()
        testDbms.remove(oldDbms)
        createManagementService(config)
      case _ => createManagementService(config)
    }
  }

  private def createManagementService(config: Map[Setting[_], Object]): DatabaseManagementService = {
    val dbms = dbBuilder.apply().impermanent().setConfig(config.asJava).build()
    onDbmsCreatedCallback.apply(dbms)
    managementServices.set(Some(config -> dbms))
    testDbms.add(dbms)
    dbms
  }

  def close(): Unit = {
    testDbms.parallelStream().forEach(dbms => dbms.shutdown())
  }
}
