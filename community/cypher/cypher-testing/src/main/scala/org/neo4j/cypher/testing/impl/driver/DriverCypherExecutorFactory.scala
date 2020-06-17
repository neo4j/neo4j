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
package org.neo4j.cypher.testing.impl.driver

import java.net.URI

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.cypher.testing.api.CypherExecutor
import org.neo4j.cypher.testing.api.CypherExecutorFactory
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.kernel.internal.GraphDatabaseAPI

case class DriverCypherExecutorFactory(private val databaseManagementService: DatabaseManagementService, private val config: Config) extends CypherExecutorFactory {

  private val driver: Driver = {
    val connectorPortRegister =
      databaseManagementService.database(config.get(GraphDatabaseSettings.default_database)).asInstanceOf[GraphDatabaseAPI]
        .getDependencyResolver.resolveDependency(classOf[ConnectorPortRegister])

    val boltURI =
      if (config.get(BoltConnector.enabled)) URI.create(s"bolt://${connectorPortRegister.getLocalAddress(BoltConnector.NAME)}/")
      else throw new IllegalStateException("Bolt connector is not configured")

    GraphDatabase.driver(boltURI)
  }

  override def executor(): CypherExecutor = DriverCypherExecutor(driver.session())

  override def executor(databaseName: String): CypherExecutor =
    DriverCypherExecutor(driver.session(SessionConfig.forDatabase(databaseName)))

  override def close(): Unit = driver.close()
}
