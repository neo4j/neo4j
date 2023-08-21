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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.cypher.testing.api.CypherExecutor
import org.neo4j.cypher.testing.api.CypherExecutorFactory
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.driver.AuthToken
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.NotificationConfig
import org.neo4j.driver.SessionConfig
import org.neo4j.kernel.internal.GraphDatabaseAPI

import java.net.URI

case class DriverCypherExecutorFactory(
  private val databaseManagementService: DatabaseManagementService,
  private val config: Config,
  token: Option[AuthToken] = None
) extends CypherExecutorFactory {

  private var notificationConfig = NotificationConfig.defaultConfig()

  val driver: Driver = {
    val connectorPortRegister =
      databaseManagementService.database(config.get(GraphDatabaseSettings.initial_default_database)).asInstanceOf[
        GraphDatabaseAPI
      ]
        .getDependencyResolver.resolveDependency(classOf[ConnectorPortRegister])

    val boltURI =
      if (config.get(BoltConnector.enabled))
        URI.create(s"neo4j://${connectorPortRegister.getLocalAddress(ConnectorType.BOLT)}/")
      else throw new IllegalStateException("Bolt connector is not configured")

    token.map(t => GraphDatabase.driver(boltURI, t)).getOrElse(GraphDatabase.driver(boltURI))
  }

  def setNotificationConfig(config: NotificationConfig): Unit =
    notificationConfig = config

  override def executor(): CypherExecutor = {
    DriverCypherExecutor(driver.session(SessionConfig.builder().withNotificationConfig(notificationConfig).build()))
  }

  override def executor(databaseName: String): CypherExecutor =
    DriverCypherExecutor(driver.session(
      SessionConfig.builder().withDatabase(databaseName).withNotificationConfig(notificationConfig).build()
    ))

  override def close(): Unit = driver.close()
}
