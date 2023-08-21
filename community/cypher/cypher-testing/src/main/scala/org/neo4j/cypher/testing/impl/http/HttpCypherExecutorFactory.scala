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
package org.neo4j.cypher.testing.impl.http

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.ConnectorPortRegister
import org.neo4j.configuration.connectors.ConnectorType
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.cypher.testing.api.CypherExecutor
import org.neo4j.cypher.testing.api.CypherExecutorFactory
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.test.server.HTTP

import java.net.URI

case class HttpCypherExecutorFactory(
  private val databaseManagementService: DatabaseManagementService,
  private val config: Config
) extends CypherExecutorFactory {

  private val http: HTTP.Builder = {
    val connectorPortRegister =
      databaseManagementService.database(config.get(GraphDatabaseSettings.initial_default_database)).asInstanceOf[
        GraphDatabaseAPI
      ]
        .getDependencyResolver.resolveDependency(classOf[ConnectorPortRegister])

    val uri =
      if (config.get(HttpConnector.enabled))
        URI.create(s"http://${connectorPortRegister.getLocalAddress(ConnectorType.HTTP)}/")
      else throw new IllegalStateException("HTTP connector is not configured")

    HTTP.withBaseUri(uri)
  }

  override def executor(): CypherExecutor =
    executor("neo4j")

  override def executor(databaseName: String): CypherExecutor =
    HttpCypherExecutor(http.withAppendedUri(s"db/$databaseName/"))

  override def close(): Unit = ()

}
