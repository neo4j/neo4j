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
package org.neo4j.cypher

import org.neo4j.configuration.Config
import org.neo4j.cypher.testing.api.CypherExecutorFactory
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.cypher.testing.impl.embedded.EmbeddedCypherExecutorFactory
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.test.TestDatabaseManagementServiceBuilder

class DeprecationEmbeddedAcceptanceTest extends DeprecationAcceptanceTestBase {

  private val config = Config.newBuilder()
    // Might need to be enabled when the next experimental version appear: .set(GraphDatabaseInternalSettings.enable_experimental_cypher_versions, java.lang.Boolean.TRUE)
    .build()

  private var managementService: DatabaseManagementService = _
  private var executorFactory: CypherExecutorFactory = _
  private var _dbms: FeatureDatabaseManagementService = _

  override protected def dbms: FeatureDatabaseManagementService = _dbms

  override def beforeAll(): Unit = {
    managementService = new TestDatabaseManagementServiceBuilder().impermanent.setConfig(config).build()
    executorFactory = EmbeddedCypherExecutorFactory(managementService, config)
    _dbms = FeatureDatabaseManagementService(managementService, executorFactory)
    super.beforeAll()
  }
}
