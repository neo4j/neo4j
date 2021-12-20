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
package org.neo4j.cypher.testing.impl

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.configuration.connectors.HttpConnector
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.cypher.testing.api.CypherExecutorFactory
import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.cypher.testing.impl.driver.DriverCypherExecutorFactory
import org.neo4j.cypher.testing.impl.embedded.EmbeddedCypherExecutorFactory
import org.neo4j.cypher.testing.impl.http.HttpCypherExecutorFactory
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.QueryExecutionEngine

object FeatureDatabaseManagementService {

  trait TestBase {
    def dbms: FeatureDatabaseManagementService
    def createBackingDbms(config: Config): DatabaseManagementService
    def baseConfig: Config.Builder = Config.newBuilder()
    def testApiKind: TestApiKind
  }

  sealed trait TestApiKind
  object TestApiKind {
    case object Bolt extends TestApiKind
    case object Embedded extends TestApiKind
    case object Http extends TestApiKind
  }

  trait TestUsingBolt extends TestBase {
    override val testApiKind: TestApiKind = TestApiKind.Bolt
    override def dbms: FeatureDatabaseManagementService = {
      val config = baseConfig
        .set(BoltConnector.enabled, java.lang.Boolean.TRUE)
        .set(BoltConnector.listen_address, new SocketAddress("localhost", 0))
        .build()
      val managementService = createBackingDbms(config)
      val executorFactory = DriverCypherExecutorFactory(managementService, config)
      FeatureDatabaseManagementService(managementService, executorFactory)
    }
  }

  trait TestUsingEmbedded extends TestBase {
    override val testApiKind: TestApiKind = TestApiKind.Embedded
    override def dbms: FeatureDatabaseManagementService = {
      val config = baseConfig.build()
      val managementService = createBackingDbms(config)
      val executorFactory = EmbeddedCypherExecutorFactory(managementService, config)
      FeatureDatabaseManagementService(managementService, executorFactory)
    }
  }

  trait TestUsingHttp extends TestBase {
    override val testApiKind: TestApiKind = TestApiKind.Http
    override def dbms: FeatureDatabaseManagementService = {
      val config = baseConfig
        .set(HttpConnector.enabled, java.lang.Boolean.TRUE)
        .set(HttpConnector.listen_address, new SocketAddress("localhost", 0))
        .build()
      val managementService = createBackingDbms(config)
      val executorFactory = HttpCypherExecutorFactory(managementService, config)
      FeatureDatabaseManagementService(managementService, executorFactory)
    }
  }
}

case class FeatureDatabaseManagementService(private val databaseManagementService: DatabaseManagementService,
                                            private val executorFactory: CypherExecutorFactory,
                                            private val databaseName: Option[String] = None) {

  private val database: GraphDatabaseFacade =
    databaseManagementService.database(databaseName.getOrElse(DEFAULT_DATABASE_NAME)).asInstanceOf[GraphDatabaseFacade]

  private val cypherExecutor = databaseName match {
    case Some(name) => executorFactory.executor(name)
    case None       => executorFactory.executor()
  }

  private lazy val kernel = database.getDependencyResolver.resolveDependency(classOf[Kernel])
  private lazy val globalProcedures = database.getDependencyResolver.provideDependency(classOf[GlobalProcedures]).get()
  private lazy val executionEngine = database.getDependencyResolver.resolveDependency(classOf[QueryExecutionEngine])

  def registerProcedure(procedure: BasicProcedure): Unit = kernel.registerProcedure(procedure)

  def registerProcedure(procedure: Class[_]): Unit = globalProcedures.registerProcedure(procedure)

  def clearQueryCaches(): Unit = executionEngine.clearQueryCaches()

  def begin(): CypherExecutorTransaction = cypherExecutor.beginTransaction()

  def execute[T](statement: String, parameters: Map[String, Object], converter: StatementResult => T): T =
    cypherExecutor.execute(statement, parameters, converter)

  def shutdown(): Unit = {
    cypherExecutor.close()
    executorFactory.close()
    databaseManagementService.shutdown()
  }

}
