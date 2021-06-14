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

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.testing.api.CypherExecutorFactory
import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.api.Kernel
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade
import org.neo4j.kernel.impl.query.QueryExecutionEngine

case class FeatureDatabaseManagementService(private val databaseManagementService: DatabaseManagementService,
                                            private val executorFactory: CypherExecutorFactory,
                                            private val databaseName: Option[String] = None) {

  private val database: GraphDatabaseFacade =
    new GraphDatabaseCypherService(databaseManagementService.database(databaseName.getOrElse(DEFAULT_DATABASE_NAME))).getGraphDatabaseService

  private val cypherExecutor = databaseName match {
    case Some(name) => executorFactory.executor(name)
    case None => executorFactory.executor()
  }

  private lazy val kernel = database.getDependencyResolver.resolveDependency(classOf[Kernel])
  private lazy val globalProcedures = database.getDependencyResolver.provideDependency(classOf[GlobalProcedures]).get()
  private lazy val executionEngine = database.getDependencyResolver.resolveDependency(classOf[QueryExecutionEngine])

  def registerProcedure(procedure: BasicProcedure): Unit = kernel.registerProcedure(procedure)

  def registerProcedure(procedure: Class[_]): Unit = globalProcedures.registerProcedure(procedure)

  def clearQueryCaches(): Unit = executionEngine.clearQueryCaches()

  def begin(): CypherExecutorTransaction = cypherExecutor.begin()

  def shutdown(): Unit = {
    cypherExecutor.close()
    executorFactory.close()
    databaseManagementService.shutdown()
  }

}
