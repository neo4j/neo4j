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
package org.neo4j.cypher

import java.lang.Boolean.TRUE
import java.util.Optional

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.security.AuthManager

import scala.collection.Map

abstract class CommunityAdministrationCommandAcceptanceTestBase extends ExecutionEngineFunSuite with GraphDatabaseTestSupport {

  def authManager: AuthManager = graph.getDependencyResolver.resolveDependency(classOf[AuthManager])

  override def databaseConfig(): Map[Setting[_], Object] = Map(GraphDatabaseSettings.auth_enabled -> TRUE)

  def selectDatabase(name: String): Unit = {
    graphOps = managementService.database(name)
    graph = new GraphDatabaseCypherService(graphOps)
    eengine = ExecutionEngineHelper.createEngine(graph)
  }

  def assertFailure(command: String, errorMsg: String): Unit = {
    the[Exception] thrownBy {
      // WHEN
      execute(command)
      // THEN
    } should have message errorMsg
  }
}
