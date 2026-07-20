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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.CommunityShowFuncProcAcceptanceTest.readAll
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.api.procedure.GlobalProcedures

import java.nio.file.NoSuchFileException

class CommunityCombineCommandsAcceptanceTestBase extends TransactionCommandAcceptanceTestSupport
    with ShowSettingsAcceptanceTestSupport {

  override def databaseConfig(): Map[Setting[?], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.default_language -> GraphDatabaseSettings.CypherVersion.Cypher25
  )

  override protected def onNewGraphDatabase(): Unit = {
    super.onNewGraphDatabase()
    val globalProcedures: GlobalProcedures = graph.getDependencyResolver.resolveDependency(classOf[GlobalProcedures])
    globalProcedures.registerFunction(classOf[TestShowFunction])
    globalProcedures.registerAggregationFunction(classOf[TestShowFunction])
  }

  // Functions

  private val funcResourceUrl = getClass.getResource("/builtInFunctions.json")
  if (funcResourceUrl == null) throw new NoSuchFileException(s"File not found: builtInFunctions.json")

  protected val builtInFunctionsNames: List[String] =
    readAll(funcResourceUrl)
      .filterNot(m => m.getOrElse("enterpriseOnly", false).asInstanceOf[Boolean])
      .filter(m => m("cypherVersionScope").asInstanceOf[List[Int]].contains(25))
      .map(m => m("name").asInstanceOf[String])

  protected val userDefinedFunctionsNames: List[String] =
    List("test.function", "test.functionWithInput", "test.return.latest")

  protected val allFunctionsNames: List[String] =
    (builtInFunctionsNames ++ userDefinedFunctionsNames).sorted

  // Procedures

  private val procResourceUrl = getClass.getResource("/procedures.json")
  if (procResourceUrl == null) throw new NoSuchFileException(s"File not found: procedures.json")

  protected val allProceduresNames: List[String] =
    readAll(procResourceUrl)
      .filterNot(m => m("enterpriseOnly").asInstanceOf[Boolean])
      .filter(m => m("cypherVersionScope").asInstanceOf[List[Int]].contains(25))
      .map(m => m("name").asInstanceOf[String])

}
