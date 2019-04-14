/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.spec

import java.util

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.internal._
import org.neo4j.dbms.database.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.test.TestGraphDatabaseFactory

class Edition[CONTEXT <: RuntimeContext](graphDatabaseFactory: TestGraphDatabaseFactory,
                                         runtimeContextCreatorFun: (CypherRuntimeConfiguration, DependencyResolver) => RuntimeContextCreator[CONTEXT],
                                         configs: (Setting[_], String)*) {

  import scala.collection.JavaConverters._

  def newGraphManagementService(): DatabaseManagementService = {
    val graphBuilder = graphDatabaseFactory.newImpermanentDatabaseBuilder
    configs.foreach{
      case (setting, value) => graphBuilder.setConfig(setting, value)
    }
    graphBuilder.newDatabaseManagementService()
  }

  def copyWith(additionalConfigs: (Setting[_], String)*): Edition[CONTEXT] = {
    val newConfigs = configs ++ additionalConfigs
    new Edition(graphDatabaseFactory, runtimeContextCreatorFun, newConfigs: _*)
  }

  def runtimeContextCreator(resolver: DependencyResolver): RuntimeContextCreator[CONTEXT] =
    runtimeContextCreatorFun(runtimeConfig(), resolver)

  private def runtimeConfig() = {
    val javaConfigMap: util.Map[String, String] = configs.map { case (setting, value) => (setting.name(), value) }.toMap.asJava
    val config = Config.fromSettings(javaConfigMap).build()
    CypherConfiguration.fromConfig(config).toCypherRuntimeConfiguration
  }
}

object COMMUNITY {
  val EDITION = new Edition(
    new TestGraphDatabaseFactory,
    (runtimeConfig, _) => CommunityRuntimeContextCreator(runtimeConfig),
    GraphDatabaseSettings.cypher_hints_error -> "true")
}
