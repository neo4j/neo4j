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
package org.neo4j.cypher.internal.runtime.spec

import java.lang.Boolean.TRUE

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.{Config, GraphDatabaseSettings}
import org.neo4j.cypher.internal._
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.LogProvider
import org.neo4j.test.TestDatabaseManagementServiceBuilder

class Edition[CONTEXT <: RuntimeContext](graphBuilderFactory: () => TestDatabaseManagementServiceBuilder,
                                         newRuntimeContextManager: (CypherRuntimeConfiguration, DependencyResolver, LifeSupport, LogProvider) => RuntimeContextManager[CONTEXT],
                                         configs: (Setting[_], Object)*) {

  import scala.collection.JavaConverters._

  def newGraphManagementService(): DatabaseManagementService = {
    val graphBuilder = graphBuilderFactory().impermanent
    configs.foreach {
      case (setting, value) => graphBuilder.setConfig(setting.asInstanceOf[Setting[Object]], value.asInstanceOf[Object])
    }
    graphBuilder.build()
  }

  def copyWith(additionalConfigs: (Setting[_], Object)*): Edition[CONTEXT] = {
    val newConfigs = configs ++ additionalConfigs
    new Edition(graphBuilderFactory, newRuntimeContextManager, newConfigs: _*)
  }

  def getSetting[T](setting: Setting[T]): Option[T] = {
    configs.collectFirst { case (key, value) if key == setting => value.asInstanceOf[T] }
  }

  def newRuntimeContextManager(resolver: DependencyResolver, lifeSupport: LifeSupport, logProvider: LogProvider): RuntimeContextManager[CONTEXT] =
    newRuntimeContextManager(runtimeConfig(), resolver, lifeSupport, logProvider)

  private def runtimeConfig(): CypherRuntimeConfiguration = {
    cypherConfig.toCypherRuntimeConfiguration
  }

  def cypherConfig(): CypherConfiguration = {
    val config = Config.defaults(configs.toMap.asJava)
    CypherConfiguration.fromConfig(config)
  }
}

object COMMUNITY {
  val EDITION = new Edition(
    () => new TestDatabaseManagementServiceBuilder,
    (runtimeConfig, _, _, logProvider) => CommunityRuntimeContextManager(logProvider.getLog("test"), runtimeConfig),
    GraphDatabaseSettings.cypher_hints_error -> TRUE)
}
