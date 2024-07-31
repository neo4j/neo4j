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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.RuntimeContextManager
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.spec.Edition.Dbms
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.EphemeralFileSystemAbstraction
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.InternalLogProvider
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import java.lang.Boolean.TRUE

import scala.jdk.CollectionConverters.MapHasAsJava

trait RuntimeContextManagerFactory[CONTEXT <: RuntimeContext] {

  def newRuntimeContextManager(
    cypherRuntimeConfiguration: CypherRuntimeConfiguration,
    dependencyResolver: DependencyResolver,
    lifeSupport: LifeSupport,
    logProvider: InternalLogProvider
  ): RuntimeContextManager[CONTEXT]
}

class Edition[CONTEXT <: RuntimeContext](
  graphBuilderFactory: () => TestDatabaseManagementServiceBuilder,
  runtimeContextManagerFactory: RuntimeContextManagerFactory[CONTEXT],
  val runtimeTestUtils: RuntimeTestUtils,
  val isSpd: Boolean,
  val configs: (Setting[_], Object)*
) {

  def newGraphManagementService(logProvider: InternalLogProvider, additionalConfigs: (Setting[_], Object)*): Dbms = {
    val fileSystem = new EphemeralFileSystemAbstraction
    val graphBuilder = graphBuilderFactory().setFileSystem(fileSystem).setInternalLogProvider(logProvider)
    configs.foreach {
      case (setting, value) => graphBuilder.setConfig(setting.asInstanceOf[Setting[Object]], value.asInstanceOf[Object])
    }
    additionalConfigs.foreach {
      case (setting, value) => graphBuilder.setConfig(setting.asInstanceOf[Setting[Object]], value.asInstanceOf[Object])
    }
    Dbms(graphBuilder.build(), fileSystem)
  }

  def copyWithSpdEnabled(): Edition[CONTEXT] = {
    new Edition(graphBuilderFactory, runtimeContextManagerFactory, runtimeTestUtils, isSpd = true, configs: _*)
  }

  def copyWith(additionalConfigs: (Setting[_], Object)*): Edition[CONTEXT] = {
    val newConfigs = (configs ++ additionalConfigs).toMap
    new Edition(graphBuilderFactory, runtimeContextManagerFactory, runtimeTestUtils, isSpd, newConfigs.toSeq: _*)
  }

  def copyWith(
    newRuntimeContextManagerFactory: RuntimeContextManagerFactory[CONTEXT],
    additionalConfigs: (Setting[_], Object)*
  ): Edition[CONTEXT] = {
    val newConfigs = (configs ++ additionalConfigs).toMap
    new Edition(graphBuilderFactory, newRuntimeContextManagerFactory, runtimeTestUtils, isSpd, newConfigs.toSeq: _*)
  }

  def copyWith(
    newGraphBuilderFactory: () => TestDatabaseManagementServiceBuilder
  ): Edition[CONTEXT] = {
    new Edition(newGraphBuilderFactory, runtimeContextManagerFactory, runtimeTestUtils, isSpd, configs: _*)
  }

  def getSetting[T](setting: Setting[T]): Option[T] = {
    configs.collectFirst { case (key, value) if key == setting => value.asInstanceOf[T] }
  }

  def newRuntimeContextManager(
    resolver: DependencyResolver,
    lifeSupport: LifeSupport,
    logProvider: InternalLogProvider
  ): RuntimeContextManager[CONTEXT] =
    runtimeContextManagerFactory.newRuntimeContextManager(runtimeConfig, resolver, lifeSupport, logProvider)

  def runtimeConfig: CypherRuntimeConfiguration = {
    CypherRuntimeConfiguration.fromCypherConfiguration(cypherConfig)
  }

  def cypherConfig: CypherConfiguration = {
    val config = Config.defaults(configs.toMap.asJava)
    CypherConfiguration.fromConfig(config)
  }
}

object Edition {
  case class Dbms(dbms: DatabaseManagementService, filesystem: EphemeralFileSystemAbstraction)
}

object COMMUNITY {

  val EDITION = new Edition(
    () => new TestDatabaseManagementServiceBuilder,
    (runtimeConfig, _, _, logProvider) => CommunityRuntimeContextManager(logProvider.getLog("test"), runtimeConfig),
    CommunityRuntimeTestUtils,
    isSpd = false,
    GraphDatabaseSettings.cypher_hints_error -> TRUE
  )
}
