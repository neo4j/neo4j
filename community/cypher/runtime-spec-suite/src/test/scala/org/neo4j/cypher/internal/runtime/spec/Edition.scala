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
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.RuntimeContextManager
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.config.CypherConfiguration
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherQueryOptions
import org.neo4j.cypher.internal.planner.spi.IndexComparatorFactory
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.QueryRuntimeConfig
import org.neo4j.cypher.internal.runtime.spec.Edition.Dbms
import org.neo4j.cypher.internal.runtime.spec.Edition.SpdConfig
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.fs.EphemeralFileSystemAbstraction
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode
import org.neo4j.kernel.lifecycle.LifeSupport
import org.neo4j.logging.InternalLogProvider
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import java.lang.Boolean.TRUE
import java.time.Clock

trait TestRuntimeContextManager[+CONTEXT <: RuntimeContext] extends RuntimeContextManager[CONTEXT] {
  final val cypherConfig: CypherConfiguration = config.cypherConfiguration

  val defaultQueryOptions: QueryOptions = {
    val queryOptions = CypherQueryOptions.fromValues(cypherConfig, Set.empty)
    QueryOptions(
      offset = InputPosition.NONE,
      queryOptions = queryOptions,
      derivedOptions = CypherQueryOptions.derivedOptions(queryOptions, cypherConfig),
      defaultLanguage = CypherVersion.Legacy.legacyVersion()
    )
  }

  def defaultQueryRuntimeConfig: QueryRuntimeConfig = {
    QueryRuntimeConfig.createFrom(defaultQueryOptions.queryOptions, defaultQueryOptions.derivedOptions, cypherConfig)
  }

  def defaultExecutionModel: ExecutionModel
}

trait RuntimeContextManagerFactory[CONTEXT <: RuntimeContext] {

  def newRuntimeContextManager(
    cypherRuntimeConfiguration: CypherRuntimeConfiguration,
    dependencyResolver: DependencyResolver,
    lifeSupport: LifeSupport,
    logProvider: InternalLogProvider
  ): TestRuntimeContextManager[CONTEXT]
}

class Edition[CONTEXT <: RuntimeContext](
  graphBuilderFactory: () => TestDatabaseManagementServiceBuilder,
  runtimeContextManagerFactory: RuntimeContextManagerFactory[CONTEXT],
  val runtimeTestUtils: RuntimeTestUtils,
  val spd: Option[SpdConfig],
  val configs: (Setting[_], Object)*
) {

  def databaseMode: DatabaseMode = spd.map(_ => DatabaseMode.SHARDED).getOrElse(DatabaseMode.SINGLE)

  def newGraphManagementService(logProvider: InternalLogProvider, additionalConfigs: (Setting[_], Object)*): Dbms = {
    val fileSystem = new EphemeralFileSystemAbstraction
    val graphBuilder = graphBuilderFactory().setFileSystem(fileSystem).setInternalLogProvider(logProvider)
    configs.foreach {
      case (setting, value) => graphBuilder.setConfig(setting.asInstanceOf[Setting[Object]], value)
    }
    additionalConfigs.foreach {
      case (setting, value) => graphBuilder.setConfig(setting.asInstanceOf[Setting[Object]], value)
    }
    Dbms(graphBuilder.build(), fileSystem)
  }

  def copyWithSpdEnabled(spdConfig: SpdConfig): Edition[CONTEXT] = {
    new Edition(graphBuilderFactory, runtimeContextManagerFactory, runtimeTestUtils, Some(spdConfig), configs: _*)
  }

  def copyWith(additionalConfigs: (Setting[_], Object)*): Edition[CONTEXT] = {
    val newConfigs = (configs ++ additionalConfigs).toMap
    new Edition(graphBuilderFactory, runtimeContextManagerFactory, runtimeTestUtils, spd, newConfigs.toSeq: _*)
  }

  def copyWith(
    newRuntimeContextManagerFactory: RuntimeContextManagerFactory[CONTEXT],
    additionalConfigs: (Setting[_], Object)*
  ): Edition[CONTEXT] = {
    val newConfigs = (configs ++ additionalConfigs).toMap
    new Edition(graphBuilderFactory, newRuntimeContextManagerFactory, runtimeTestUtils, spd, newConfigs.toSeq: _*)
  }

  def copyWith(
    newGraphBuilderFactory: () => TestDatabaseManagementServiceBuilder
  ): Edition[CONTEXT] = {
    new Edition(newGraphBuilderFactory, runtimeContextManagerFactory, runtimeTestUtils, spd, configs: _*)
  }

  def getSetting[T](setting: Setting[T]): Option[T] = {
    configs.collectFirst { case (key, value) if key == setting => value.asInstanceOf[T] }
  }

  def newRuntimeContextManager(
    resolver: DependencyResolver,
    lifeSupport: LifeSupport,
    logProvider: InternalLogProvider
  ): TestRuntimeContextManager[CONTEXT] = {
    val config = resolver.resolveDependency(classOf[Config])
    runtimeContextManagerFactory.newRuntimeContextManager(runtimeConfig(config), resolver, lifeSupport, logProvider)
  }

  def runtimeConfig(config: Config): CypherRuntimeConfiguration = {
    CypherRuntimeConfiguration.fromCypherConfiguration(cypherConfig(config))
  }

  def cypherConfig(config: Config): CypherConfiguration = {
    CypherConfiguration.fromConfig(config)
  }
}

object Edition {
  case class Dbms(dbms: DatabaseManagementService, filesystem: EphemeralFileSystemAbstraction)
  case class SpdConfig(primaries: Int, shards: Int)
}

object COMMUNITY {

  object CommunityRuntimeContextManagerFactory extends RuntimeContextManagerFactory[CommunityRuntimeContext] {

    override def newRuntimeContextManager(
      runtimeConfig: CypherRuntimeConfiguration,
      resolver: DependencyResolver,
      lifeSupport: LifeSupport,
      logProvider: InternalLogProvider
    ): TestRuntimeContextManager[CommunityRuntimeContext] = {
      CommunityTestRuntimeContextManager(CommunityRuntimeContextManager(logProvider.getLog("test"), runtimeConfig))
    }
  }

  val EDITION = new Edition(
    () => new TestDatabaseManagementServiceBuilder,
    CommunityRuntimeContextManagerFactory,
    CommunityRuntimeTestUtils,
    spd = None,
    GraphDatabaseSettings.cypher_hints_error -> TRUE
  )

  case class CommunityTestRuntimeContextManager(delegate: CommunityRuntimeContextManager)
      extends TestRuntimeContextManager[CommunityRuntimeContext] {

    override def create(
      cypherVersion: CypherVersion,
      tokenContext: ReadTokenContext,
      transactionalContext: TransactionalContext,
      clock: Clock,
      debugOptions: CypherDebugOptions,
      queryOptions: QueryOptions,
      anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
      executionModel: ExecutionModel,
      indexComparatorFactory: IndexComparatorFactory
    ): CommunityRuntimeContext = {
      delegate.create(
        cypherVersion,
        tokenContext,
        transactionalContext,
        clock,
        debugOptions,
        queryOptions,
        anonymousVariableNameGenerator,
        executionModel,
        indexComparatorFactory
      )
    }

    override def config: CypherRuntimeConfiguration = delegate.config
    override def assertAllReleased(): Unit = delegate.assertAllReleased()
    override def waitForWorkersToIdle(timeoutMs: Int): Boolean = delegate.waitForWorkersToIdle(timeoutMs)

    override def defaultExecutionModel: ExecutionModel = ExecutionModel.Volcano
  }
}
