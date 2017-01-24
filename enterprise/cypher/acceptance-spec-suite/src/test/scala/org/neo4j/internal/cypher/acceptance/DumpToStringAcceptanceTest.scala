/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.{CommunityCompatibilityFactory, EnterpriseCompatibilityFactory, ExecutionEngine}
import org.neo4j.cypher.{ExecutionEngineFunSuite, ExecutionEngineTestSupport, NewPlannerTestSupport}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.WindowsStringSafe
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.logging.{LogProvider, NullLogProvider}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory
import org.neo4j.kernel.monitoring.{Monitors => KernelMonitors}

import collection.JavaConverters._

class DumpToStringAcceptanceTest extends ExecutionEngineFunSuite with NewRuntimeTestSupport {

  implicit val windowsSafe = WindowsStringSafe

  test("format node") {
    createNode(Map("prop1" -> "A", "prop2" -> 2))

    executeWithAllPlannersAndRuntimesAndCompatibilityMode("match (n) return n").dumpToString() should
      equal(
        """+----------------------------+
          || n                          |
          |+----------------------------+
          || Node[0]{prop1:"A",prop2:2} |
          |+----------------------------+
          |1 row
          |""".stripMargin)
  }

  test("format relationship") {
    relate(createNode(), createNode(), "T", Map("prop1" -> "A", "prop2" -> 2))

    executeWithAllPlannersAndRuntimesAndCompatibilityMode("match ()-[r]->() return r").dumpToString() should equal(
      """+--------------------------+
        || r                        |
        |+--------------------------+
        || :T[0]{prop1:"A",prop2:2} |
        |+--------------------------+
        |1 row
        |""".stripMargin)
  }

  test("format collection of maps") {
    executeWithAllPlannersAndRuntimesAndCompatibilityMode( """RETURN [{ inner: 'Map1' }, { inner: 'Map2' }]""").dumpToString() should
      equal(
        """+----------------------------------------+
          || [{ inner: 'Map1' }, { inner: 'Map2' }] |
          |+----------------------------------------+
          || [{inner -> "Map1"},{inner -> "Map2"}]  |
          |+----------------------------------------+
          |1 row
          |""".stripMargin)
  }
}

trait NewRuntimeTestSupport extends NewPlannerTestSupport with ExecutionEngineTestSupport {
  self: ExecutionEngineFunSuite =>
  override protected def createGraphDatabase(): GraphDatabaseCypherService = {
    val impermanentDatabase = new TestEnterpriseGraphDatabaseFactory().newImpermanentDatabase(databaseConfig().asJava)
    new GraphDatabaseCypherService(impermanentDatabase)
  }

  override protected def initTest(): Unit = {
    eengine = createEngine(graph)
  }

  def createEngine(graphDatabaseCypherService: GraphDatabaseQueryService, logProvider: LogProvider = NullLogProvider.getInstance()): ExecutionEngine = {
    val resolver = graphDatabaseCypherService.getDependencyResolver
    val kernel = resolver.resolveDependency(classOf[KernelAPI])
    val kernelMonitors: KernelMonitors = resolver.resolveDependency(classOf[KernelMonitors])
    val communityCompatibilityFactory = new CommunityCompatibilityFactory(graphDatabaseCypherService, kernel, kernelMonitors, logProvider)
    val compatibilityFactory = new EnterpriseCompatibilityFactory(communityCompatibilityFactory, graph, kernel, kernelMonitors, logProvider)
    new ExecutionEngine(graphDatabaseCypherService, logProvider, compatibilityFactory)
  }
}