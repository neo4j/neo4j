/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.internal.{CompatibilityFactory, ExecutionEngine, RewindableExecutionResult}
import org.neo4j.cypher.{ExecutionEngineFunSuite, RunWithConfigTestSupport, ShortestPathCommonEndNodesForbiddenException}
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Versions.{V3_1, V3_3}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.logging.NullLogProvider

class ShortestPathSameNodeAcceptanceTest extends ExecutionEngineFunSuite with RunWithConfigTestSupport with CypherComparisonSupport {

  val expectedToFail = TestConfiguration(
    Versions(Versions.Default, V3_1, V3_3),
    Planners(Planners.Cost, Planners.Rule, Planners.Default),
    Runtimes(Runtimes.Interpreted, Runtimes.Slotted, Runtimes.Default, Runtimes.ProcedureOrSchema))

  def setupModel(db: GraphDatabaseCypherService) {
    db.inTx {
      val a = db.getGraphDatabaseService.createNode()
      val b = db.getGraphDatabaseService.createNode()
      val c = db.getGraphDatabaseService.createNode()
      a.createRelationshipTo(b, RelationshipType.withName("KNOWS"))
      b.createRelationshipTo(c, RelationshipType.withName("KNOWS"))
    }
  }

  test("shortest paths with explicit same start and end nodes should throw exception by default") {
    setupModel(graph)
    val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
    failWithError(expectedToFail, query, List("The shortest path algorithm does not work when the start and end nodes are the same."))
  }

  test("shortest paths with explicit same start and end nodes should throw exception when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "true") { db =>
      setupModel(db)
      val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
      intercept[ShortestPathCommonEndNodesForbiddenException](
        executeUsingCostPlannerOnly(db, query).toList
      ).getMessage should include("The shortest path algorithm does not work when the start and end nodes are the same")
    }
  }

  test("shortest paths with explicit same start and end nodes should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false") { db =>
      setupModel(db)
      val query = "MATCH p=shortestPath((a)-[*]-(a)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(0)
    }
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should throw exception by default") {
    setupModel(graph)
    val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
    failWithError(expectedToFail, query, List("The shortest path algorithm does not work when the start and end nodes are the same."))
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should throw exception when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "true") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
      intercept[ShortestPathCommonEndNodesForbiddenException](
        executeUsingCostPlannerOnly(db, query).toList
      ).getMessage should include("The shortest path algorithm does not work when the start and end nodes are the same")
    }
  }

  test("shortest paths that discover at runtime that the start and end nodes are the same should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(6)
    }
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should not throw exception by default") {
    setupModel(graph)
    val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
    executeWith(Configs.Interpreted, query).toList.length should be(9)
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should throw exception even when when configured to do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "true") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(9)
    }
  }

  test("shortest paths with min length 0 that discover at runtime that the start and end nodes are the same should not throw exception when configured to not do so") {
    runWithConfig(GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false") { db =>
      setupModel(db)
      val query = "MATCH (a), (b) MATCH p=shortestPath((a)-[*0..]-(b)) RETURN p"
      executeUsingCostPlannerOnly(db, query).toList.length should be(9)
    }
  }

  def executeUsingCostPlannerOnly(db: GraphDatabaseCypherService, query: String): InternalExecutionResult = {
    val compatibilityFactory = db.getDependencyResolver.resolveDependency(classOf[CompatibilityFactory])
    RewindableExecutionResult(
      new ExecutionEngine(db, NullLogProvider.getInstance(), compatibilityFactory)
        .execute(s"CYPHER planner=COST $query", Map.empty[String, Any])
    )
  }
}
