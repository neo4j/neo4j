/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

/**
  * These tests are similar with the tests in LeafPlanningIntegrationTest, but
  * instead test with an actual database to make sure they work with the whole stack
  * including settings in the database configuration.
  * For more light-weight testing please use
  * [[org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LeafPlanningIntegrationTest]]
  */
class CostPlannerAcceptanceTest extends ExecutionEngineFunSuite {

  override def databaseConfig(): Map[Setting[_], String] =
    Map(GraphDatabaseSettings.query_non_indexed_label_warning_threshold -> "10",
      GraphDatabaseSettings.cypher_plan_with_minimum_cardinality_estimates -> "true")

  test("should do two index seeks instead of scans with explicit index hint (import scenario)") {
    graph.createIndex("A", "prop")
    graph.createIndex("B", "prop")

    val query =
      """LOAD CSV WITH HEADERS FROM 'file:///dummy.csv' AS row
        |MATCH (a:A), (b:B)
        |USING INDEX a:A(prop)
        |USING INDEX b:B(prop)
        |WHERE a.prop = row.propA AND b.prop = row.propB
        |CREATE (a)-[r:R]->(b)
      """.stripMargin

    testPlanNodeIndexSeek(query, assertNumberOfIndexSeeks = 2)
  }

  test("should do two index seeks instead of scans without explicit index hint (import scenario)") {
    graph.createIndex("A", "prop")
    graph.createIndex("B", "prop")

    val query =
      """LOAD CSV WITH HEADERS FROM 'file:///dummy.csv' AS row
        |MATCH (a:A), (b:B)
        |WHERE a.prop = row.propA AND b.prop = row.propB
        |CREATE (a)-[r:R]->(b)
      """.stripMargin

    testPlanNodeIndexSeek(query, assertNumberOfIndexSeeks = 2)
  }

  test("should do index seek instead of scan with explicit index seek hint") {
    graph.createIndex("A", "prop")

    val query = """
                  |MATCH (a:A)
                  |USING INDEX a:A(prop)
                  |WHERE a.prop = 42
                  |RETURN a.prop
                """.stripMargin

    testPlanNodeIndexSeek(query, assertNumberOfIndexSeeks = 1)
  }

  test("should do index seek instead of scan without explicit index seek hint") {
    graph.createIndex("A", "prop")

    val query = """
                  |MATCH (a:A)
                  |WHERE a.prop = 42
                  |RETURN a.prop
                """.stripMargin

    testPlanNodeIndexSeek(query, assertNumberOfIndexSeeks = 1)
  }

  test("should do node by id seek instead of scan") {
    val query = """
                  |MATCH (a:A)
                  |WHERE id(a) = 42
                  |RETURN a.prop
                """.stripMargin

    val explainAndAssert = () => {
      val result = execute(s"EXPLAIN $query")
      result.executionPlanDescription() should useOperatorTimes("NodeByIdSeek", 1)
    }
    new GeneratedTestValues().test(executeOnDbWithInitialNumberOfNodes(explainAndAssert, _))
  }

  test("should do relationship by id seek instead of scan") {
    val query = """
                  |MATCH ()-[r]-()
                  |WHERE id(r) = 42
                  |RETURN r.prop
                """.stripMargin

    val explainAndAssert = () => {
      val result = execute(s"EXPLAIN $query")
      result.executionPlanDescription() should useOperatorTimes("UndirectedRelationshipByIdSeek", 1)
    }
    new GeneratedTestValues().test(executeOnDbWithInitialNumberOfNodes(explainAndAssert, _))
  }

  test("cartesian products should not hide costs") {
    val aNode = createLabeledNode("A", "B", "C", "D")
    (0 to 100) foreach { _ =>
      relate(aNode, createLabeledNode("X"))
    }
    (0 to 100) foreach { _ =>
      relate(aNode, createLabeledNode("Y"))
    }

    val query =
      """MATCH (a:A:B:C:D)-->(b)
        |WHERE id(a) = 0 AND b:X or b:Y
        |RETURN *
      """.stripMargin

    val result = execute(s"EXPLAIN $query")
    result.executionPlanDescription() should not(useOperators("CartesianProduct"))
  }

  private def testPlanNodeIndexSeek(query: String, assertNumberOfIndexSeeks: Int): Unit = {
    val explainAndAssertNodeIndexSeekIsUsed = () => {
      val result = execute(s"EXPLAIN $query")
      result.executionPlanDescription() should useOperatorTimes("NodeIndexSeek", assertNumberOfIndexSeeks)
    }
    new GeneratedTestValues().test(executeOnDbWithInitialNumberOfNodes(explainAndAssertNodeIndexSeekIsUsed, _))
  }

  private class GeneratedTestValues {
    // This will yield 4 ^ 5 = 1024 combinations
    //def nodesWithoutLabelGen = List(0, 1, 10, 100)
    //def aNodesWithoutPropGen = List(0, 1, 10, 100)
    //def bNodesWithoutPropGen = List(0, 1, 10, 100)
    //def aNodesWithPropGen = List(0, 1, 10, 100)
    //def bNodesWithPropGen = List(0, 1, 10, 100)

    // This will yield 3 ^ 5 = 243 combinations
    def nodesWithoutLabelGen = List(0, 1, 100)
    def aNodesWithoutPropGen = List(0, 1, 100)
    def bNodesWithoutPropGen = List(0, 1, 100)
    def aNodesWithPropGen = List(0, 1, 100)
    def bNodesWithPropGen = List(0, 1, 100)

    val dbCounts = for {
      nodesWithoutLabel <- nodesWithoutLabelGen
      aNodesWithoutProp <- aNodesWithoutPropGen
      bNodesWithoutProp <- bNodesWithoutPropGen
      aNodesWithProp <- aNodesWithPropGen
      bNodesWithProp <- bNodesWithPropGen
    } yield {
      val y = InitialNumberOfNodes(
        nodesWithoutLabel,
        aNodesWithoutProp,
        bNodesWithoutProp,
        aNodesWithProp,
        bNodesWithProp)
      y
    }

    def test(f: (InitialNumberOfNodes) => Unit) {
      dbCounts.foreach(f)
    }
  }

  private case class InitialNumberOfNodes(nodesWithoutLabel: Int,
                                          aNodesWithoutProp: Int,
                                          bNodesWithoutProp: Int,
                                          aNodesWithProp: Int,
                                          bNodesWithProp: Int) {
    override def toString: String =
      s"""
         |InitialNumberOfNodes(nodesWithoutLabel: $nodesWithoutLabel,
         |                     aNodesWithoutProp: $aNodesWithoutProp,
         |                     bNodesWithoutProp: $bNodesWithoutProp,
         |                     aNodesWithProp:    $aNodesWithProp,
         |                     bNodesWithProp:    $bNodesWithProp)
      """.stripMargin
  }

  private def executeOnDbWithInitialNumberOfNodes(f: () => Unit,
                                                  config: InitialNumberOfNodes): Unit = {
    graph.inTx {
      (1 to config.nodesWithoutLabel).foreach { _ => createNode() }
      (1 to config.aNodesWithoutProp).foreach { _ => createLabeledNode("A") }
      (1 to config.bNodesWithoutProp).foreach { i => createLabeledNode("B") }
      (1 to config.aNodesWithProp).foreach { i => createLabeledNode(Map("prop" -> i), "A") }
      (1 to config.bNodesWithProp).foreach { i => createLabeledNode(Map("prop" -> (i + 10000)), "B") }
    }

    try {
      f()
    } catch {
      case t: Throwable =>
        System.err.println(s"Failed with $config")
        throw t
    }

    deleteAllEntities()
  }
}
