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

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.factory.GraphDatabaseSettings

class ReplanningAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  override def databaseConfig(): collection.Map[Setting[_], String] = super.databaseConfig() ++ Map(
    GraphDatabaseSettings.cypher_min_replan_interval -> "100ms",
    GraphDatabaseSettings.query_statistics_divergence_threshold -> "0.1"
  )

  test("should replan query from AllNodeScan to NodeById as the graph grows") {
    val params = Map("rows" -> List(
                       Map("startNodeId" -> 374337L,
                           "relRef" -> -200864L,
                           "endNodeId" -> 540311L,
                           "props" -> Map("timestamp" -> 1552893253242L)
                       )
                     ))

    val query =
      """
        |EXPLAIN
        |UNWIND {rows} as row
        |MATCH (startNode) WHERE ID(startNode) = row.startNodeId
        |MATCH (endNode) WHERE ID(endNode) = row.endNodeId
        |CREATE (startNode)-[rel:R]->(endNode) SET rel += row.props
        |RETURN rel
      """.stripMargin

    // given
    val planForEmptyGraph = innerExecuteDeprecated(query, params).executionPlanDescription()
    planForEmptyGraph should useOperators("AllNodesScan")
    planForEmptyGraph should not(useOperators("NodeByIdSeek"))

    // when
    graph.inTx {
      (0 until 1000).map(_ => createNode())
    }
    Thread.sleep(200)

    // then
    val planForPopulatedGraph = innerExecuteDeprecated(query, params).executionPlanDescription()
    planForPopulatedGraph should useOperators("NodeByIdSeek")
    planForPopulatedGraph should not(useOperators("AllNodesScan"))
  }
}
