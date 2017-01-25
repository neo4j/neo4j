/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.internal.cypher.acceptance

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor3_0
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node

class ShortestPathComplexQueryAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("allShortestPaths with complex LHS should be planned with exhaustive fallback and include predicate") {
    setupModel()
    val result = executeUsingCostPlannerOnly(
      """
        |//same idea but iterating over two collections of nodes
        |profile match (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |with kim as kimDeal, collect(charles) as charlesT, collect(joey) as joeyS
        |unwind charlesT AS charlesThompson
        |unwind joeyS AS joeySantiago
        |match pathx = allShortestPaths((charlesThompson)-[*1..5]-(joeySantiago))
        |where none (n IN nodes(pathx) where id(n) = id(kimDeal))
        |with nodes(pathx) as nodes
        |return nodes
      """.stripMargin)
    val results = result.columnAs("nodes").toList
    println(results)
    println(result.executionPlanDescription())
    val ids = results(0).asInstanceOf[Seq[_]].map(n => n.asInstanceOf[Node].getId)
    ids should be(List(0, 4, 3, 2))
    result should use("VarLengthExpand(Into)", "AntiConditionalApply")
  }

  test("shortestPath with complex LHS should be planned with exhaustive fallback and include predicate") {
    System.setProperty("pickBestPlan.VERBOSE","true")
    setupModel()
    val result = executeUsingCostPlannerOnly(
      """
        |profile match (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |with kim as kimDeal, collect(charles) as charlesT, collect(joey) as joeyS
        |unwind charlesT AS charlesThompson
        |unwind joeyS AS joeySantiago
        |match pathx = shortestPath((charlesThompson)-[*1..5]-(joeySantiago))
        |where none (n IN nodes(pathx) where id(n) = id(kimDeal))
        |unwind nodes(pathx) as nodes
        |return nodes
      """.stripMargin)
    val results = result.columnAs("nodes").toList
    println(results)
    println(result.executionPlanDescription())
    assertThat(results.length, equalTo(4))
    result should use("VarLengthExpand(Into)", "AntiConditionalApply")
  }

  def executeUsingCostPlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=IDP $query", Map.empty[String, Any], graph.session()) match {
      case e:ExecutionResultWrapperFor3_0 => RewindableExecutionResult(e)
    }

  private def setupModel(): Unit = {
    executeUsingCostPlannerOnly(
      """
        |//dataset creation
        |merge (p1:Pixie {fname:'Charles'})
        |merge (p2:Pixie {fname:'Kim'})
        |merge (p3:Pixie {fname:'Joey'})
        |merge (p4:Pixie {fname:'David'})
        |merge (p5:Pixie {fname:'Paz'})
        |merge (p1)-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4)-[:KNOWS]->(p5)-[:KNOWS]->(p1)
        |return p1,p2,p3,p4,p5
      """.stripMargin)
  }
}
