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

import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.compatibility.ExecutionResultWrapperFor3_0
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class ShortestPathComplexQueryAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("allShortestPaths with complex LHS should be planned with exhaustive fallback and include predicate") {
    setupModel()
    val result = executeUsingCostPlannerOnly(
      """
        |PROFILE MATCH (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |WITH kim AS kimDeal, collect(charles) AS charlesT, collect(joey) AS joeyS
        |UNWIND charlesT AS charlesThompson
        |UNWIND joeyS AS joeySantiago
        |MATCH pathx = allShortestPaths((charlesThompson)-[*1..5]-(joeySantiago))
        |WHERE none (n IN nodes(pathx) WHERE id(n) = id(kimDeal))
        |RETURN extract(node in nodes(pathx) | id(node)) as ids
      """.stripMargin)
    val results = result.columnAs("ids").toList
    results should be(List(List(0, 4, 3, 2)))
    result should use("VarLengthExpand(Into)", "AntiConditionalApply")
  }

  test("shortestPath with complex LHS should be planned with exhaustive fallback and include predicate") {
    setupModel()
    val result = executeUsingCostPlannerOnly(
      """
        |PROFILE MATCH (charles:Pixie { fname : 'Charles'}),(joey:Pixie { fname : 'Joey'}),(kim:Pixie { fname : 'Kim'})
        |WITH kim AS kimDeal, collect(charles) AS charlesT, collect(joey) AS joeyS
        |UNWIND charlesT AS charlesThompson
        |UNWIND joeyS AS joeySantiago
        |MATCH pathx = shortestPath((charlesThompson)-[*1..5]-(joeySantiago))
        |WHERE none (n IN nodes(pathx) WHERE id(n) = id(kimDeal))
        |RETURN extract(node in nodes(pathx) | id(node)) as ids
      """.stripMargin)
    val results = result.columnAs("ids").toList
    results should be(List(List(0, 4, 3, 2)))
    result should use("VarLengthExpand(Into)", "AntiConditionalApply")
  }

  def executeUsingCostPlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=IDP $query", Map.empty[String, Any], graph.session()) match {
      case e:ExecutionResultWrapperFor3_0 => RewindableExecutionResult(e)
    }

  private def setupModel(): Unit = {
    executeUsingCostPlannerOnly(
      """
        |MERGE (p1:Pixie {fname:'Charles'})
        |MERGE (p2:Pixie {fname:'Kim'})
        |MERGE (p3:Pixie {fname:'Joey'})
        |MERGE (p4:Pixie {fname:'David'})
        |MERGE (p5:Pixie {fname:'Paz'})
        |MERGE (p1)-[:KNOWS]->(p2)-[:KNOWS]->(p3)-[:KNOWS]->(p4)-[:KNOWS]->(p5)-[:KNOWS]->(p1)
        |RETURN p1,p2,p3,p4,p5
      """.stripMargin)
  }
}
