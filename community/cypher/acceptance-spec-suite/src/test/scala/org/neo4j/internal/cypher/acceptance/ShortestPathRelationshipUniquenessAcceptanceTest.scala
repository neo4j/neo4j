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
import org.neo4j.cypher.internal.compatibility.ClosingExecutionResult
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}

class ShortestPathRelationshipUniquenessAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should not find shortest path due to relationship uniquess") {
    val p0 = createLabeledNode(Map("id" -> "2228"), "Model")
    val p1 = createLabeledNode(Map("id" -> "2246"), "Model")
    val p2 = createLabeledNode(Map("id" -> "2248"), "Model")
    val p3 = createLabeledNode(Map("id" -> "32"), "Model")
    val p4 = createLabeledNode(Map("id" -> "2640"), "Model")
    val p5 = createLabeledNode(Map("id" -> "2638"), "Model")

    relate(p0, p1, "2633")
    relate(p1, p2, "2636")
    relate(p2, p3, "2644")
    relate(p2, p4, "2644")
    relate(p4, p5, "2640")
    val query =
      """MATCH p=shortestpath((a:Model)-[r*]-(b:Model))
    WHERE a.id="2228" AND b.id="2638" AND ANY ( n IN nodes(p)[1..-1] WHERE (n.id = "32") )
    RETURN nodes(p) as nodes"""

    val result = executeUsingCostPlannerOnly(query).columnAs("nodes").toList
    result should be(List.empty)
  }

  test("should find the longer short path") {
    val p0 = createLabeledNode(Map("id" -> "2228"), "Model")
    val p1 = createLabeledNode(Map("id" -> "2246"), "Model")
    val p2 = createLabeledNode(Map("id" -> "2248"), "Model")
    val p3 = createLabeledNode(Map("id" -> "32"), "Model")
    val p4 = createLabeledNode(Map("id" -> "2640"), "Model")
    val p5 = createLabeledNode(Map("id" -> "2638"), "Model")

    val pLongPath0 = createLabeledNode(Map("id" -> "1"), "Model")
    val pLongPath1 = createLabeledNode(Map("id" -> "2"), "Model")
    val pLongPath2 = createLabeledNode(Map("id" -> "3"), "Model")
    val pLongPath3 = createLabeledNode(Map("id" -> "4"), "Model")
    val pLongPath4 = createLabeledNode(Map("id" -> "5"), "Model")
    val pLongPath5 = createLabeledNode(Map("id" -> "6"), "Model")

    relate(p0, pLongPath0, "10")
    relate(pLongPath0, pLongPath1, "20")
    relate(pLongPath1, pLongPath2, "30")
    relate(pLongPath2, pLongPath3, "40")
    relate(pLongPath3, pLongPath4, "50")
    relate(pLongPath4, pLongPath5, "60")
    relate(pLongPath5, p3, "70")

    relate(p0, p1, "2633")
    relate(p1, p2, "2636")
    relate(p2, p3, "2644")
    relate(p2, p4, "2644")
    relate(p4, p5, "2640")
    val query =
      """MATCH p=shortestpath((a:Model)-[r*]-(b:Model))
    WHERE a.id="2228" AND b.id="2638" AND ANY ( n IN nodes(p)[1..-1] WHERE (n.id = "32") )
    RETURN nodes(p) as nodes"""

    val result = executeUsingCostPlannerOnly(query).columnAs("nodes").toList
    result should be(List(List(p0, pLongPath0, pLongPath1, pLongPath2, pLongPath3, pLongPath4, pLongPath5, p3, p2, p4, p5)))
  }

  def executeUsingCostPlannerOnly(query: String) =
    eengine.execute(s"CYPHER planner=COST $query", Map.empty[String, Any]) match {
      case e: ClosingExecutionResult => RewindableExecutionResult(e.inner)
    }
}
