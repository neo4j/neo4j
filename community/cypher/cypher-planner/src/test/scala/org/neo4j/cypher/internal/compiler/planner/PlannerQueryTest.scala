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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PlannerQueryTest extends CypherFunSuite with AstConstructionTestSupport {

  test("pair map") {

    val qg1 = QueryGraph.empty
    val qg2 = QueryGraph.empty
    val qg3 = QueryGraph.empty

    val pq3 = RegularSinglePlannerQuery(queryGraph = qg3, tail = None)
    val pq2 = RegularSinglePlannerQuery(queryGraph = qg2, tail = Some(pq3))
    val pq1 = RegularSinglePlannerQuery(queryGraph = qg1, tail = Some(pq2))

    var seenOnPos1 = List.empty[SinglePlannerQuery]
    var seenOnPos2 = List.empty[SinglePlannerQuery]

    val result = pq1.foldMap {
      case (pq1: SinglePlannerQuery, pq2: SinglePlannerQuery) =>
        seenOnPos1 = seenOnPos1 :+ pq1
        seenOnPos2 = seenOnPos2 :+ pq2
        pq2
    }

    seenOnPos1 should equal(List(pq1, pq2))
    seenOnPos2 should equal(List(pq2, pq3))
    result should equal(pq1)
  }

  test("foldMap on single plannerQuery returns that PQ") {

    val input = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty, tail = None)

    val result = input.foldMap {
      case (_: SinglePlannerQuery, _: SinglePlannerQuery) =>
        fail("should not pass through here")
    }

    result should equal(input)
  }

  test("foldMap plannerQuery with tail should change when reverseMapped") {

    val tail = RegularSinglePlannerQuery(queryGraph = QueryGraph.empty)
    val firstQueryGraph = QueryGraph.empty
    val secondQueryGraph = QueryGraph(patternNodes = Set(v"a"))
    val input = RegularSinglePlannerQuery(queryGraph = firstQueryGraph, tail = Some(tail))

    val result = input.foldMap {
      case (_: SinglePlannerQuery, pq2: SinglePlannerQuery) =>
        pq2.withQueryGraph(secondQueryGraph)
    }

    result should not equal input
    result.queryGraph should equal(firstQueryGraph)
    result.tail.get.queryGraph should equal(secondQueryGraph)
  }

  test("should not mutate SinglePlannerQuery.empty state") {
    val q = SinglePlannerQuery.empty
    q.allQGsWithLeafInfo.foreach(_.allKnownUnstableNodeLabels(SemanticTable()))
    q.allQGsWithLeafInfo.foreach(_.allKnownUnstableNodeLabels.cacheSize shouldBe 1)

    SinglePlannerQuery.empty.allQGsWithLeafInfo.foreach(_.allKnownUnstableNodeLabels.cacheSize shouldBe 0)
  }
}
