/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName

class PlannerQueryTest extends CypherFunSuite {
  test("reverse pair map") {

    val qg1 = QueryGraph.empty
    val qg2 = QueryGraph.empty
    val qg3 = QueryGraph.empty

    val pq3 = PlannerQuery(graph = qg3, tail = None)
    val pq2 = PlannerQuery(graph = qg2, tail = Some(pq3))
    val pq1 = PlannerQuery(graph = qg1, tail = Some(pq2))

    var seenOnPos1 = List.empty[PlannerQuery]
    var seenOnPos2 = List.empty[PlannerQuery]

    val result = pq1.reverseFoldMap {
      case (pq1: PlannerQuery, pq2: PlannerQuery) =>
        seenOnPos1 = seenOnPos1 :+ pq1
        seenOnPos2 = seenOnPos2 :+ pq2
        pq2
    }

    seenOnPos1 should equal(List(pq2, pq1))
    seenOnPos2 should equal(List(pq3, pq2))
    result should equal(pq1)
  }

  test("reverseFoldMap on single plannerQuery returns that PQ") {

    val input = PlannerQuery(graph = QueryGraph.empty, tail = None)

    val result = input.reverseFoldMap {
      case (pq1: PlannerQuery, pq2: PlannerQuery) =>
        fail("should not pass through here")
    }

    result should equal(input)
  }

  test("reverseFoldMap plannerQuery with tail should change when reverseMapped") {

    val tail = PlannerQuery(graph = QueryGraph.empty)
    val firstQueryGraph = QueryGraph.empty
    val secondQueryGraph = QueryGraph(patternNodes = Set(IdName("a")))
    val input = PlannerQuery(graph = firstQueryGraph, tail = Some(tail))

    val result = input.reverseFoldMap {
      case (pq1: PlannerQuery, pq2: PlannerQuery) =>
        pq2.withGraph(secondQueryGraph)
    }

    result should not equal input
    result.graph should equal(firstQueryGraph)
    result.tail.get.graph should equal(secondQueryGraph)
  }
}
