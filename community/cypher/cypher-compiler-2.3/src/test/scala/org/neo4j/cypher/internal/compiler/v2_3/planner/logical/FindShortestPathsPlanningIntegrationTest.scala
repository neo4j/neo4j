/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport2, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Equals, Identifier, Not}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("finds shortest paths") {
    planFor("MATCH a, b, shortestPath(a-[r]->b) RETURN b").plan should equal(
      FindShortestPaths(
        CartesianProduct(
          AllNodesScan("b", Set.empty)(solved),
          AllNodesScan("a", Set.empty)(solved)
        )(solved),
        ShortestPathPattern(
          None,
          PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          single = true
        )(null)
      )(solved)
    )
  }

  test("finds all shortest paths") {
    planFor("MATCH a, b, allShortestPaths(a-[r]->b) RETURN b").plan should equal(
      FindShortestPaths(
        CartesianProduct(
          AllNodesScan("b", Set.empty)(solved),
          AllNodesScan("a", Set.empty)(solved)
        )(solved),
        ShortestPathPattern(
          None,
          PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          single = false
        )(null)
      )(solved)
    )
  }

  test("find shortest paths on top of hash joins") {
    val result = (new given {
      cardinality = mapCardinality {
        // node label scan
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.size == 1 => 100.0
        // all node scan
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.isEmpty => 10000.0
        // expand
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        case _                             => Double.MaxValue
      }
    } planFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p").plan

    val expected =
      FindShortestPaths(
        Selection(
          Seq(Not(Equals(Identifier("r1") _, Identifier("r2") _) _) _),
          NodeHashJoin(
            Set(IdName("b")),
            Expand(
              NodeByLabelScan(IdName("a"), LazyLabel("X"), Set.empty)(solved),
              IdName("a"), SemanticDirection.INCOMING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved),
            Expand(
              NodeByLabelScan(IdName("c"), LazyLabel("X"), Set.empty)(solved),
              IdName("c"), SemanticDirection.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
          )(solved)
        )(solved),
        ShortestPathPattern(Some(IdName("p")), PatternRelationship("r", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null))(solved)

    result should equal(expected)
  }
}
