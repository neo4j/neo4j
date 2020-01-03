/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.{PatternRelationship, ShortestPathPattern, SimplePatternLength, RegularSinglePlannerQuery}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("finds shortest paths") {
    planFor("MATCH (a), (b), shortestPath((a)-[r]->(b)) RETURN b")._2 should equal(
      FindShortestPaths(
        CartesianProduct(
          AllNodesScan("a", Set.empty),
          AllNodesScan("b", Set.empty)
        ),
        ShortestPathPattern(
          Some("  FRESHID16"),
          PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          single = true
        )(null)
      )
    )
  }

  test("find shortest path with length predicate and WITH should not plan fallback") {
    planFor("MATCH (a), (b), p = shortestPath((a)-[r]->(b)) WITH p WHERE length(p) > 1 RETURN p")._2 should equal(
      Selection(ands(greaterThan(function("length", varFor("p")), literalInt(1))),
        FindShortestPaths(
          CartesianProduct(
            AllNodesScan("a", Set.empty),
            AllNodesScan("b", Set.empty)
          ),
          ShortestPathPattern(
            Some("p"),
            PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
            single = true
          )(null)
        )
      )
    )
  }

  test("finds all shortest paths") {
    planFor("MATCH (a), (b), allShortestPaths((a)-[r]->(b)) RETURN b")._2 should equal(
      FindShortestPaths(
        CartesianProduct(
          AllNodesScan("a", Set.empty),
          AllNodesScan("b", Set.empty)
        ),
        ShortestPathPattern(
          Some("  FRESHID16"),
          PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          single = false
        )(null)
      )
    )
  }

  test("find shortest paths on top of hash joins") {
    val result = (new given {
      cardinality = mapCardinality {
        // node label scan
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.size == 1 => 100.0
        // all node scan
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.isEmpty => 10000.0
        // expand
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        case _                             => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p")._2

    val expected =
      FindShortestPaths(
        Selection(
          ands(not(equals(varFor("r1"), varFor("r2")))),
          NodeHashJoin(
            Set("b"),
            Expand(
              NodeByLabelScan("a", labelName("X"), Set.empty),
              "a", SemanticDirection.INCOMING, Seq.empty, "b", "r1", ExpandAll),
            Expand(
              NodeByLabelScan("c", labelName("X"), Set.empty),
              "c", SemanticDirection.INCOMING, Seq.empty, "b", "r2", ExpandAll)
          )
        ),
        ShortestPathPattern(Some("p"), PatternRelationship("r", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null))

    result should equal(expected)
  }
}
