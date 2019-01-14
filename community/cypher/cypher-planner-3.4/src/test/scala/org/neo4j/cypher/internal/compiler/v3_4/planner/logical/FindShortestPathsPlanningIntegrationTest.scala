/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.v3_4.logical.plans._
import org.neo4j.cypher.internal.v3_4.expressions.{Equals, Not, SemanticDirection, Variable}

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
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.size == 1 => 100.0
        // all node scan
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 && queryGraph.selections.predicates.isEmpty => 10000.0
        // expand
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        case _                             => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)<-[r1]-(b)-[r2]->(c:X), p = shortestPath((a)-[r]->(c)) RETURN p")._2

    val expected =
      FindShortestPaths(
        Selection(
          Seq(Not(Equals(Variable("r1") _, Variable("r2") _) _) _),
          NodeHashJoin(
            Set("b"),
            Expand(
              NodeByLabelScan("a", lblName("X"), Set.empty),
              "a", SemanticDirection.INCOMING, Seq.empty, "b", "r1", ExpandAll),
            Expand(
              NodeByLabelScan("c", lblName("X"), Set.empty),
              "c", SemanticDirection.INCOMING, Seq.empty, "b", "r2", ExpandAll)
          )
        ),
        ShortestPathPattern(Some("p"), PatternRelationship("r", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null))

    result should equal(expected)
  }
}
