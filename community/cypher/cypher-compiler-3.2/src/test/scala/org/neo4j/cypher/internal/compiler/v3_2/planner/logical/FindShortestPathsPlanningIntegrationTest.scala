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
package org.neo4j.cypher.internal.compiler.v3_2.planner.logical

import org.neo4j.cypher.internal.compiler.v3_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_2.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_2.ast.{Equals, Not, Variable}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_2._

class FindShortestPathsPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("finds shortest paths") {
    planFor("MATCH (a), (b), shortestPath((a)-[r]->(b)) RETURN b")._2 should equal(
      FindShortestPaths(
        CartesianProduct(
          AllNodesScan("a", Set.empty)(solved),
          AllNodesScan("b", Set.empty)(solved)
        )(solved),
        ShortestPathPattern(
          Some("  FRESHID16"),
          PatternRelationship("r", ("a", "b"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength),
          single = true
        )(null)
      )(solved)
    )
  }

  test("finds all shortest paths") {
    planFor("MATCH (a), (b), allShortestPaths((a)-[r]->(b)) RETURN b")._2 should equal(
      FindShortestPaths(
        CartesianProduct(
          AllNodesScan("a", Set.empty)(solved),
          AllNodesScan("b", Set.empty)(solved)
        )(solved),
        ShortestPathPattern(
          Some("  FRESHID16"),
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
            Set(IdName("b")),
            Expand(
              NodeByLabelScan(IdName("a"), lblName("X"), Set.empty)(solved),
              IdName("a"), SemanticDirection.INCOMING, Seq.empty, IdName("b"), IdName("r1"), ExpandAll)(solved),
            Expand(
              NodeByLabelScan(IdName("c"), lblName("X"), Set.empty)(solved),
              IdName("c"), SemanticDirection.INCOMING, Seq.empty, IdName("b"), IdName("r2"), ExpandAll)(solved)
          )(solved)
        )(solved),
        ShortestPathPattern(Some(IdName("p")), PatternRelationship("r", ("a", "c"), SemanticDirection.OUTGOING, Seq.empty, SimplePatternLength), single = true)(null))(solved)

    result should equal(expected)
  }
}
