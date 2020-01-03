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

import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.{SinglePlannerQuery, PlannerQueryPart, RegularSinglePlannerQuery}
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions.{RelTypeName, SemanticDirection}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.Cardinality

class ExpandPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("Should build plans containing expand for single relationship pattern") {
    planFor("MATCH (a)-[r]->(b) RETURN r")._2 should equal(
        Expand(
          AllNodesScan("a", Set.empty),
          "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"
        )
    )
  }

  test("Should build plans containing expand for two unrelated relationship patterns") {

    (new given {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("c") => 3000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("d") => 4000.0
        case _ => 100.0
      }
    } getLogicalPlanFor "MATCH (a)-[r1]->(b), (c)-[r2]->(d) RETURN r1, r2")._2 should beLike {
      case
        Selection(_,
          CartesianProduct(
            Expand(
              AllNodesScan("c", _), _, _, _, _, _, _),
            Expand(
              AllNodesScan("a", _), _, _, _, _, _, _)
          )
        ) => ()
    }
  }

  test("Should build plans containing expand for self-referencing relationship patterns") {
    val result = planFor("MATCH (a)-[r]->(a) RETURN r")._2

    result should equal(
      Expand(
        AllNodesScan("a", Set.empty),
        "a", SemanticDirection.OUTGOING, Seq.empty, "a", "r", ExpandInto)
    )
  }

  test("Should build plans containing expand for looping relationship patterns") {
    (new given {
      cardinality = mapCardinality {
        // all node scans
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes.size == 1 => 1000.0
        case _                                                                   => 1.0
      }

    } getLogicalPlanFor "MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2")._2 should equal(
      Selection(ands(not(equals(varFor("r1"), varFor("r2")))),
        Expand(
          Expand(
            AllNodesScan("a",Set.empty),
           "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r2",ExpandAll),
          "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandInto)
        )
    )
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {

    def myCardinality(plan: PlannerQueryPart): Cardinality = Cardinality(plan match {
      case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if !queryGraph.selections.isEmpty  => 10
      case _ => 1000
    })

    (new given {
      cardinality = PartialFunction(myCardinality)
    } getLogicalPlanFor "MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a")._2 should equal(
        Expand(
          Selection(
            ands(equals(prop("a", "name"), literalString("Andres"))),
            AllNodesScan("a", Set.empty)
          ),
          "a", SemanticDirection.BOTH, Seq(RelTypeName("x")_), "start", "rel"
      )
    )
  }

  test("Should build plans expanding from the more expensive side if that is requested by using a hint") {
    (new given {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.selections.predicates.size == 2 => 1000.0
        case _                => 10.0
      }

      indexOn("Person", "name")
    } getLogicalPlanFor "MATCH (a)-[r]->(b) USING INDEX b:Person(name) WHERE b:Person AND b.name = 'Andres' return r")._2 should equal(
        Expand(
          IndexSeek("b:Person(name = 'Andres')"),
          "b", SemanticDirection.INCOMING, Seq.empty, "a", "r"
        )
    )
  }
}
