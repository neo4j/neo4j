/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.opencypher.v9_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v3_5.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.{PlannerQuery, RegularPlannerQuery}
import org.opencypher.v9_0.util.{Cardinality, LabelId, PropertyKeyId}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.expressions._

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
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("c") => 3000.0
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes == Set("d") => 4000.0
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
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternNodes.size == 1 => 1000.0
        case _                                                                   => 1.0
      }

    } getLogicalPlanFor "MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2")._2 should equal(
      Selection(Seq(Not(Equals(Variable("r1")_,Variable("r2")_)_)_),
        Expand(
          Expand(
            AllNodesScan("a",Set.empty),
           "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r2",ExpandAll),
          "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r1", ExpandInto)
        )
    )
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {

    def myCardinality(plan: PlannerQuery): Cardinality = Cardinality(plan match {
      case RegularPlannerQuery(queryGraph, _, _) if !queryGraph.selections.isEmpty  => 10
      case _ => 1000
    })

    (new given {
      cardinality = PartialFunction(myCardinality)
    } getLogicalPlanFor "MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a")._2 should equal(
        Expand(
          Selection(
            Seq(In(Property(Variable("a")_, PropertyKeyName("name")_)_, ListLiteral(Seq(StringLiteral("Andres")_))_)_),
            AllNodesScan("a", Set.empty)
          ),
          "a", SemanticDirection.BOTH, Seq(RelTypeName("x")_), "start", "rel"
      )
    )
  }

  test("Should build plans expanding from the more expensive side if that is requested by using a hint") {
    (new given {
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.selections.predicates.size == 2 => 1000.0
        case _                => 10.0
      }

      indexOn("Person", "name")
    } getLogicalPlanFor "MATCH (a)-[r]->(b) USING INDEX b:Person(name) WHERE b:Person AND b.name = 'Andres' return r")._2 should equal(
        Expand(
          NodeIndexSeek("b", LabelToken("Person", LabelId(0)), Seq(PropertyKeyToken("name", PropertyKeyId(0))), SingleQueryExpression(StringLiteral("Andres")_), Set.empty),
          "b", SemanticDirection.INCOMING, Seq.empty, "a", "r"
        )
    )
  }
}
