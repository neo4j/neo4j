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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.frontend.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.{IdName, RegularPlannerQuery}
import org.neo4j.cypher.internal.v3_3.logical.plans.{AllNodesScan, CartesianProduct, NodeByLabelScan, Selection}

class CartesianProductPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans for simple cartesian product") {
    planFor("MATCH (n), (m) RETURN n, m")._2 should equal(
      CartesianProduct(
        AllNodesScan(IdName("n"), Set.empty)(solved),
        AllNodesScan(IdName("m"), Set.empty)(solved)
      )(solved)
    )
  }

  test("should build plans so the cheaper plan is on the left") {
    (new given {
      cost = {
        case (_: Selection, _) => 1000.0
        case (_: NodeByLabelScan, _) => 20.0
      }
      cardinality = mapCardinality {
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.selections.predicates.size == 1 => 10
      }
    } getLogicalPlanFor  "MATCH (n), (m) WHERE n.prop = 12 AND m:Label RETURN n, m")._2 should beLike {
      case CartesianProduct(_: Selection, _: NodeByLabelScan) => ()
    }
  }

  test("should combine three plans so the cost is minimized") {
    implicit val plan = new given {
      labelCardinality = Map(
        "A" -> 30.0,
        "B" -> 20.0,
        "C" -> 10.0
      )
    } getLogicalPlanFor "MATCH (a), (b), (c) WHERE a:A AND b:B AND c:C RETURN a, b, c"

    plan._2 should equal(
      CartesianProduct(
        NodeByLabelScan("a", lblName("A"), Set.empty)(solved),
        CartesianProduct(
          NodeByLabelScan("c", lblName("C"), Set.empty)(solved),
          NodeByLabelScan("b", lblName("B"), Set.empty)(solved)
        )(solved)
      )(solved)
    )
  }

  test("should combine two plans so the cost is minimized") {
    implicit val plan = new given {
      labelCardinality = Map(
        "A" -> 30.0,
        "B" -> 20.0
      )
    } getLogicalPlanFor "MATCH (a), (b) WHERE a:A AND b:B RETURN a, b"

    // A x B = 30 * 2 + 30 * (20 * 2) => 1260
    // B x A = 20 * 2 + 20 * (30 * 2) => 1240

    plan._2 should equal(
      CartesianProduct(
        NodeByLabelScan("b", lblName("B"), Set.empty)(solved),
        NodeByLabelScan("a", lblName("A"), Set.empty)(solved)
      )(solved)
    )
  }
}
