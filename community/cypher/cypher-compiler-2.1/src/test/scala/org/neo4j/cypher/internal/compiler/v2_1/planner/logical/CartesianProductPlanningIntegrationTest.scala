/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.BeLikeMatcher._

class CartesianProductPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans for simple cartesian product") {
    planFor("MATCH n, m RETURN n, m").plan.plan should equal(
      CartesianProduct(
        AllNodesScan(IdName("m")),
        AllNodesScan(IdName("n")))
    )
  }

  test("should build plans so the cheaper plan is on the left") {
    (new given {
      cost = {
        case _: Selection => Cost(1000)
        case _: NodeByLabelScan => Cost(20)
      }
      cardinality = mapCardinality {
        case _: Selection => 10
        case _: NodeByLabelScan => 10
      }
    } planFor "MATCH n, m WHERE n.prop = 12 AND m:Label RETURN n, m").plan.plan should beLike {
      case CartesianProduct(_: Selection, _: NodeByLabelScan) => ()
    }
  }

  test("should combine three plans so the cost is minimized") {
    implicit val plan = new given {
      labelCardinality = Map(
        "A" -> Cardinality(30),
        "B" -> Cardinality(20),
        "C" -> Cardinality(10)
      )
    } planFor "MATCH a, b, c WHERE a:A AND b:B AND c:C RETURN a, b, c"

    plan.plan.plan should equal(
      CartesianProduct(
        NodeByLabelScan("a", Right(labelId("A"))),
        CartesianProduct(
          NodeByLabelScan("c", Right(labelId("C"))),
          NodeByLabelScan("b", Right(labelId("B")))
        )
      )
    )
  }

  test("should combine two plans so the cost is minimized") {
    implicit val plan = new given {
      labelCardinality = Map(
        "A" -> Cardinality(30),
        "B" -> Cardinality(20)
      )
    } planFor "MATCH a, b WHERE a:A AND b:B RETURN a, b"

    // A x B = 30 * 2 + 30 * (20 * 2) => 1260
    // B x A = 20 * 2 + 20 * (30 * 2) => 1240

    plan.plan.plan should equal(
      CartesianProduct(
        NodeByLabelScan("b", Right(labelId("B"))),
        NodeByLabelScan("a", Right(labelId("A")))
      )
    )
  }
}
