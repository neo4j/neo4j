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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.CartesianProduct
import org.mockito.Mockito._

class CartesianProductPlanningIT extends CypherFunSuite with LogicalPlanningTestSupport  {

  test("should build plans for simple cartesian product") {
    implicit val planner = newPlanner(newMetricsFactory.withCardinalityEstimator {
      case _: AllNodesScan => 1000
    })

    produceLogicalPlan("MATCH n, m RETURN n, m") should equal(
      CartesianProduct(AllNodesScan(IdName("n")), AllNodesScan(IdName("m")))
    )
  }

  test("should build plans for simple cartesian product with a predicate on the elements") {
    implicit val planner = newPlanner(newMetricsFactory.withCardinalityEstimator {
      case _: AllNodesScan => 1000
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("Label")).thenReturn(None)
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(None)

    produceLogicalPlan("MATCH n, m WHERE n.prop = 12 AND m:Label RETURN n, m") should equal(
      CartesianProduct(
        NodeByLabelScan("m", Left("Label"))(),
        Selection(
          Seq(Equals(Property(Identifier("n")_, PropertyKeyName("prop")()_)_, SignedIntegerLiteral("12")_)_),
          AllNodesScan("n"))
      )
    )
  }

  test("should build plans with cartesian joins such that cross product cardinality is minimized") {
    val labelIdA = Right(LabelId(30))
    val labelIdB = Right(LabelId(10))
    val labelIdC = Right(LabelId(20))

    implicit val planner = newPlanner(newMetricsFactory.withCardinalityEstimator {
      case _: AllNodesScan                             => 1000
      case NodeByLabelScan(_, Right(LabelId(labelId))) => labelId
    })

    implicit val planContext = newMockedPlanContext
    when(planContext.getOptLabelId("A")).thenReturn(Some(30))
    when(planContext.getOptLabelId("B")).thenReturn(Some(10))
    when(planContext.getOptLabelId("C")).thenReturn(Some(20))

    produceLogicalPlan("MATCH a, b, c WHERE a:A AND b:B AND c:C RETURN a, b, c") should equal(
      CartesianProduct(
        CartesianProduct(
          NodeByLabelScan("b", labelIdB)(),
          NodeByLabelScan("c", labelIdC)()
        ),
        NodeByLabelScan("a", labelIdA)()
      )
    )
  }
}
