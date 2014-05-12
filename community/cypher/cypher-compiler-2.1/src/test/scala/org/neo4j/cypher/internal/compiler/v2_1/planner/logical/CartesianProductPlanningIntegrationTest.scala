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
import org.neo4j.cypher.internal.compiler.v2_1
import v2_1.planner.LogicalPlanningTestSupport
import v2_1.planner.logical.plans._
import v2_1.ast._
import v2_1.LabelId
import v2_1.planner.logical.Metrics.{SelectivityModel, CardinalityModel}
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock
import org.mockito.Matchers._
import org.mockito.Mockito._

class CartesianProductPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport  {

  test("should build plans for simple cartesian product") {
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMockedMetricsFactory)
    produceLogicalPlan("MATCH n, m RETURN n, m") should equal(
      CartesianProduct(AllNodesScan(IdName("m")), AllNodesScan(IdName("n")))
    )
  }

  test("should build plans for simple cartesian product with a predicate on the elements") {
    val factory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: AllNodesScan     => 1000
      case _: NodeByLabelScan  => 100
      case _: Selection        => 500
      case _                   => Double.MaxValue
    })

    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(factory)
    when(planContext.getOptLabelId("Label")).thenReturn(None)
    when(planContext.getOptPropertyKeyId("prop")).thenReturn(None)

    produceLogicalPlan("MATCH n, m WHERE n.prop = 12 AND m:Label RETURN n, m") should equal(
      CartesianProduct(
        Selection(
          Seq(Equals(Property(Identifier("n")_, PropertyKeyName("prop")_)_, SignedIntegerLiteral("12")_)_),
          AllNodesScan("n")
        ),
        NodeByLabelScan("m", Left("Label"))
      )
    )
  }

  test("should build plans with cartesian joins such that cross product cardinality is minimized") {
    val labelIdA = Right(LabelId(30))
    val labelIdB = Right(LabelId(10))
    val labelIdC = Right(LabelId(20))

    val factory = newMockedMetricsFactory
    def f(plan: LogicalPlan): Double = plan match {
      case _: AllNodesScan                             => 1000
      case NodeByLabelScan(_, Right(LabelId(labelId))) => labelId
      case CartesianProduct(left, right)               => f(left) * f(right)
      case _                                           => Double.MaxValue
    }
    when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn(f _)

    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("A")).thenReturn(Some(30))
    when(planContext.getOptLabelId("B")).thenReturn(Some(10))
    when(planContext.getOptLabelId("C")).thenReturn(Some(20))

    produceLogicalPlan("MATCH a, b, c WHERE a:A AND b:B AND c:C RETURN a, b, c") should equal(
      CartesianProduct(
        CartesianProduct(
          NodeByLabelScan("b", labelIdB),
          NodeByLabelScan("c", labelIdC)
        ),
        NodeByLabelScan("a", labelIdA)
      )
    )
  }

  ignore("should build plans with cartesian joins such that cross product cardinality is minimized according to selectivity") {
    val labelIdA = Right(LabelId(30))
    val labelIdB = Right(LabelId(20))
    val labelIdC = Right(LabelId(10))

    val factory = newMockedMetricsFactory
    when(factory.newSelectivityEstimator(any(), any())).thenReturn((expression: Expression) => expression match {
      case Equals(Property(Identifier(lhs), _), Property(Identifier(rhs), _)) =>
        (lhs, rhs) match {
          case ("a", "b") => /* 60 */ 0.5 // => 30
          case ("b", "c") => /* 20 */ 1.0 // => 20
          case ("a", "c") => /* 30 */ 0.5 // => 15
        }
    })

    when(factory.newCardinalityEstimator(any(), any(), any())).thenAnswer(new Answer[CardinalityModel] {
      def answer(invocation: InvocationOnMock): CardinalityModel = {
        val selectivity = invocation.getArguments()(1).asInstanceOf[SelectivityModel]
        new CardinalityModel {
          def apply(plan: LogicalPlan): Double = plan match {
            case _: AllNodesScan                             => 1000
            case NodeByLabelScan(_, Right(LabelId(labelId))) => labelId
            case CartesianProduct(left, right)               => apply(left) * apply(right)
            case Selection(predicates, left)                 => predicates.foldLeft(1.0)(_ * selectivity(_)) * apply(left)
            case _                                           => Double.MaxValue
          }
        }
      }
    })

    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(factory)

    when(planContext.getOptLabelId("A")).thenReturn(Some(30))
    when(planContext.getOptLabelId("B")).thenReturn(Some(20))
    when(planContext.getOptLabelId("C")).thenReturn(Some(10))
    when(planContext.getOptPropertyKeyId("x")).thenReturn(None)

    produceLogicalPlan("MATCH a, b, c WHERE a:A AND b:B AND c:C AND a.x = b.x AND b.x = c.x AND a.x = c.x RETURN a, b, c") should equal(
      Selection(
        predicates = Seq(
          Equals(
            Property(Identifier("a")_, PropertyKeyName("x")_)_,
            Property(Identifier("b")_, PropertyKeyName("x")_)_
          )_,
          Equals(
            Property(Identifier("b")_, PropertyKeyName("x")_)_,
            Property(Identifier("c")_, PropertyKeyName("x")_)_
          )_
        ),
        left = CartesianProduct(
          NodeByLabelScan("b", labelIdB),
          Selection(
            predicates = Seq(Equals(
              Property(Identifier("a")_, PropertyKeyName("x")_)_,
              Property(Identifier("c")_, PropertyKeyName("x")_)_
            )_),
            left = CartesianProduct(
              NodeByLabelScan("c", labelIdC),
              NodeByLabelScan("a", labelIdA)
            )
          )
        )
      )
    )
  }
}
