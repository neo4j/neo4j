/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.pipes

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.Monitors
import org.neo4j.cypher.internal.compiler.v2_2.planner.execution.{PipeExecutionBuilderContext, PipeExecutionPlanBuilder}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityInput
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{AllNodesScan, CartesianProduct, LogicalPlan}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{LogicalPlanningTestSupport2, PlannerQuery, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.helpers.Clock

class EstimationIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should amend estimated produced rows when seeing a cartesian product") {
    givenNodes(42)

    val plan = CartesianProduct(
      AllNodesScan("a", Set.empty)(solved),
      AllNodesScan("b", Set.empty)(solved)
    )(solved)

    val result = builder.build(plan)

    result.pipe.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0 * 42.0)
    result.pipe.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0)

    val (lhs :: rhs :: Nil) = result.pipe.sources

    lhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    lhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0)

    rhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    rhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0)
  }

  test("should amend estimated produced rows when seeing 2 cartesian products nested on the left") {
    givenNodes(42)

    val plan = CartesianProduct(
      CartesianProduct(
        AllNodesScan("a", Set.empty)(solved),
        AllNodesScan("b", Set.empty)(solved)
      )(solved),
      AllNodesScan("c", Set.empty)(solved)
    )(solved)

    val result = builder.build(plan)

    result.pipe.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0 * 42.0 * 42.0)
    result.pipe.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0 * 42.0)

    val (lhs :: rhs :: Nil) = result.pipe.sources

    lhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0 * 42.0)
    lhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0)

    rhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    rhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0 * 42.0)

    val (innerLhs :: innerRhs :: Nil) = lhs.sources

    innerLhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    innerLhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0)

    innerRhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    innerRhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0)
  }

  test("should amend estimated produced rows when seeing 2 cartesian products nested on the right") {
    givenNodes(42)

    val plan = CartesianProduct(
      AllNodesScan("c", Set.empty)(solved),
      CartesianProduct(
        AllNodesScan("a", Set.empty)(solved),
        AllNodesScan("b", Set.empty)(solved)
      )(solved)
    )(solved)

    val result = builder.build(plan)

    result.pipe.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0 * 42.0 * 42.0)
    result.pipe.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0 * 42.0)

    val (lhs :: rhs :: Nil) = result.pipe.sources

    lhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    lhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0)

    rhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0 * 42.0)
    rhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0 * 42.0)

    val (innerLhs :: innerRhs :: Nil) = rhs.sources

    innerLhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    innerLhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0)

    innerRhs.asInstanceOf[RonjaPipe].estimation.operatorCardinality.get should equal(42.0)
    innerRhs.asInstanceOf[RonjaPipe].estimation.producedRows.get should equal(42.0 * 42.0 * 42.0)
  }

  private var builder: PipeExecutionPlanBuilder = null
  private implicit var ctx: PipeExecutionBuilderContext = null
  private implicit val planCtx = mock[PlanContext]
  private val solved = mock[PlannerQuery]

  private def givenNodes(nodes: Long) {
    def model(plan: LogicalPlan, input: QueryGraphCardinalityInput): Cardinality = plan match {
      case _: AllNodesScan => Cardinality(nodes)
      case CartesianProduct(lhs, rhs) => model(lhs, input) * model(rhs, input)
    }

    builder = new PipeExecutionPlanBuilder(mock[Clock], mock[Monitors])
    ctx = new PipeExecutionBuilderContext(model, mock[SemanticTable])
  }
}
