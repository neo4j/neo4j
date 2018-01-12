/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.PlanningAttributes.TransactionLayers
import org.neo4j.cypher.internal.compiler.v3_4.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.frontend.v3_4.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticTable
import org.neo4j.cypher.internal.ir.v3_4.{CardinalityEstimation, RegularPlannerQuery, RegularQueryProjection}
import org.neo4j.cypher.internal.planner.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.util.v3_4.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, DummyPosition}
import org.neo4j.cypher.internal.v3_4.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.v3_4.logical.plans._

class PlanEventHorizonTest extends CypherFunSuite {

  implicit val idGen = new SequentialIdGen()

  val pos = DummyPosition(1)
  val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[Metrics.CardinalityModel], LogicalPlan.LOWEST_TX_LAYER, new TransactionLayers, idGen),
    mock[Metrics], SemanticTable(), mock[QueryGraphSolver], notificationLogger = mock[InternalNotificationLogger])

  test("should do projection if necessary") {
    // Given
    val literal = SignedDecimalIntegerLiteral("42")(pos)
    val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
    val inputPlan = Argument()(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = PlanEventHorizon(pq, inputPlan, context)

    // Then
    producedPlan should equal(Projection(inputPlan, Map("a" -> literal))(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1))))
  }

  test("should plan procedure calls") {
    // Given
    val literal = SignedDecimalIntegerLiteral("42")(pos)
    val call = mock[ResolvedCall]
    val pq = RegularPlannerQuery(horizon = ProcedureCallProjection(call))
    val inputPlan = Argument()(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = PlanEventHorizon(pq, inputPlan, context)

    // Then
    producedPlan should equal(ProcedureCall(inputPlan, call)(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1))))
  }
}
