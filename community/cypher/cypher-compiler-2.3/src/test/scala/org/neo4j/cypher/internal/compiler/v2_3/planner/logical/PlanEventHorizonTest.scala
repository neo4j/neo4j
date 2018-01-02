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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{Projection, SingleRow}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps.LogicalPlanProducer
import org.neo4j.cypher.internal.compiler.v2_3.planner.{CardinalityEstimation, PlannerQuery, RegularQueryProjection}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v2_3.ast.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, SemanticTable}

class PlanEventHorizonTest extends CypherFunSuite {

  val pos = DummyPosition(1)
  implicit val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[Metrics.CardinalityModel]), mock[Metrics], SemanticTable(), mock[QueryGraphSolver])

  test("should do projection if necessary") {
    // Given
    val literal: SignedDecimalIntegerLiteral = SignedDecimalIntegerLiteral("42")(pos)
    val pq = PlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
    val inputPlan = SingleRow()(CardinalityEstimation.lift(PlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = PlanEventHorizon()(pq, inputPlan)

    // Then
    producedPlan should equal(Projection(inputPlan, Map("a" -> literal))(CardinalityEstimation.lift(PlannerQuery(), Cardinality(1))))
  }
}
