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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical

import org.neo4j.cypher.internal.compiler.v3_3.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps.{CostComparisonListener, LogicalPlanProducer}
import org.neo4j.cypher.internal.compiler.v3_3.spi.PlanContext
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_3.symbols.{CTInteger, CTList, CTNode}
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.{Cardinality, CardinalityEstimation, RegularPlannerQuery, RegularQueryProjection}
import org.neo4j.cypher.internal.v3_3.logical.plans._

class PlanEventHorizonTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val context = LogicalPlanningContext(mock[PlanContext], LogicalPlanProducer(mock[Metrics.CardinalityModel]),
    mock[Metrics], SemanticTable(), mock[QueryGraphSolver], notificationLogger = mock[InternalNotificationLogger],
    costComparisonListener = mock[CostComparisonListener])

  test("should do projection if necessary") {
    // Given
    val literal = SignedDecimalIntegerLiteral("42")(pos)
    val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
    val inputPlan = SingleRow()(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = PlanEventHorizon(pq, inputPlan)

    // Then
    producedPlan should equal(Projection(inputPlan, Map("a" -> literal))(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1))))
  }

  test("should plan procedure calls") {
    // Given
    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
    val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
    val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
    val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))

    val call =  ResolvedCall(signature, Seq.empty, callResults)(pos)

    val pq = RegularPlannerQuery(horizon = ProcedureCallProjection(call))
    val inputPlan = SingleRow()(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1)))

    // When
    val producedPlan = PlanEventHorizon(pq, inputPlan)

    // Then
    producedPlan should equal(ProcedureCall(inputPlan, call)(CardinalityEstimation.lift(RegularPlannerQuery(), Cardinality(1))))
  }
}
