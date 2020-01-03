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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_5.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.ast.AscSortItem
import org.neo4j.cypher.internal.v3_5.ast.ProcedureResultItem
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class PlanEventHorizonTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  test("should do projection if necessary") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context)

      // Then
      producedPlan should equal(Projection(inputPlan, Map("a" -> literal)))
    }
  }

  test("should plan procedure calls") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val ns = Namespace(List("my", "proc"))(pos)
      val name = ProcedureName("foo")(pos)
      val qualifiedName = QualifiedName(ns.parts, name.name)
      val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
      val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
      val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
      val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))

      val call = ResolvedCall(signature, Seq.empty, callResults)(pos)
      val pq = RegularPlannerQuery(horizon = ProcedureCallProjection(call))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context)

      // Then
      producedPlan should equal(ProcedureCall(inputPlan, call))
    }
  }

  test("should plan entire projection if there is no pre-projection") {
    // Given
    new given().withLogicalPlanningContext { (cfg, context) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val sortItems = Seq(AscSortItem(Variable("a")(pos))(pos))
      val interestingOrder = InterestingOrder.asc("a")
      val horizon = RegularQueryProjection(Map("a" -> Variable("a")(pos), "b" -> literal, "c" -> literal), QueryShuffle(sortItems))
      val pq = RegularPlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "a")
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context)

      // Then
      producedPlan should equal(Projection(Sort(inputPlan, Seq(Ascending("a"))), Map("b" -> literal, "c" -> literal)))
    }
  }

  test("should plan partial projection if there is a pre-projection for sorting") {
    // Given
    new given().withLogicalPlanningContext { (cfg, context) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val sortItems = Seq(AscSortItem(Variable("a")(pos))(pos))
      val interestingOrder = InterestingOrder.asc("a")
      val horizon = RegularQueryProjection(Map("a" -> literal, "b" -> literal, "c" -> literal), QueryShuffle(sortItems))
      val pq = RegularPlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes)
      context.planningAttributes.solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context)

      // Then
      producedPlan should equal(Projection(Sort(Projection(inputPlan, Map("a" -> literal)), Seq(Ascending("a"))), Map("b" -> literal, "c" -> literal)))
    }
  }
}
