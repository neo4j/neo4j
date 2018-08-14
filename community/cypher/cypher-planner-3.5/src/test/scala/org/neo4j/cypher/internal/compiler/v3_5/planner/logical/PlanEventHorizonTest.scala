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

import org.neo4j.cypher.internal.compiler.v3_5.planner.{FakePlan, LogicalPlanningTestSupport2, ProcedureCallProjection}
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.opencypher.v9_0.ast.{AscSortItem, ProcedureResultItem}
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.util.symbols._
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class PlanEventHorizonTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should do projection if necessary") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(Projection(inputPlan, Map("a" -> literal)))
    }
  }

  test("should not do projection if index provides the value already") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("n.prop" -> property)))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(inputPlan)
    }
  }

  test("should do renaming projection if index provides the value already, but with another name") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("foo" -> property)))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(Projection(inputPlan, Map("foo" -> varFor("n.prop"))))
    }
  }

  test("should do renaming aggregation if index provides the value already") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = AggregatingQueryProjection(Map("n.prop" -> property), Map.empty))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(Aggregation(inputPlan, Map("n.prop" -> varFor("n.prop")), Map.empty))
    }
  }

  test("should do renaming aggregation if index provides the value already, but with another name") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = AggregatingQueryProjection(Map.empty, Map("foo" -> property)))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(Aggregation(inputPlan, Map.empty, Map("foo" -> varFor("n.prop"))))
    }
  }

  test("should do renaming distinct if index provides the value already") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = DistinctQueryProjection(Map("n.prop" -> property)))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(Distinct(inputPlan, Map("n.prop" -> varFor("n.prop"))))
    }
  }

  test("should do renaming distinct if index provides the value already, but with another name") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = DistinctQueryProjection(Map("foo" -> property)))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(Distinct(inputPlan, Map("foo" -> varFor("n.prop"))))
    }
  }

  test("should do renaming unwind if index provides the value already") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val property = prop("n", "prop")
      val pq = RegularPlannerQuery(horizon = UnwindProjection("foo", ListLiteral(Seq(property))(pos)))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(UnwindCollection(inputPlan, "foo", ListLiteral(Seq(varFor("n.prop")))(pos)))
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
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(ProcedureCall(inputPlan, call))
    }
  }

  test("should plan renaming procedure calls  if index provides the value already") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (cfg, context) =>
      val ns = Namespace(List("my", "proc"))(pos)
      val name = ProcedureName("foo")(pos)
      val qualifiedName = QualifiedName(ns.parts, name.name)
      val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
      val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
      val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty))
      val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))

      val property = prop("n", "prop")

      val call = ResolvedCall(signature, Seq(ListLiteral(Seq(property))(pos)), callResults)(pos)
      val pq = RegularPlannerQuery(horizon = ProcedureCallProjection(call))
      val inputPlan = FakePlan(Set("n.prop"), Map(property -> "n.prop"))

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, new StubSolveds, new StubCardinalities)

      // Then
      producedPlan should equal(ProcedureCall(inputPlan, ResolvedCall(signature, Seq(ListLiteral(Seq(varFor("n.prop")))(pos)), callResults)(pos)))
    }
  }

  test("should plan entire projection if there is no pre-projection") {
    // Given
    new given().withLogicalPlanningContext { (cfg, context, solveds, _) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val sortItems = Seq(AscSortItem(Variable("a")(pos))(pos))
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> Variable("a")(pos), "b" -> literal, "c" -> literal), QueryShuffle(sortItems)))
      val inputPlan = Argument(Set("a"))
      solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, solveds, new StubCardinalities)

      // Then
      producedPlan should equal(Projection(Sort(inputPlan, Seq(Ascending("a"))), Map("b" -> literal, "c" -> literal)))
    }
  }

  test("should plan partial projection if there is a pre-projection for sorting") {
    // Given
    new given().withLogicalPlanningContext { (cfg, context, solveds, _) =>
      val literal = SignedDecimalIntegerLiteral("42")(pos)
      val sortItems = Seq(AscSortItem(Variable("a")(pos))(pos))
      val pq = RegularPlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal, "b" -> literal, "c" -> literal), QueryShuffle(sortItems)))
      val inputPlan = Argument()
      solveds.set(inputPlan.id, PlannerQuery.empty)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, context, solveds, new StubCardinalities)

      // Then
      producedPlan should equal(Projection(Sort(Projection(inputPlan, Map("a" -> literal)), Seq(Ascending("a"))), Map("b" -> literal, "c" -> literal)))
    }
  }
}
