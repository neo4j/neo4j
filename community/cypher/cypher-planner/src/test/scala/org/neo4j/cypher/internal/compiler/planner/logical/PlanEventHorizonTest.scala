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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CallSubqueryHorizon
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class PlanEventHorizonTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should do projection if necessary") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val literal = literalInt(42)
      val pq = RegularSinglePlannerQuery(horizon = RegularQueryProjection(Map("a" -> literal)))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      producedPlan should equal(Projection(inputPlan, Map("a" -> literal)))
    }
  }

  test("should plan procedure calls") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (_, context) =>
      val ns = Namespace(List("my", "proc"))(pos)
      val name = ProcedureName("foo")(pos)
      val qualifiedName = QualifiedName(ns.parts, name.name)
      val signatureInputs = IndexedSeq(FieldSignature("a", CTInteger))
      val signatureOutputs = Some(IndexedSeq(FieldSignature("x", CTInteger), FieldSignature("y", CTList(CTNode))))
      val signature = ProcedureSignature(qualifiedName, signatureInputs, signatureOutputs, None, ProcedureReadOnlyAccess(Array.empty), id = 42)
      val callResults = IndexedSeq(ProcedureResultItem(varFor("x"))(pos), ProcedureResultItem(varFor("y"))(pos))

      val call = ResolvedCall(signature, Seq.empty, callResults)(pos)
      val pq = RegularSinglePlannerQuery(horizon = ProcedureCallProjection(call))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      producedPlan should equal(ProcedureCall(inputPlan, call))
    }
  }

  test("should plan subqueries calls") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (_, context) =>

      val sq = RegularSinglePlannerQuery(QueryGraph(patternNodes = Set("a")), horizon = RegularQueryProjection(Map("a" -> varFor("a"))))

      val pq = RegularSinglePlannerQuery(horizon = CallSubqueryHorizon(sq, correlated = false))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      producedPlan should equal(CartesianProduct(
        inputPlan,
        AllNodesScan("a", Set.empty)
      ))
    }
  }

  test("should plan correlated subqueries calls") {
    // Given
    new given().withLogicalPlanningContextWithFakeAttributes { (_, context) =>

      val sq = RegularSinglePlannerQuery(QueryGraph(patternNodes = Set("a")), horizon = RegularQueryProjection(Map("a" -> varFor("a"))))

      val pq = RegularSinglePlannerQuery(horizon = CallSubqueryHorizon(sq, correlated = true))
      val inputPlan = Argument()

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      producedPlan should equal(Apply(
        inputPlan,
        AllNodesScan("a", Set.empty)
      ))
    }
  }

  test("should plan entire projection if there is no pre-projection") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val literal = literalInt(42)
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a"), Map("a" -> varFor("a"))))
      val horizon = RegularQueryProjection(Map("a" -> varFor("a"), "b" -> literal, "c" -> literal), QueryPagination())
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "a")
      context.planningAttributes.solveds.set(inputPlan.id, SinglePlannerQuery.empty)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      producedPlan should equal(Projection(Sort(inputPlan, Seq(Ascending("a"))), Map("b" -> literal, "c" -> literal)))
    }
  }

  test("should plan partial projection if there is a pre-projection for sorting") {
    // Given
    new given().withLogicalPlanningContext { (_, context) =>
      val literal = literalInt(42)
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("a"), Map("a" -> literal)))
      val horizon = RegularQueryProjection(Map("a" -> literal, "b" -> literal, "c" -> literal), QueryPagination())
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes)
      context.planningAttributes.solveds.set(inputPlan.id, SinglePlannerQuery.empty)

      // When
      val producedPlan = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      producedPlan should equal(Projection(Sort(Projection(inputPlan, Map("a" -> literal)), Seq(Ascending("a"))), Map("b" -> literal, "c" -> literal)))
    }
  }

  test("should add the correct plans when query uses both ORDER BY, SKIP and LIMIT") {
    // given
    val x = literalUnsignedInt(110)
    val y = literalUnsignedInt(10)
    new given().withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))
      val horizon = RegularQueryProjection(Map("x" -> varFor("x")), queryPagination = QueryPagination(skip = Some(y), limit = Some(x)))
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "x")

      // When
      val result = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      val sorted = Sort(inputPlan, Seq(Ascending("x")))
      val limited = Limit(sorted, add(x, y), DoNotIncludeTies)
      val skipped = Skip(limited, y)
      result should equal(skipped)
    }
  }

  test("should add sort without pre-projection for DistinctQueryProjection") {
    // [WITH DISTINCT n, m] WITH n AS n, m AS m, 5 AS notSortColumn ORDER BY m
    val mSortVar = varFor("m")
    val projectionsMap = Map(
      "n" -> varFor("n"),
      mSortVar.name -> mSortVar,
      // a projection that sort will not take care of
      "notSortColumn" -> literalUnsignedInt(5))
    new given().withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("m")))
      val horizon = DistinctQueryProjection(groupingExpressions = projectionsMap)
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = horizon)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "m", "n")

      // When
      val result = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      val distinct = Distinct(inputPlan, groupingExpressions = projectionsMap)
      val sorted = Sort(distinct, Seq(Ascending("m")))
      result should equal(sorted)
    }
  }

  test("should add sort without pre-projection for AggregatingQueryProjection") {
    // [WITH n, m, o] // o is an aggregating expression
    // WITH o, n AS n, m AS m, 5 AS notSortColumn ORDER BY m, o
    val grouping = Map(
      "n" -> varFor("n"),
      "m" -> varFor("m"),
      // a projection that sort will not take care of
      "notSortColumn" -> literalUnsignedInt(5))
    val aggregating = Map("o" -> varFor("o"))

    val projection = AggregatingQueryProjection(
      groupingExpressions = grouping,
      aggregationExpressions = aggregating,
      queryPagination = QueryPagination(skip = None, limit = None)
    )

    new given().withLogicalPlanningContext { (_, context) =>
      val interestingOrder = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("m")).asc(varFor("o")))
      val pq = RegularSinglePlannerQuery(interestingOrder = interestingOrder, horizon = projection)
      val inputPlan = fakeLogicalPlanFor(context.planningAttributes, "m", "n", "o")

      // When
      val result = PlanEventHorizon(pq, inputPlan, None, context)

      // Then
      val aggregation = Aggregation(inputPlan, grouping, aggregating)
      val sorted = Sort(aggregation, Seq(Ascending("m"), Ascending("o")))
      result should equal(sorted)
    }
  }
}
