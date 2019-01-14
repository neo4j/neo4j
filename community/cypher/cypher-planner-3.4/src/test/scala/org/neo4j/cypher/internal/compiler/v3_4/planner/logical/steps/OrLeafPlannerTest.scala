/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.{LeafPlanFromExpressions, LeafPlansForVariable}
import org.neo4j.cypher.internal.ir.v3_4.{QueryGraph, Selections}
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.{Ors, Variable}
import org.neo4j.cypher.internal.v3_4.logical.plans.{Distinct, Union}

class OrLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("two predicates on the same variable can be used") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(newMockedPlanContext)

    val inner1 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(solveds, cardinalities, "x")
    val p2 = newMockedLogicalPlan(solveds, cardinalities, "x")
    val e1 = Variable("e1")(pos)
    val e2 = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set
    (p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p2))))
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val expected = Distinct(
      source = Union(p1, p2),
      groupingExpressions = Map("x" -> Variable("x")(pos)))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    orPlanner.apply(queryGraph, context, solveds, cardinalities) should equal(Seq(expected))
  }

  test("two predicates on different variables are not used") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(newMockedPlanContext)

    val inner1 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(solveds, cardinalities, "x")
    val p2 = newMockedLogicalPlan(solveds, cardinalities, "x")
    val e1 = Variable("e1")(pos)
    val e2 = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("e1", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set(LeafPlansForVariable("e2", Set(p2))))
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    orPlanner.apply(queryGraph, context, solveds, cardinalities) should equal(Seq.empty)
  }

  test("two predicates, where one cannot be leaf-plan-solved, is not used") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(newMockedPlanContext)

    val inner1 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(solveds, cardinalities, "x")
    val e1 = Variable("e1")(pos)
    val e2 = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("e1", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set.empty[LeafPlansForVariable])
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    orPlanner.apply(queryGraph, context, solveds, cardinalities) should equal(Seq.empty)
  }

  test("two predicates that produce two plans each") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(newMockedPlanContext)

    val inner1 = mock[LeafPlanFromExpressions]
    val inner2 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(solveds, cardinalities, "x", "a")
    val p2 = newMockedLogicalPlan(solveds, cardinalities, "x", "b")
    val p3 = newMockedLogicalPlan(solveds, cardinalities, "x", "c")
    val p4 = newMockedLogicalPlan(solveds, cardinalities, "x", "d")

    val e1 = Variable("e1")(pos)
    val e2 = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p2))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p3))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p4))))
    val orPlanner = OrLeafPlanner(Seq(inner1, inner2))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    val expected1 = Distinct(
      source = Union(p1, p2),
      groupingExpressions = Map("x" -> Variable("x")(pos)))
    val expected2 = Distinct(
      source = Union(p1, p4),
      groupingExpressions = Map("x" -> Variable("x")(pos)))
    val expected3 = Distinct(
      source = Union(p3, p2),
      groupingExpressions = Map("x" -> Variable("x")(pos)))
    val expected4 = Distinct(
      source = Union(p3, p4),
      groupingExpressions = Map("x" -> Variable("x")(pos)))


    orPlanner.apply(queryGraph, context, solveds, cardinalities) should equal(Seq(expected1, expected2, expected3, expected4))
  }

  test("two predicates that produce two plans each mk 2") {
    val (context, solveds, cardinalities) = newMockedLogicalPlanningContext(newMockedPlanContext)

    val inner1 = mock[LeafPlanFromExpressions]
    val inner2 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(solveds, cardinalities, "x", "a")
    val p2 = newMockedLogicalPlan(solveds, cardinalities, "x", "b")
    val p3 = newMockedLogicalPlan(solveds, cardinalities, "x", "c")

    val e1 = Variable("e1")(pos)
    val e2 = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p2))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p3))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any())).thenReturn(Set.empty[LeafPlansForVariable])
    val orPlanner = OrLeafPlanner(Seq(inner1, inner2))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    val expected1 = Distinct(
      source = Union(p1, p2),
      groupingExpressions = Map("x" -> Variable("x")(pos)))
    val expected3 = Distinct(
      source = Union(p3, p2),
      groupingExpressions = Map("x" -> Variable("x")(pos)))


    orPlanner.apply(queryGraph, context, solveds, cardinalities) should equal(Seq(expected1, expected3))
  }
}
