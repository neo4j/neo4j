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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.{LeafPlanFromExpressions, LeafPlansForVariable}
import org.neo4j.cypher.internal.ir.{InterestingOrder, QueryGraph, Selections}
import org.neo4j.cypher.internal.logical.plans.{Distinct, Union}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class OrLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("two predicates on the same variable can be used") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val inner1 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(context.planningAttributes, "x")
    val p2 = newMockedLogicalPlan(context.planningAttributes, "x")
    val e1 = varFor("e1")
    val e2 = varFor("e2")
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p2))))
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val expected = Distinct(source = Union(p1, p2), groupingExpressions = Map("x" -> varFor("x")))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors(e1, e2)))

    orPlanner.apply(queryGraph, InterestingOrder.empty, context) should equal(Seq(expected))
  }

  test("two predicates on different variables are not used") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val inner = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(context.planningAttributes, "x")
    val p2 = newMockedLogicalPlan(context.planningAttributes, "x")
    val e1 = varFor("e1")
    val e2 = varFor("e2")
    when(inner.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("e1", Set(p1))))
    when(inner.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("e2", Set(p2))))
    val orPlanner = OrLeafPlanner(Seq(inner))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors(e1, e2)))

    orPlanner.apply(queryGraph, InterestingOrder.empty, context) should equal(Seq.empty)
  }

  test("two predicates, where one cannot be leaf-plan-solved, is not used") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val inner = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(context.planningAttributes, "x")
    val e1 = varFor("e1")
    val e2 = varFor("e2")
    when(inner.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("e1", Set(p1))))
    when(inner.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set.empty[LeafPlansForVariable])
    val orPlanner = OrLeafPlanner(Seq(inner))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors(e1, e2)))

    orPlanner.apply(queryGraph, InterestingOrder.empty, context) should equal(Seq.empty)
  }

  test("two predicates that produce two plans each") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val inner1 = mock[LeafPlanFromExpressions]
    val inner2 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(context.planningAttributes, "x", "a")
    val p2 = newMockedLogicalPlan(context.planningAttributes, "x", "b")
    val p3 = newMockedLogicalPlan(context.planningAttributes, "x", "c")
    val p4 = newMockedLogicalPlan(context.planningAttributes, "x", "d")

    val e1 = varFor("e1")
    val e2 = varFor("e2")
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p2))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p3))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p4))))
    val orPlanner = OrLeafPlanner(Seq(inner1, inner2))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors(e1, e2)))

    val expected1 = Distinct(source = Union(p1, p2), groupingExpressions = Map("x" -> varFor("x")))
    val expected2 = Distinct(source = Union(p1, p4), groupingExpressions = Map("x" -> varFor("x")))
    val expected3 = Distinct(source = Union(p3, p2), groupingExpressions = Map("x" -> varFor("x")))
    val expected4 = Distinct(source = Union(p3, p4), groupingExpressions = Map("x" -> varFor("x")))

    orPlanner.apply(queryGraph, InterestingOrder.empty, context) should equal(Seq(expected1, expected2, expected3, expected4))
  }

  test("two predicates that produce two plans each mk 2") {
    val context = newMockedLogicalPlanningContext(newMockedPlanContext())

    val inner1 = mock[LeafPlanFromExpressions]
    val inner2 = mock[LeafPlanFromExpressions]
    val p1 = newMockedLogicalPlan(context.planningAttributes, "x", "a")
    val p2 = newMockedLogicalPlan(context.planningAttributes, "x", "b")
    val p3 = newMockedLogicalPlan(context.planningAttributes, "x", "c")

    val e1 = varFor("e1")
    val e2 = varFor("e2")
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p1))))
    when(inner1.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p2))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e1)), any(), any(), any())).thenReturn(Set(LeafPlansForVariable("x", Set(p3))))
    when(inner2.producePlanFor(ArgumentMatchers.eq(Set(e2)), any(), any(), any())).thenReturn(Set.empty[LeafPlansForVariable])
    val orPlanner = OrLeafPlanner(Seq(inner1, inner2))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors(e1, e2)))

    val expected1 = Distinct(source = Union(p1, p2), groupingExpressions = Map("x" -> varFor("x")))
    val expected2 = Distinct(source = Union(p3, p2), groupingExpressions = Map("x" -> varFor("x")))

    orPlanner.apply(queryGraph, InterestingOrder.empty, context) should equal(Seq(expected1, expected2))
  }
}
