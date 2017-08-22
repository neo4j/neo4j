/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.mockito.Matchers
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v3_3.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LeafPlanFromExpressions
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LeafPlansForVariable
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.frontend.v3_3.ast.Ors
import org.neo4j.cypher.internal.frontend.v3_3.ast.Variable
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_3.IdName
import org.neo4j.cypher.internal.ir.v3_3.QueryGraph
import org.neo4j.cypher.internal.ir.v3_3.Selections

class OrLeafPlannerTest extends CypherFunSuite with LogicalPlanningTestSupport {

  implicit val context: LogicalPlanningContext = newMockedLogicalPlanningContext(newMockedPlanContext)

  test("two predicates on the same variable can be used") {
    val inner1 = mock[LeafPlanFromExpressions]
    val p1     = newMockedLogicalPlan("x")
    val p2     = newMockedLogicalPlan("x")
    val e1     = Variable("e1")(pos)
    val e2     = Variable("e2")(pos)
    val ors    = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p1))))
    when(inner1.producePlanFor(Matchers.eq(Set(e2)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p2))))
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val expected = Aggregation(left = Union(p1, p2)(solved),
                               groupingExpressions = Map("x" -> Variable("x")(pos)),
                               aggregationExpression = Map.empty)(solved)

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    orPlanner.apply(queryGraph)(newMockedLogicalPlanningContext(newMockedPlanContext)) should equal(Seq(expected))
  }

  test("two predicates on different variables are not used") {
    val inner1 = mock[LeafPlanFromExpressions]
    val p1     = newMockedLogicalPlan("x")
    val p2     = newMockedLogicalPlan("x")
    val e1     = Variable("e1")(pos)
    val e2     = Variable("e2")(pos)
    val ors    = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("e1"), Set(p1))))
    when(inner1.producePlanFor(Matchers.eq(Set(e2)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("e2"), Set(p2))))
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    orPlanner.apply(queryGraph)(context) should equal(Seq.empty)
  }

  test("two predicates, where one cannot be leaf-plan-solved, is not used") {
    val inner1 = mock[LeafPlanFromExpressions]
    val p1     = newMockedLogicalPlan("x")
    val e1     = Variable("e1")(pos)
    val e2     = Variable("e2")(pos)
    val ors    = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("e1"), Set(p1))))
    when(inner1.producePlanFor(Matchers.eq(Set(e2)), any())(any())).thenReturn(Set.empty[LeafPlansForVariable])
    val orPlanner = OrLeafPlanner(Seq(inner1))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    orPlanner.apply(queryGraph)(context) should equal(Seq.empty)
  }

  test("two predicates that produce two plans each") {
    val inner1 = mock[LeafPlanFromExpressions]
    val inner2 = mock[LeafPlanFromExpressions]
    val p1     = newMockedLogicalPlan("x", "a")
    val p2     = newMockedLogicalPlan("x", "b")
    val p3     = newMockedLogicalPlan("x", "c")
    val p4     = newMockedLogicalPlan("x", "d")

    val e1  = Variable("e1")(pos)
    val e2  = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p1))))
    when(inner1.producePlanFor(Matchers.eq(Set(e2)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p2))))
    when(inner2.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p3))))
    when(inner2.producePlanFor(Matchers.eq(Set(e2)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p4))))
    val orPlanner = OrLeafPlanner(Seq(inner1, inner2))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    val expected1 = Aggregation(left = Union(p1, p2)(solved),
                                groupingExpressions = Map("x" -> Variable("x")(pos)),
                                aggregationExpression = Map.empty)(solved)
    val expected2 = Aggregation(left = Union(p1, p4)(solved),
                                groupingExpressions = Map("x" -> Variable("x")(pos)),
                                aggregationExpression = Map.empty)(solved)
    val expected3 = Aggregation(left = Union(p3, p2)(solved),
                                groupingExpressions = Map("x" -> Variable("x")(pos)),
                                aggregationExpression = Map.empty)(solved)
    val expected4 = Aggregation(left = Union(p3, p4)(solved),
                                groupingExpressions = Map("x" -> Variable("x")(pos)),
                                aggregationExpression = Map.empty)(solved)

    orPlanner.apply(queryGraph)(context) should equal(Seq(expected1, expected2, expected3, expected4))
  }

  test("two predicates that produce two plans each mk 2") {
    val inner1 = mock[LeafPlanFromExpressions]
    val inner2 = mock[LeafPlanFromExpressions]
    val p1     = newMockedLogicalPlan("x", "a")
    val p2     = newMockedLogicalPlan("x", "b")
    val p3     = newMockedLogicalPlan("x", "c")

    val e1  = Variable("e1")(pos)
    val e2  = Variable("e2")(pos)
    val ors = Ors(Set(e1, e2))(pos)
    when(inner1.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p1))))
    when(inner1.producePlanFor(Matchers.eq(Set(e2)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p2))))
    when(inner2.producePlanFor(Matchers.eq(Set(e1)), any())(any()))
      .thenReturn(Set(LeafPlansForVariable(IdName("x"), Set(p3))))
    when(inner2.producePlanFor(Matchers.eq(Set(e2)), any())(any())).thenReturn(Set.empty[LeafPlansForVariable])
    val orPlanner = OrLeafPlanner(Seq(inner1, inner2))

    val queryGraph = QueryGraph.empty.withSelections(Selections.from(ors))

    val expected1 = Aggregation(left = Union(p1, p2)(solved),
                                groupingExpressions = Map("x" -> Variable("x")(pos)),
                                aggregationExpression = Map.empty)(solved)
    val expected3 = Aggregation(left = Union(p3, p2)(solved),
                                groupingExpressions = Map("x" -> Variable("x")(pos)),
                                aggregationExpression = Map.empty)(solved)

    orPlanner.apply(queryGraph)(context) should equal(Seq(expected1, expected3))
  }
}
