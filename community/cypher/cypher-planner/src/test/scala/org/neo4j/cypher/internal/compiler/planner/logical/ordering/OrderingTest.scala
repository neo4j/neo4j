/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.ordering

import org.mockito.Answers.RETURNS_DEEP_STUBS
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderingTest extends CypherFunSuite with AstConstructionTestSupport {

  implicit val noPlan: Option[LogicalPlan] = None
  implicit val poFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory

  private var id = 0

  private def plan(name: String, providedOrder: ProvidedOrder, mockContext: LogicalPlanningContext): LogicalPlan = {
    val plan = new LogicalPlanBuilder(false, initialId = id).fakeLeafPlan(name).build()
    id += 1
    when(mockContext.staticComponents.planningAttributes.providedOrders.apply(plan.id)).thenReturn(providedOrder)
    plan
  }

  test("2 plans, sorted by same 1 column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should equal(
      Seq(Ascending(v"x"))
    )
  }

  test("2 plans, sorted by same 1 column, but not a variable") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(prop("x", "prop")), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(prop("x", "prop")), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should be(empty)
  }

  test("2 plans, sorted by same 1 column, but not same direction") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.desc(v"x"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should be(empty)
  }

  test("2 plans, sorted by different 1 column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"y"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should be(empty)
  }

  test("2 plans, sorted by same 2 columns") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x").asc(v"y"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x").asc(v"y"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should equal(
      Seq(Ascending(v"x"), Ascending(v"y"))
    )
  }

  test("2 plans, sorted by same 2 columns, but 2nd is not a variable") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x").asc(prop("x", "prop")), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x").asc(prop("x", "prop")), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should equal(
      Seq(Ascending(v"x"))
    )
  }

  test("2 plans, sorted by same 2 columns, but 1st is not a variable") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(prop("x", "prop")).asc(v"y"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(prop("x", "prop")).asc(v"y"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should be(empty)
  }

  test("2 plans, sorted by same 1 column and then 1 different column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x").asc(v"y"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x").asc(v"z"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2), context) should equal(
      Seq(Ascending(v"x"))
    )
  }

  test("3 plans, sorted by same 1 column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p3 = plan("p3", DefaultProvidedOrderFactory.asc(v"x"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2, p3), context) should equal(
      Seq(Ascending(v"x"))
    )
  }

  test("3 plans, 2 sorted by same 1 column, 1 sorted by a different column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x"), context)
    val p3 = plan("p3", DefaultProvidedOrderFactory.asc(v"y"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2, p3), context) should be(empty)
  }

  test("3 plans, sorted by same 2 columns and then 1 different column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x").asc(v"y").asc(v"z0"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x").asc(v"y").asc(v"z0"), context)
    val p3 = plan("p3", DefaultProvidedOrderFactory.asc(v"x").asc(v"y").asc(v"z1"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2, p3), context) should equal(
      Seq(Ascending(v"x"), Ascending(v"y"))
    )
  }

  test("3 plans, sorted by same 2 columns and then sometimes 1 extra column") {
    val context = mock[LogicalPlanningContext](RETURNS_DEEP_STUBS)
    val p1 = plan("p1", DefaultProvidedOrderFactory.asc(v"x").asc(v"y").asc(v"z0"), context)
    val p2 = plan("p2", DefaultProvidedOrderFactory.asc(v"x").asc(v"y").asc(v"z0"), context)
    val p3 = plan("p3", DefaultProvidedOrderFactory.asc(v"x").asc(v"y"), context)

    Ordering.orderedUnionColumns(Seq(p1, p2, p3), context) should equal(
      Seq(Ascending(v"x"), Ascending(v"y"))
    )
  }
}
