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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.ProvidedOrder
import org.neo4j.cypher.internal.v3_5.logical.plans.CachedNodeProperty
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class LogicalPlanProducerTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should rename provided order of property columns in projection if property projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("x.foo"))))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("xfoo"))))
    }
  }

  test("should rename provided order of property columns in projection if node projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("x.foo"))))
      // projection
      val projections = Map("y" -> varFor("x"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("y.foo"))))
    }
  }

  test("should rename provided order of variable columns in projection if variable projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("x"))))
      // projection
      val projections = Map("y" -> varFor("x"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("y"))))
    }
  }

  test("should rename provided order of variable columns in projection if cached node property is projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("x.foo"))))
      // projection
      val projections = Map("carrot" -> cachedProp("x", "foo"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("carrot"))))
    }
  }

  test("should rename provided order of property columns in distinct if property projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("x.foo"))))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      //when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("xfoo"))))
    }
  }

  test("should trim provided order (1 column) of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("x.foo"))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("y"), ProvidedOrder.Asc("x.foo"))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("y"))))
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("y"), ProvidedOrder.Asc("x.foo"))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("z"))))
    }
  }

  test("should trim provided order (2 columns) in aggregation and only keep exact grouping column matches") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("y.bar"), ProvidedOrder.Asc("x.foo"))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename property") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("y.bar"), ProvidedOrder.Asc("x.foo"))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> prop("y", "bar"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("z"))))
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename cached node property") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc("y.bar"), ProvidedOrder.Asc("x.foo"))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> cachedProp("y", "bar"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("z"))))
    }
  }

  test("should trim provided order (3 columns) in left outer hash join") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.planningAttributes, "x", "z.bar")
      context.planningAttributes.providedOrders.set(lhs.id, ProvidedOrder(Seq(ProvidedOrder.Asc("z.bar"), ProvidedOrder.Desc("x"))))
      val rhs = fakeLogicalPlanFor(context.planningAttributes, "x", "y.bar", "x.foo")
      context.planningAttributes.providedOrders.set(rhs.id, ProvidedOrder(Seq(ProvidedOrder.Asc("y.bar"), ProvidedOrder.Asc("x"), ProvidedOrder.Desc("x.foo"))))

      val joinColumns = Set("x")

      //when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Nil, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc("y.bar"))))
    }
  }

  private def cachedProp(node: String, prop: String) =
    CachedNodeProperty(node, PropertyKeyName(prop)(pos))(pos)
}
