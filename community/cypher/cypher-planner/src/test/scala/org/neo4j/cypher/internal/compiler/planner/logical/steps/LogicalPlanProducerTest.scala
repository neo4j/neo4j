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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class LogicalPlanProducerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp{

  test("should rename provided order of property columns in projection if property projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("x","foo")))))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("xfoo")))))
    }
  }

  test("should rename provided order of property columns in projection if node projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("x","foo")))))
      // projection
      val projections = Map("y" -> varFor("x"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(prop("y","foo")))))
    }
  }

  test("should rename provided order of variable columns in projection if variable projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("x")))))
      // projection
      val projections = Map("y" -> varFor("x"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")))))
    }
  }

  test("should rename provided order of variable columns in projection if cached node property is projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("x","foo")))))
      // projection
      val projections = Map("carrot" -> cachedNodeProp("x", "foo"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("carrot")))))
    }
  }

  test("should rename provided order of property columns in distinct if property projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("x","foo")))))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      //when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("xfoo")))))
    }
  }

  test("should rename provided order of function if function projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "id(n)")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(id(varFor("n"))))))
      // projection
      val projections = Map("id(n)" -> id(varFor("n")))

      //when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("id(n)")))))
    }
  }

  test("should trim provided order (1 column) of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("x","foo")))))

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
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")), ProvidedOrder.Asc(prop("x","foo")))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")))))
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")), ProvidedOrder.Asc(prop("x","foo")))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("z")))))
    }
  }

  test("should trim provided order (2 columns) in aggregation and only keep exact grouping column matches") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("y","bar")), ProvidedOrder.Asc(prop("x","foo")))))

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
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("y","bar")), ProvidedOrder.Asc(prop("x","foo")))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> prop("y", "bar"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("z")))))
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename cached node property") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("y","bar")), ProvidedOrder.Asc(prop("x","foo")))))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> cachedNodeProp("y", "bar"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("z")))))
    }
  }

  test("should trim provided order (3 columns) in left outer hash join") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.planningAttributes, "x", "z.bar")
      context.planningAttributes.providedOrders.set(lhs.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("z","bar")), ProvidedOrder.Desc(varFor("x")))))
      val rhs = fakeLogicalPlanFor(context.planningAttributes, "x", "y.bar", "x.foo")
      context.planningAttributes.providedOrders.set(rhs.id, ProvidedOrder(Seq(ProvidedOrder.Asc(prop("y","bar")), ProvidedOrder.Asc(varFor("x")), ProvidedOrder.Desc(prop("x","foo")))))

      val joinColumns = Set("x")

      //when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(prop("y","bar")))))
    }
  }

  test("should trim provided order (2 columns) in aggregation of function column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "size(x)", "y")
      context.planningAttributes.providedOrders.set(plan.id,
        ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")),
        ProvidedOrder.Asc(function("size", varFor("x"))))))

      val aggregations = Map("size(x)" -> function("size", varFor("x")))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")))))
    }
  }

  test("should trim provided order (2 columns) in aggregation of property of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo.bar", "y")

      val propOfProp = prop(prop("x","foo"), "bar")

      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")), ProvidedOrder.Asc(propOfProp))))

      val aggregations = Map("xfoobar" -> propOfProp)
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder(Seq(ProvidedOrder.Asc(varFor("y")))))
    }
  }
}
