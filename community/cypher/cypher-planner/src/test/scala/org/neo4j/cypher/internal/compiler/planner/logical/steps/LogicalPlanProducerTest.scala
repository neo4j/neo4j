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
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LogicalPlanProducerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp{

  test("should rename provided order of property columns in projection if property projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x","foo")))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("xfoo")).fromLeft)
    }
  }

  test("should rename provided order of property columns in projection if node projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x","foo")))
      // projection
      val projections = Map("y" -> varFor("x"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(prop("y","foo")).fromLeft)
    }
  }

  test("should rename provided order of variable columns in projection if variable projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(varFor("x")))
      // projection
      val projections = Map("y" -> varFor("x"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("y")).fromLeft)
    }
  }

  test("should rename provided order of variable columns in projection if cached node property is projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x","foo")))
      // projection
      val projections = Map("carrot" -> cachedNodeProp("x", "foo"))

      //when
      val result = lpp.planRegularProjection(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("carrot")).fromLeft)
    }
  }

  test("should rename provided order of property columns in distinct if property projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x","foo")))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      //when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("xfoo")).fromLeft)
    }
  }

  test("should rename provided order of function if function projected") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "id(n)")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(id(varFor("n"))))
      // projection
      val projections = Map("id(n)" -> id(varFor("n")))

      //when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("id(n)")).fromLeft)
    }
  }

  test("should trim provided order (1 column) of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x","foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(varFor("y")).asc(prop("x","foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("y")).fromLeft)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(varFor("y")).asc(prop("x","foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("z")).fromLeft)
    }
  }

  test("should trim provided order (2 columns) in aggregation and only keep exact grouping column matches") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("y","bar")).asc(prop("x","foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename property") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("y","bar")).asc(prop("x","foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> prop("y", "bar"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("z")).fromLeft)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename cached node property") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo", "y.bar")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("y","bar")).asc(prop("x","foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> cachedNodeProp("y", "bar"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("z")).fromLeft)
    }
  }

  test("should trim provided order (3 columns) in left outer hash join") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.planningAttributes, "x", "z.bar")
      context.planningAttributes.providedOrders.set(lhs.id, ProvidedOrder.asc(prop("z","bar")).desc(varFor("x")))
      val rhs = fakeLogicalPlanFor(context.planningAttributes, "x", "y.bar", "x.foo")
      context.planningAttributes.providedOrders.set(rhs.id, ProvidedOrder.asc(prop("y","bar")).asc(varFor("x")).desc(prop("x","foo")))

      val joinColumns = Set("x")

      //when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(prop("y","bar")).fromRight)
    }
  }

  test("should trim provided order (2 columns) in aggregation of function column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "size(x)", "y")
      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(varFor("y")).asc(function("size", varFor("x"))))

      val aggregations = Map("size(x)" -> function("size", varFor("x")))
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("y")).fromLeft)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property of property column if a sort column is also not a grouping column") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.planningAttributes, "x.foo.bar", "y")

      val propOfProp = prop(prop("x","foo"), "bar")

      context.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(varFor("y")).asc(propOfProp))

      val aggregations = Map("xfoobar" -> propOfProp)
      val groupings = Map("y" -> varFor("y"))

      //when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(varFor("y")).fromLeft)
    }
  }

  test("should mark leveraged order in plans and their origin") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)

      val initialOrder = ProvidedOrder.asc(varFor("x"))
      // plan with provided order
      def plan() = {
        val p = fakeLogicalPlanFor(context.planningAttributes, "x", "y")
        context.planningAttributes.providedOrders.set(p.id, initialOrder)
        p
      }

      val vx = varFor("x")
      val x_vx = Map("x" -> vx)
      val foo_vx = Map("foo" -> vx)
      val foo_collect = Map("foo" -> FunctionInvocation(vx, FunctionName(Collect.name)(pos)))
      val interesting_vx = InterestingOrder.required(RequiredOrderCandidate.asc(vx))
      val one = literalInt(1)

      //when
      val resultsAndNames = Seq(
        ("PartialSort", lpp.planPartialSort(plan(), Seq(Ascending("x")), Seq(Ascending("y")), initialOrder.asc(varFor("y")).columns, InterestingOrder.empty, context)),
        ("OrderedAggregation with grouping", lpp.planOrderedAggregation(plan(), x_vx, foo_vx, Seq(vx), x_vx, foo_vx, context)),
        ("OrderedAggregation no grouping", lpp.planOrderedAggregation(plan(), Map.empty, foo_vx, Seq(vx), Map.empty, foo_vx, context)),
        ("Limit for aggregation", lpp.planLimitForAggregation(plan(), DoNotIncludeTies, x_vx, foo_vx, InterestingOrder.empty, context).lhs.get), // Get the Limit under the Optional
        ("Limit", lpp.planLimit(plan(), one, one, interesting_vx, DoNotIncludeTies, context)),
        ("Skip", lpp.planSkip(plan(), one, interesting_vx, context)),
        ("Collect with previous required order", lpp.planAggregation(plan(), Map.empty, foo_collect, Map.empty, foo_collect, Some(interesting_vx), context)),
        ("ProduceResult", lpp.planProduceResult(plan(), Seq("x"), Some(interesting_vx),context)),
      )

      // then
      resultsAndNames.foreach { case (name, result) =>
        withClue(name) {
          context.planningAttributes.leveragedOrders.get(result.id) should be(true)
          result.lhs.foreach { lhs => context.planningAttributes.leveragedOrders.get(lhs.id) should be(true) }
        }
      }
    }
  }

  test("should traverse tree towards order origin when marking leveraged order") {
    new given().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.planningAttributes, idGen)

      val initialOrder = ProvidedOrder.asc(varFor("x"))

      val leaf1 = fakeLogicalPlanFor(context.planningAttributes, "x", "y")
      val leaf2 = fakeLogicalPlanFor(context.planningAttributes, "x", "y")
      val p1 = lpp.planSort(leaf1, Seq(Ascending("x")), initialOrder.columns, InterestingOrder.empty, context)
      val p2 = lpp.planEager(p1, context)
      val p3 = lpp.planRightOuterHashJoin(Set("x"), leaf2, p2, Set.empty, context)

      // when
      val result = lpp.planProduceResult(p3, Seq("x"), Some(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))), context)

      // then
      context.planningAttributes.leveragedOrders.get(result.id) should be(true)
      context.planningAttributes.leveragedOrders.get(p3.id) should be(true)
      context.planningAttributes.leveragedOrders.get(p2.id) should be(true)
      context.planningAttributes.leveragedOrders.get(p1.id) should be(true)
      context.planningAttributes.leveragedOrders.get(leaf1.id) should be(false)
      context.planningAttributes.leveragedOrders.get(leaf2.id) should be(false)
    }
  }
}
