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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.ast.Union.UnionMapping
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.ForeachPattern
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.ProvidedOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AtMostOneRow
import org.neo4j.cypher.internal.logical.plans.DistinctColumns
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NotDistinct
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.InternalException
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.ListSet

class LogicalPlanProducerTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp
    with TableDrivenPropertyChecks {

  test("should rename provided order of property columns in projection if property projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x", "foo")))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      // when
      val result = lpp.planRegularProjection(plan, projections, Some(projections), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("xfoo")).fromLeft
      )
    }
  }

  test("should rename provided order of property columns in projection if node projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x", "foo")))
      // projection
      val projections = Map("y" -> varFor("x"))

      // when
      val result = lpp.planRegularProjection(plan, projections, Some(projections), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(prop(
        "y",
        "foo"
      )).fromLeft)
    }
  }

  test("should rename provided order of variable columns in projection if variable projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(varFor("x")))
      // projection
      val projections = Map("y" -> varFor("x"))

      // when
      val result = lpp.planRegularProjection(plan, projections, Some(projections), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("y")).fromLeft
      )
    }
  }

  test("should rename provided order of variable columns in projection if cached node property is projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x", "foo")))
      // projection
      val projections = Map("carrot" -> cachedNodeProp("x", "foo"))

      // when
      val result = lpp.planRegularProjection(plan, projections, Some(projections), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("carrot")).fromLeft
      )
    }
  }

  test("should rename provided order of property columns in distinct if property projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x", "foo")))
      // projection
      val projections = Map("xfoo" -> prop("x", "foo"))

      // when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("xfoo")).fromLeft
      )
    }
  }

  test("should rename provided order of function if function projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "id(n)")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(id(varFor("n"))))
      // projection
      val projections = Map("id(n)" -> id(varFor("n")))

      // when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("id(n)")).fromLeft
      )
    }
  }

  test("should trim provided order (1 column) of property column if a sort column is also not a grouping column") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, ProvidedOrder.asc(prop("x", "foo")))

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  test(
    "should trim provided order (2 columns) in aggregation of property column if a sort column is also not a grouping column"
  ) {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(varFor("y")).asc(prop("x", "foo"))
      )

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("y" -> varFor("y"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("y")).fromLeft
      )
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(varFor("y")).asc(prop("x", "foo"))
      )

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("z")).fromLeft
      )
    }
  }

  test("should trim provided order (2 columns) in aggregation and only keep exact grouping column matches") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(prop("y", "bar")).asc(prop("x", "foo"))
      )

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> varFor("y"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename property") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(prop("y", "bar")).asc(prop("x", "foo"))
      )

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> prop("y", "bar"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("z")).fromLeft
      )
    }
  }

  test("should trim provided order (2 columns) in aggregation of property column and rename cached node property") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(prop("y", "bar")).asc(prop("x", "foo"))
      )

      val aggregations = Map("xfoo" -> prop("x", "foo"))
      val groupings = Map("z" -> cachedNodeProp("y", "bar"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("z")).fromLeft
      )
    }
  }

  test("should trim provided order in left outer hash join after variable access") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "z.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        lhs.id,
        ProvidedOrder.asc(prop("z", "bar")).desc(varFor("x"))
      )
      val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y.bar", "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        rhs.id,
        ProvidedOrder.asc(prop("y", "bar")).asc(varFor("x")).asc(prop("x", "foo"))
      )

      val joinColumns = Set("x")

      // when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(prop(
        "y",
        "bar"
      )).fromRight)
    }
  }

  test("should trim provided order in left outer hash join after property access") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "z.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        lhs.id,
        ProvidedOrder.asc(prop("z", "bar")).desc(varFor("x"))
      )
      val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y.bar", "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        rhs.id,
        ProvidedOrder.asc(prop("y", "bar")).asc(prop("x", "foo")).asc(prop("y", "foo"))
      )

      val joinColumns = Set("x")

      // when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(prop(
        "y",
        "bar"
      )).fromRight)
    }
  }

  test("should trim provided order in left outer hash join after complex property access") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "z.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        lhs.id,
        ProvidedOrder.asc(prop("z", "bar")).desc(varFor("x"))
      )
      val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y.bar", "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        rhs.id,
        ProvidedOrder.asc(prop("y", "bar")).asc(add(literalInt(10), prop("x", "foo"))).asc(prop("y", "foo"))
      )

      val joinColumns = Set("x")

      // when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.asc(prop(
        "y",
        "bar"
      )).fromRight)
    }
  }

  test(
    "should trim provided order (2 columns) in aggregation of function column if a sort column is also not a grouping column"
  ) {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "size(x)", "y")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(varFor("y")).asc(function("size", varFor("x")))
      )

      val aggregations = Map("size(x)" -> function("size", varFor("x")))
      val groupings = Map("y" -> varFor("y"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("y")).fromLeft
      )
    }
  }

  test(
    "should trim provided order (2 columns) in aggregation of property of property column if a sort column is also not a grouping column"
  ) {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo.bar", "y")

      val propOfProp = prop(prop("x", "foo"), "bar")

      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        ProvidedOrder.asc(varFor("y")).asc(propOfProp)
      )

      val aggregations = Map("xfoobar" -> propOfProp)
      val groupings = Map("y" -> varFor("y"))

      // when
      val result = lpp.planAggregation(plan, groupings, aggregations, groupings, aggregations, None, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        ProvidedOrder.asc(varFor("y")).fromLeft
      )
    }
  }

  test("Create should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planCreate(ctx.lhs, CreatePattern(Seq(CreateNode(varFor("n"), Set(), None))), ctx.context)
    )
  }

  test("MERGE ... ON MATCH should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planMerge(
        ctx.lhs,
        Seq(CreateNode(varFor("n"), Set(), None)),
        Seq.empty,
        Seq(SetNodePropertyPattern(varFor("x"), PropertyKeyName("p")(pos), literalInt(1))),
        Seq.empty,
        Set.empty,
        ctx.context
      )
    )
  }

  test("MERGE without ON MATCH should not eliminate provided order") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planMerge(
        ctx.lhs,
        Seq(CreateNode(varFor("n"), Set(), None)),
        Seq.empty,
        Seq.empty,
        Seq.empty,
        Set.empty,
        ctx.context
      )
    )
  }

  test("DeleteNode should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeleteNode(ctx.lhs, DeleteExpression(varFor("n"), false), ctx.context)
    )
  }

  test("DeleteRelationship should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeleteRelationship(ctx.lhs, DeleteExpression(varFor("r"), false), ctx.context)
    )
  }

  test("DeletePath should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeletePath(ctx.lhs, DeleteExpression(varFor("p"), false), ctx.context)
    )
  }

  test("DeleteExpression should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeleteExpression(ctx.lhs, DeleteExpression(varFor("x"), false), ctx.context)
    )
  }

  test("Setlabel should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetLabel(ctx.lhs, SetLabelPattern(varFor("n"), Seq(labelName("N"))), ctx.context)
    )
  }

  test("RemoveLabel should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planRemoveLabel(ctx.lhs, RemoveLabelPattern(varFor("n"), Seq(labelName("N"))), ctx.context)
    )
  }

  test("SetProperty should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetProperty(
        ctx.lhs,
        SetPropertyPattern(varFor("x"), PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("SetPropertiesFromMap should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetPropertiesFromMap(
        ctx.lhs,
        SetPropertiesFromMapPattern(varFor("x"), mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("SetNodeProperty should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetNodeProperty(
        ctx.lhs,
        SetNodePropertyPattern(varFor("x"), PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("SetNodePropertiesFromMap should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetNodePropertiesFromMap(
        ctx.lhs,
        SetNodePropertiesFromMapPattern(varFor("x"), mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("SetRelationshipProperty should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetRelationshipProperty(
        ctx.lhs,
        SetRelationshipPropertyPattern(varFor("x"), PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("SetRelationshipPropertiesFromMap should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetRelationshipPropertiesFromMap(
        ctx.lhs,
        SetRelationshipPropertiesFromMapPattern(varFor("r"), mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("ProcedureCall RW should eliminate provided order") {
    val writer = ProcedureSignature(
      QualifiedName(Seq(), "writer"),
      IndexedSeq(),
      None,
      None,
      ProcedureReadWriteAccess,
      id = 0
    )
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planProcedureCall(ctx.lhs, ResolvedCall(writer, Seq(), IndexedSeq())(pos), ctx.context)
    )
  }

  test("ProcedureCall RO should retain provided order") {
    val reader = ProcedureSignature(
      QualifiedName(Seq(), "reader"),
      IndexedSeq(),
      None,
      None,
      ProcedureReadOnlyAccess,
      id = 1
    )

    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planProcedureCall(ctx.lhs, ResolvedCall(reader, Seq(), IndexedSeq())(pos), ctx.context)
    )
  }

  test("CartesianProduct should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planCartesianProduct(ctx.lhs, ctx.rhsWithUpdate, ctx.context)
    )
  }

  test("CartesianProduct should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planCartesianProduct(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("Apply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planApply(ctx.lhs, ctx.rhsWithUpdate, ctx.context)
    )
  }

  test("Apply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planApply(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("uncorrelated Subquery should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithUpdate,
        ctx.context,
        correlated = false,
        yielding = true,
        inTransactionsParameters = None
      )
    )
  }

  test("uncorrelated Subquery should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ctx.context,
        correlated = false,
        yielding = true,
        inTransactionsParameters = None
      )
    )
  }

  test("correlated Subquery should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithUpdate,
        ctx.context,
        correlated = true,
        yielding = true,
        inTransactionsParameters = None
      )
    )
  }

  test("correlated Subquery should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ctx.context,
        correlated = true,
        yielding = true,
        inTransactionsParameters = None
      )
    )
  }

  test("uncorrelated unit Subquery should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithUpdate,
        ctx.context,
        correlated = false,
        yielding = false,
        inTransactionsParameters = None
      )
    )
  }

  test("uncorrelated unit Subquery should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ctx.context,
        correlated = false,
        yielding = false,
        inTransactionsParameters = None
      )
    )
  }

  test("correlated unit Subquery should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithUpdate,
        ctx.context,
        correlated = true,
        yielding = false,
        inTransactionsParameters = None
      )
    )
  }

  test("correlated unit Subquery should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ctx.context,
        correlated = true,
        yielding = false,
        inTransactionsParameters = None
      )
    )
  }

  test("ForListSubqueryExpressionSolver.planApply fail when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.ForSubqueryExpressionSolver.planApply(ctx.lhs, ctx.rhsWithUpdate, ctx.context)
    )
  }

  test("ForListSubqueryExpressionSolver.planApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.ForSubqueryExpressionSolver.planApply(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("ForListSubqueryExpressionSolver.planRollup should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.ForSubqueryExpressionSolver.planRollup(ctx.lhs, ctx.rhsWithUpdate, "x", "y", ctx.context)
    )
  }

  test("ForListSubqueryExpressionSolver.planRollup should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.ForSubqueryExpressionSolver.planRollup(ctx.lhs, ctx.rhsWithoutUpdate, "x", "y", ctx.context)
    )
  }

  test("TriadicSelection should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planTriadicSelection(
        positivePredicate = true,
        ctx.lhs,
        "a",
        "b",
        "c",
        ctx.rhsWithUpdate,
        varFor("x"),
        ctx.context
      )
    )
  }

  test("TriadicSelection should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planTriadicSelection(
        positivePredicate = true,
        ctx.lhs,
        "a",
        "b",
        "c",
        ctx.rhsWithoutUpdate,
        varFor("x"),
        ctx.context
      )
    )
  }

  test("ConditionalApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planConditionalApply(ctx.lhs, ctx.rhsWithUpdate, Seq("a"), ctx.context)
    )
  }

  test("ConditionalApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planConditionalApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq("a"), ctx.context)
    )
  }

  test("AntiConditionalApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planAntiConditionalApply(ctx.lhs, ctx.rhsWithUpdate, Seq("a"), ctx.context)
    )
  }

  test("AntiConditionalApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planAntiConditionalApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq("a"), ctx.context)
    )
  }

  test("TailApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planTailApply(ctx.lhs, ctx.rhsWithUpdate, ctx.context)
    )
  }

  test("TailApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planTailApply(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("InputApply should fail when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planInputApply(ctx.lhs, ctx.rhsWithUpdate, Seq(varFor("x")), ctx.context)
    )
  }

  test("InputApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planInputApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq(varFor("x")), ctx.context)
    )
  }

  test("ForeachApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planForeachApply(
        ctx.lhs,
        ctx.rhsWithUpdate,
        ForeachPattern(varFor("x"), varFor("x"), SinglePlannerQuery.empty),
        ctx.context,
        varFor("x")
      )
    )
  }

  test("ForeachApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planForeachApply(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ForeachPattern(varFor("x"), varFor("x"), SinglePlannerQuery.empty),
        ctx.context,
        varFor("x")
      )
    )
  }

  test("SemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSemiApply(ctx.lhs, ctx.rhsWithUpdate, varFor("x"), ctx.context)
    )
  }

  test("SemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, varFor("x"), ctx.context)
    )
  }

  test("AntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, varFor("x"), ctx.context)
    )
  }

  test("AntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, varFor("x"), ctx.context)
    )
  }

  test("LetSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetSemiApply(ctx.lhs, ctx.rhsWithUpdate, "x", ctx.context)
    )
  }

  test("LetSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, "x", ctx.context)
    )
  }

  test("LetAntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, "x", ctx.context)
    )
  }

  test("LetAntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, "x", ctx.context)
    )
  }

  test("SelectOrSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSelectOrSemiApply(ctx.lhs, ctx.rhsWithUpdate, varFor("x"), ctx.context)
    )
  }

  test("SelectOrSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSelectOrSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, varFor("x"), ctx.context)
    )
  }

  test("SelectOrAntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, varFor("x"), ctx.context)
    )
  }

  test("SelectOrAntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, varFor("x"), ctx.context)
    )
  }

  test("LetSelectOrSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetSelectOrSemiApply(ctx.lhs, ctx.rhsWithUpdate, "x", varFor("x"), ctx.context)
    )
  }

  test("LetSelectOrSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetSelectOrSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, "x", varFor("x"), ctx.context)
    )
  }

  test("LetSelectOrAntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, "x", varFor("x"), ctx.context)
    )
  }

  test("LetSelectOrAntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, "x", varFor("x"), ctx.context)
    )
  }

  test("SemiApplyInHorizon should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSemiApplyInHorizon(
        ctx.lhs,
        ctx.rhsWithUpdate,
        simpleExistsExpression(
          patternForMatch(nodePat(Some("x"))),
          None,
          introducedVariables = Set(varFor("x"))
        ),
        ctx.context
      )
    )
  }

  test("SemiApplyInHorizon should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSemiApplyInHorizon(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        simpleExistsExpression(
          patternForMatch(nodePat(Some("x"))),
          None,
          introducedVariables = Set(varFor("x"))
        ),
        ctx.context
      )
    )
  }

  test("AntiSemiApplyInHorizon should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planAntiSemiApplyInHorizon(
        ctx.lhs,
        ctx.rhsWithUpdate,
        simpleExistsExpression(
          patternForMatch(nodePat(Some("x"))),
          None,
          introducedVariables = Set(varFor("x"))
        ),
        ctx.context
      )
    )
  }

  test("AntiSemiApplyInHorizon should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planAntiSemiApplyInHorizon(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        simpleExistsExpression(
          patternForMatch(nodePat(Some("x"))),
          None,
          introducedVariables = Set(varFor("x"))
        ),
        ctx.context
      )
    )
  }

  case class PlanCreationContext(
    producer: LogicalPlanProducer,
    context: LogicalPlanningContext,
    lhs: LogicalPlan,
    rhsWithUpdate: LogicalPlan,
    rhsWithoutUpdate: LogicalPlan
  )

  private def shouldEliminateProvidedOrder(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val result = getPlan(context, createPlan)
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(ProvidedOrder.empty)
    }
  }

  private def shouldRetainProvidedOrder(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val result = getPlan(context, createPlan)
      val lhsOrder = context.staticComponents.planningAttributes.providedOrders.get(result.lhs.get.id)
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(lhsOrder.fromLeft)
    }
  }

  private def shouldFailAssertion(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      intercept[AssertionError](getPlan(context, createPlan))
    }
  }

  private def getPlan(context: LogicalPlanningContext, createPlan: PlanCreationContext => LogicalPlan) = {
    val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
    val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
    val providedOrder = ProvidedOrder.asc(varFor("y")).asc(varFor("x"))
    context.staticComponents.planningAttributes.providedOrders.set(lhs.id, providedOrder)

    val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "a")
    val rhsWithUpdate = lpp.planSetLabel(rhs, SetLabelPattern(varFor("n"), Seq(labelName("N"))), context)
    createPlan(PlanCreationContext(lpp, context, lhs, rhsWithUpdate, rhs))
  }

  test("should mark leveraged order in plans and their origin") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val initialOrder = ProvidedOrder.asc(varFor("x"))
      // plan with provided order
      def plan() = {
        val p = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
        context.staticComponents.planningAttributes.providedOrders.set(p.id, initialOrder)
        p
      }
      def plan2() = {
        val p = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
        context.staticComponents.planningAttributes.providedOrders.set(p.id, initialOrder)
        p
      }

      val vx = varFor("x")
      val x_vx = Map("x" -> vx)
      val foo_vx = Map("foo" -> vx)
      val foo_collect = Map("foo" -> FunctionInvocation(vx, FunctionName(Collect.name)(pos)))
      val interesting_vx = InterestingOrder.required(RequiredOrderCandidate.asc(vx))
      val one = literalInt(1)
      val unionMappings =
        List(UnionMapping(varFor("x"), varFor("x"), varFor("x")), UnionMapping(varFor("y"), varFor("y"), varFor("y")))

      // when
      val resultsAndNames = Seq(
        (
          "PartialSort",
          lpp.planPartialSort(
            plan(),
            Seq(Ascending(varFor("x"))),
            Seq(Ascending(varFor("y"))),
            initialOrder.asc(varFor("y")).columns,
            InterestingOrder.empty,
            context
          )
        ),
        (
          "OrderedAggregation with grouping",
          lpp.planOrderedAggregation(plan(), x_vx, foo_vx, Seq(vx), x_vx, foo_vx, context)
        ),
        ("OrderedDistinct", lpp.planOrderedDistinct(plan(), foo_vx, Seq(vx), foo_vx, context)),
        ("OrderedUnion", lpp.planOrderedUnion(plan(), plan2(), unionMappings, Seq(Ascending(varFor("x"))), context)),
        (
          "OrderedDistinct for Union",
          lpp.planOrderedDistinctForUnion(
            lpp.planOrderedUnion(plan(), plan2(), unionMappings, Seq(Ascending(varFor("x"))), context),
            Seq(vx),
            context
          )
        ),
        (
          "Limit for aggregation",
          lpp.planLimitForAggregation(plan(), x_vx, foo_vx, InterestingOrder.empty, context).lhs.get
        ), // Get the Limit under the Optional
        ("Limit", lpp.planLimit(plan(), one, one, interesting_vx, context)),
        ("Skip", lpp.planSkip(plan(), one, interesting_vx, context)),
        (
          "Collect with previous required order",
          lpp.planAggregation(plan(), Map.empty, foo_collect, Map.empty, foo_collect, Some(interesting_vx), context)
        ),
        ("ProduceResult", lpp.planProduceResult(plan(), Seq("x"), Some(interesting_vx), context))
      )

      // then
      resultsAndNames.foreach { case (name, result) =>
        withClue(name) {
          context.staticComponents.planningAttributes.leveragedOrders.get(result.id) should be(true)
          result.lhs.foreach { lhs =>
            context.staticComponents.planningAttributes.leveragedOrders.get(lhs.id) should be(true)
          }
          result.rhs.foreach { rhs =>
            context.staticComponents.planningAttributes.leveragedOrders.get(rhs.id) should be(true)
          }
        }
      }
    }
  }

  test("should traverse tree towards order origin when marking leveraged order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val initialOrder = ProvidedOrder.asc(varFor("x"))

      val leaf1 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val leaf2 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val p1 = lpp.planSort(leaf1, Seq(Ascending(varFor("x"))), initialOrder.columns, InterestingOrder.empty, context)
      val p2 = lpp.planEager(p1, context, ListSet.empty)
      val p3 = lpp.planRightOuterHashJoin(Set("x"), leaf2, p2, Set.empty, context)

      // when
      val result = lpp.planProduceResult(
        p3,
        Seq("x"),
        Some(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))),
        context
      )

      // then
      context.staticComponents.planningAttributes.leveragedOrders.get(result.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(p3.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(p2.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(p1.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(leaf1.id) should be(false)
      context.staticComponents.planningAttributes.leveragedOrders.get(leaf2.id) should be(false)
    }
  }

  test("should traverse tree towards multiple order origins when marking leveraged order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val initialOrder = ProvidedOrder.asc(varFor("x"))

      val leaf1 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val leaf2 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val sort1 =
        lpp.planSort(leaf1, Seq(Ascending(varFor("x"))), initialOrder.columns, InterestingOrder.empty, context)
      val sort2 =
        lpp.planSort(leaf2, Seq(Ascending(varFor("x"))), initialOrder.columns, InterestingOrder.empty, context)
      val u = lpp.planOrderedUnion(sort1, sort2, List(), Seq(Ascending(varFor("x"))), context)

      // when
      val result = lpp.planProduceResult(
        u,
        Seq("x"),
        Some(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x")))),
        context
      )

      // then
      context.staticComponents.planningAttributes.leveragedOrders.get(result.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(u.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(sort1.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(sort2.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(leaf1.id) should be(false)
      context.staticComponents.planningAttributes.leveragedOrders.get(leaf2.id) should be(false)
    }
  }

  test("should retain solved hints when planning union for leaf plans") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val lhs = fakeLogicalPlanFor("x", "y")
      val rhs = fakeLogicalPlanFor("x", "y")
      val hint1 = UsingIndexHint(varFor("foo"), labelOrRelTypeName("bar"), Seq())(InputPosition.NONE)
      val hint2 = UsingIndexHint(varFor("blah"), labelOrRelTypeName("meh"), Seq())(InputPosition.NONE)

      val solveds = context.staticComponents.planningAttributes.solveds
      val spqLhs = SinglePlannerQuery.empty.amendQueryGraph(qg => qg.addHints(Seq(hint1)))
      val spqRhs = SinglePlannerQuery.empty.amendQueryGraph(qg => qg.addHints(Seq(hint2)))

      solveds.set(lhs.id, spqLhs)
      context.staticComponents.planningAttributes.providedOrders.set(lhs.id, ProvidedOrder.empty)

      solveds.set(rhs.id, spqRhs)
      context.staticComponents.planningAttributes.providedOrders.set(rhs.id, ProvidedOrder.empty)

      val p1 = lpp.planUnion(lhs, rhs, List(), context)
      val p2 = lpp.planDistinctForUnion(p1, context)
      val p3 = lpp.updateSolvedForOr(p2, QueryGraph(), context)

      solveds.get(p3.id).allHints shouldBe (Set(hint1, hint2))
      context.staticComponents.planningAttributes.providedOrders.get(p3.id) shouldBe (ProvidedOrder.empty)
    }
  }

  test("should validate the inner plan against the quantified path pattern when planning Trail") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val producer = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val sourcePlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "source")
      val innerPlan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "inner")

      val quantifiedPathPattern = QuantifiedPathPattern(
        leftBinding = NodeBinding(v"n", v"anon_0"),
        rightBinding = NodeBinding(v"m", v"anon_1"),
        patternRelationships =
          NonEmptyList(PatternRelationship(
            v"r",
            (v"n", v"m"),
            SemanticDirection.OUTGOING,
            Nil,
            SimplePatternLength
          )),
        repetition = Repetition(min = 1, max = UpperBound.Unlimited),
        nodeVariableGroupings = Set(VariableGrouping(v"n", v"n"), VariableGrouping(v"m", v"m")),
        relationshipVariableGroupings = Set(VariableGrouping(v"r", v"r"))
      )

      the[InternalException] thrownBy producer.planTrail(
        source = sourcePlan,
        pattern = quantifiedPathPattern,
        startBinding = quantifiedPathPattern.leftBinding,
        endBinding = quantifiedPathPattern.rightBinding,
        maybeHiddenFilter = None,
        context = context,
        innerPlan = innerPlan,
        predicates = Nil,
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        reverseGroupVariableProjections = false
      ) should have message "The provided inner plan doesn't conform with the quantified path pattern being planned"
    }
  }

  test("providedOrderOfApply: is correct") {
    val po_empty = ProvidedOrder.empty
    val po_a = ProvidedOrder.asc(varFor("a"))
    val po_b = ProvidedOrder.asc(varFor("b"))
    val po_c = ProvidedOrder.asc(varFor("c"))
    val po_ab = ProvidedOrder.asc(varFor("a")).asc(varFor("b"))
    val po_abc = ProvidedOrder.asc(varFor("a")).asc(varFor("b")).asc(varFor("c"))
    val d_a = DistinctColumns(varFor("a"))
    val d_ab = DistinctColumns(varFor("a"), varFor("b"))

    val providedOrderOfApplyTest = Table(
      ("leftProvidedOrder", "rightProvidedOrder", "leftDistinctness", "expectedProvidedOrder"),
      // NotDistinct
      (po_empty, po_empty, NotDistinct, po_empty),
      (po_a, po_empty, NotDistinct, po_a.fromLeft),
      (po_empty, po_a, NotDistinct, po_empty),
      (po_a, po_b, NotDistinct, po_a.fromLeft),
      // AtMostOneRow
      (po_empty, po_empty, AtMostOneRow, po_empty),
      (po_a, po_empty, AtMostOneRow, po_a.fromLeft),
      (po_empty, po_a, AtMostOneRow, po_a.fromRight),
      (po_a, po_b, AtMostOneRow, po_ab.fromBoth),
      // Distinct columns
      (po_a, po_b, d_a, po_ab.fromBoth),
      (po_ab, po_c, d_a, po_abc.fromBoth),
      (po_a, po_b, d_ab, po_a.fromLeft),
      (po_ab, po_c, d_ab, po_abc.fromBoth)
    )

    forAll(providedOrderOfApplyTest) {
      case (leftProvidedOrder, rightProvidedOrder, leftDistinctness, expectedProvidedOrder) =>
        LogicalPlanProducer.providedOrderOfApply(
          leftProvidedOrder,
          rightProvidedOrder,
          leftDistinctness
        ) should equal(expectedProvidedOrder)
    }
  }

  test("Planning OrderedDistinct should not assume that leveraged order attributes are 'unseen'") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val varX = varFor("x")
      val fooToX = Map("foo" -> varX)
      val initialOrder = ProvidedOrder.asc(varX)

      val leaf = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      val p = lpp.planSort(leaf, Seq(Ascending(varX)), initialOrder.columns, InterestingOrder.empty, context)
      val p1 = lpp.planOrderedDistinct(p, fooToX, Seq(varX), fooToX, context)
      context.staticComponents.planningAttributes.leveragedOrders.get(p1.id) // observe leveraged order
      lpp.planOrderedDistinct(p1, fooToX, Seq(varX), fooToX, context) // should not crash
    }
  }
}
