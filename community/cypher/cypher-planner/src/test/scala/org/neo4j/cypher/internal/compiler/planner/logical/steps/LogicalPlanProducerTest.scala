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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.PlanMatchHelp
import org.neo4j.cypher.internal.compiler.planner.logical.steps.projection.MaybeReportedProjections
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentAsc
import org.neo4j.cypher.internal.expressions.ImpliedLabel
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PartialPredicate
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.DeleteExpression
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.ForeachPattern
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern.CountInteger
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.ir.ordering.RequiredOrderCandidate
import org.neo4j.cypher.internal.logical.builder.TestNFABuilder
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.AtMostOneRow
import org.neo4j.cypher.internal.logical.plans.CachedProperties
import org.neo4j.cypher.internal.logical.plans.DistinctColumns
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA.PathLength
import org.neo4j.cypher.internal.logical.plans.NotDistinct
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoteBatchPropertiesWithFilter
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Selector.Shortest
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.logical.plans.TraversalPathMode.Trail
import org.neo4j.cypher.internal.logical.plans.ordering.DefaultProvidedOrderFactory
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.ordering.ProvidedOrderFactory
import org.neo4j.cypher.internal.util.AssertionRunner
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.exceptions.InternalException
import org.scalatest.prop.TableDrivenPropertyChecks

class LogicalPlanProducerTest extends CypherPlannerTestSuite with LogicalPlanningTestSupport2 with PlanMatchHelp
    with TableDrivenPropertyChecks {

  implicit val noPlan: Option[LogicalPlan] = None
  implicit val poFactory: ProvidedOrderFactory = DefaultProvidedOrderFactory

  test("should rename provided order of property columns in projection if property projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        DefaultProvidedOrderFactory.asc(prop("x", "foo"))
      )
      // projection
      val projections = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

      // when
      val result = lpp.planRegularProjection(plan, projections, MaybeReportedProjections(Some(projections)), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"xfoo").fromLeft
      )
    }
  }

  test("should rename provided order of property columns in projection if node projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        DefaultProvidedOrderFactory.asc(prop("x", "foo"))
      )
      // projection
      val projections = Map[LogicalVariable, Expression](v"y" -> v"x")

      // when
      val result = lpp.planRegularProjection(plan, projections, MaybeReportedProjections(Some(projections)), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(prop(
          "y",
          "foo"
        )).fromLeft
      )
    }
  }

  test("should rename provided order of variable columns in projection if variable projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, DefaultProvidedOrderFactory.asc(v"x"))
      // projection
      val projections = Map[LogicalVariable, Expression](v"y" -> v"x")

      // when
      val result = lpp.planRegularProjection(plan, projections, MaybeReportedProjections(Some(projections)), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"y").fromLeft
      )
    }
  }

  test("should rename provided order of variable columns in projection if cached node property is projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        DefaultProvidedOrderFactory.asc(prop("x", "foo"))
      )
      // projection
      val projections = Map[LogicalVariable, Expression](v"carrot" -> cachedNodeProp("x", "foo"))

      // when
      val result = lpp.planRegularProjection(plan, projections, MaybeReportedProjections(Some(projections)), context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"carrot").fromLeft
      )
    }
  }

  test("should rename provided order of property columns in distinct if property projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        DefaultProvidedOrderFactory.asc(prop("x", "foo"))
      )
      // projection
      val projections = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))

      // when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"xfoo").fromLeft
      )
    }
  }

  test("should rename provided order of function if function projected") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "id(n)")
      context.staticComponents.planningAttributes.providedOrders.set(plan.id, DefaultProvidedOrderFactory.asc(id(v"n")))
      // projection
      val projections = Map[LogicalVariable, Expression](v"id(n)" -> id(v"n"))

      // when
      val result = lpp.planDistinct(plan, projections, projections, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"id(n)").fromLeft
      )
    }
  }

  test("should trim provided order (1 column) of property column if a sort column is also not a grouping column") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val plan = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x.foo", "y")
      context.staticComponents.planningAttributes.providedOrders.set(
        plan.id,
        DefaultProvidedOrderFactory.asc(prop("x", "foo"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
      val groupings = Map[LogicalVariable, Expression](v"y" -> v"y")

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

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
        DefaultProvidedOrderFactory.asc(v"y").asc(prop("x", "foo"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
      val groupings = Map[LogicalVariable, Expression](v"y" -> v"y")

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"y").fromLeft
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
        DefaultProvidedOrderFactory.asc(v"y").asc(prop("x", "foo"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
      val groupings = Map[LogicalVariable, Expression](v"z" -> v"y")

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"z").fromLeft
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
        DefaultProvidedOrderFactory.asc(prop("y", "bar")).asc(prop("x", "foo"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
      val groupings = Map[LogicalVariable, Expression](v"z" -> v"y")

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

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
        DefaultProvidedOrderFactory.asc(prop("y", "bar")).asc(prop("x", "foo"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
      val groupings = Map[LogicalVariable, Expression](v"z" -> prop("y", "bar"))

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"z").fromLeft
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
        DefaultProvidedOrderFactory.asc(prop("y", "bar")).asc(prop("x", "foo"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoo" -> prop("x", "foo"))
      val groupings = Map[LogicalVariable, Expression](v"z" -> cachedNodeProp("y", "bar"))

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"z").fromLeft
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
        DefaultProvidedOrderFactory.asc(prop("z", "bar")).desc(v"x")
      )
      val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y.bar", "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        rhs.id,
        DefaultProvidedOrderFactory.asc(prop("y", "bar")).asc(v"x").asc(prop("x", "foo"))
      )

      val joinColumns = Set[LogicalVariable](v"x")

      // when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(prop(
          "y",
          "bar"
        )).fromRight
      )
    }
  }

  test("should trim provided order in left outer hash join after property access") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "z.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        lhs.id,
        DefaultProvidedOrderFactory.asc(prop("z", "bar")).desc(v"x")
      )
      val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y.bar", "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        rhs.id,
        DefaultProvidedOrderFactory.asc(prop("y", "bar")).asc(prop("x", "foo")).asc(prop("y", "foo"))
      )

      val joinColumns = Set[LogicalVariable](v"x")

      // when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(prop(
          "y",
          "bar"
        )).fromRight
      )
    }
  }

  test("should trim provided order in left outer hash join after complex property access") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
      // plan with provided order
      val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "z.bar")
      context.staticComponents.planningAttributes.providedOrders.set(
        lhs.id,
        DefaultProvidedOrderFactory.asc(prop("z", "bar")).desc(v"x")
      )
      val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y.bar", "x.foo")
      context.staticComponents.planningAttributes.providedOrders.set(
        rhs.id,
        DefaultProvidedOrderFactory.asc(prop("y", "bar")).asc(add(literalInt(10), prop("x", "foo"))).asc(prop(
          "y",
          "foo"
        ))
      )

      val joinColumns = Set[LogicalVariable](v"x")

      // when
      val result = lpp.planLeftOuterHashJoin(joinColumns, lhs, rhs, Set.empty, context)

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(prop(
          "y",
          "bar"
        )).fromRight
      )
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
        DefaultProvidedOrderFactory.asc(v"y").asc(function("size", v"x"))
      )

      val aggregations = Map[LogicalVariable, Expression](v"size(x)" -> function("size", v"x"))
      val groupings = Map[LogicalVariable, Expression](v"y" -> v"y")

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"y").fromLeft
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
        DefaultProvidedOrderFactory.asc(v"y").asc(propOfProp)
      )

      val aggregations = Map[LogicalVariable, Expression](v"xfoobar" -> propOfProp)
      val groupings = Map[LogicalVariable, Expression](v"y" -> v"y")

      // when
      val result = lpp.planAggregation(
        plan,
        groupings,
        aggregations,
        groupings,
        aggregations,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      // then
      context.staticComponents.planningAttributes.providedOrders.get(result.id) should be(
        DefaultProvidedOrderFactory.asc(v"y").fromLeft
      )
    }
  }

  test("Create should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planCreate(ctx.lhs, CreatePattern(Seq(CreateNode(v"n", Set(), Set(), None))), ctx.context)
    )
  }

  test("MERGE ... ON MATCH should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planMerge(
        ctx.lhs,
        Seq(CreateNode(v"n", Set(), Set(), None)),
        Seq.empty,
        Seq(SetNodePropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1))),
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
        Seq(CreateNode(v"n", Set(), Set(), None)),
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
      ctx.producer.planDeleteNode(ctx.lhs, DeleteExpression(v"n", false), ctx.context)
    )
  }

  test("DeleteRelationship should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeleteRelationship(ctx.lhs, DeleteExpression(v"r", false), ctx.context)
    )
  }

  test("DeletePath should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeletePath(ctx.lhs, DeleteExpression(v"p", false), ctx.context)
    )
  }

  test("DeleteExpression should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planDeleteExpression(ctx.lhs, DeleteExpression(v"x", false), ctx.context)
    )
  }

  test("Setlabel should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetLabel(ctx.lhs, SetLabelPattern(v"n", Seq(labelName("N")), Seq.empty), ctx.context)
    )
  }

  test("RemoveLabel should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planRemoveLabel(ctx.lhs, RemoveLabelPattern(v"n", Seq(labelName("N")), Seq.empty), ctx.context)
    )
  }

  test("SetProperty should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetProperty(
        ctx.lhs,
        SetPropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("SetPropertiesFromMap should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetPropertiesFromMap(
        ctx.lhs,
        SetPropertiesFromMapPattern(v"x", mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("SetNodeProperty should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetNodeProperty(
        ctx.lhs,
        SetNodePropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("SetNodePropertiesFromMap should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetNodePropertiesFromMap(
        ctx.lhs,
        SetNodePropertiesFromMapPattern(v"x", mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("SetRelationshipProperty should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetRelationshipProperty(
        ctx.lhs,
        SetRelationshipPropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("SetRelationshipPropertiesFromMap should eliminate provided order") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planSetRelationshipPropertiesFromMap(
        ctx.lhs,
        SetRelationshipPropertiesFromMapPattern(v"r", mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("ProcedureCall RW should eliminate provided order") {
    val writer = ProcedureSignature(
      procedureName("writer"),
      IndexedSeq(),
      None,
      None,
      ProcedureReadWriteAccess,
      id = 0
    )
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planProcedureCall(
        ctx.lhs,
        ResolvedNonLocalCall(writer, Seq(), IndexedSeq())(pos),
        ctx.context,
        Set.empty
      )
    )
  }

  test("ProcedureCall RO should retain provided order") {
    val reader = ProcedureSignature(
      procedureName("reader"),
      IndexedSeq(),
      None,
      None,
      ProcedureReadOnlyAccess,
      id = 1
    )

    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planProcedureCall(
        ctx.lhs,
        ResolvedNonLocalCall(reader, Seq(), IndexedSeq())(pos),
        ctx.context,
        Set.empty
      )
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
        inTransactionsParameters = None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
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
      ctx.producer.ForSubqueryExpressionSolver.planRollup(ctx.lhs, ctx.rhsWithUpdate, v"x", v"y", ctx.context)
    )
  }

  test("ForListSubqueryExpressionSolver.planRollup should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.ForSubqueryExpressionSolver.planRollup(ctx.lhs, ctx.rhsWithoutUpdate, v"x", v"y", ctx.context)
    )
  }

  test("TriadicSelection should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planTriadicSelection(
        positivePredicate = true,
        ctx.lhs,
        v"a",
        v"b",
        v"c",
        ctx.rhsWithUpdate,
        v"x",
        ctx.context
      )
    )
  }

  test("TriadicSelection should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planTriadicSelection(
        positivePredicate = true,
        ctx.lhs,
        v"a",
        v"b",
        v"c",
        ctx.rhsWithoutUpdate,
        v"x",
        ctx.context
      )
    )
  }

  test("ConditionalApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planConditionalApply(ctx.lhs, ctx.rhsWithUpdate, Seq(v"a"), ctx.context)
    )
  }

  test("ConditionalApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planConditionalApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq(v"a"), ctx.context)
    )
  }

  test("AntiConditionalApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planAntiConditionalApply(ctx.lhs, ctx.rhsWithUpdate, Seq(v"a"), ctx.context)
    )
  }

  test("AntiConditionalApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planAntiConditionalApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq(v"a"), ctx.context)
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
      ctx.producer.planInputApply(ctx.lhs, ctx.rhsWithUpdate, Seq(v"x"), ctx.context)
    )
  }

  test("InputApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planInputApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq(v"x"), ctx.context)
    )
  }

  test("ForeachApply should eliminate provided order when rhs contains update") {
    shouldEliminateProvidedOrder(ctx =>
      ctx.producer.planForeachApply(
        ctx.lhs,
        ctx.rhsWithUpdate,
        ForeachPattern(v"x", v"x", SinglePlannerQuery.empty),
        ctx.context,
        v"x"
      )
    )
  }

  test("ForeachApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planForeachApply(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ForeachPattern(v"x", v"x", SinglePlannerQuery.empty),
        ctx.context,
        v"x"
      )
    )
  }

  test("SemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", ctx.context)
    )
  }

  test("SemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("AntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", ctx.context)
    )
  }

  test("AntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("LetSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", ctx.context)
    )
  }

  test("LetSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("LetAntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", ctx.context)
    )
  }

  test("LetAntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("SelectOrSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSelectOrSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", ctx.context)
    )
  }

  test("SelectOrSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSelectOrSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("SelectOrAntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", ctx.context)
    )
  }

  test("SelectOrAntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("LetSelectOrSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetSelectOrSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", v"x", ctx.context)
    )
  }

  test("LetSelectOrSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetSelectOrSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", v"x", ctx.context)
    )
  }

  test("LetSelectOrAntiSemiApply should fail when rhs contains update") {
    shouldFailAssertion(ctx =>
      ctx.producer.planLetSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithUpdate, v"x", v"x", ctx.context)
    )
  }

  test("LetSelectOrAntiSemiApply should retain provided order when rhs contains no update") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planLetSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", v"x", ctx.context)
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
          introducedVariables = Set(v"x")
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
          introducedVariables = Set(v"x")
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
          introducedVariables = Set(v"x")
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
          introducedVariables = Set(v"x")
        ),
        ctx.context
      )
    )
  }

  test("RemoteBatchProperties should retain provided order") {
    shouldRetainProvidedOrder(ctx =>
      ctx.producer.planRemoteBatchProperties(
        ctx.lhs,
        Set(cachedNodeProp("x", "bar")),
        ctx.context
      )
    )
  }

  test("planRemoteBatchProperties should fuse if inner is a RemoteBatchProperties") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerRBP = planCreationContext.producer.planRemoteBatchProperties(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context
      )
      val outerCachedProperty = cachedNodeProp("y", "foo")
      val outer = planCreationContext.producer.planRemoteBatchProperties(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context
      )
      outer shouldEqual RemoteBatchProperties(planCreationContext.lhs, Set(innerCachedProperty, outerCachedProperty))(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(outer.id) should equal(
        context.staticComponents.planningAttributes.solveds.get(planCreationContext.lhs.id)
      )
    }
  }

  test("planRemoteBatchProperties should fuse if inner is a RemoteBatchPropertiesWithFilter for the same variable") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerPredicates = Seq(equals(innerCachedProperty, literal(42)))
      val innerRBP = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context,
        innerPredicates,
        innerPredicates
      )
      val outerCachedProperty = cachedNodeProp("x", "foo")
      val outer = planCreationContext.producer.planRemoteBatchProperties(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context
      )
      outer shouldEqual RemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        innerPredicates.toSet,
        Set(innerCachedProperty, outerCachedProperty)
      )(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(outer.id) should equal(
        context.staticComponents.planningAttributes.solveds.get(innerRBP.id)
      )
    }
  }

  test("planRemoteBatchProperties should not fuse if inner is a RemoteBatchPropertiesWithFilter") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerPredicates = Seq(equals(innerCachedProperty, literal(42)))
      val innerRBP = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context,
        innerPredicates,
        innerPredicates
      )
      val outerCachedProperty = cachedNodeProp("y", "foo")
      val outer = planCreationContext.producer.planRemoteBatchProperties(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context
      )
      outer shouldEqual RemoteBatchProperties(
        innerRBP,
        Set(outerCachedProperty)
      )(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(outer.id) should equal(
        context.staticComponents.planningAttributes.solveds.get(innerRBP.id)
      )
    }
  }

  test(
    "planRemoteBatchPropertiesWithFilter should fuse if inner is a RemoteBatchPropertiesWithFilter with same variables"
  ) {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerPredicates = Seq(equals(innerCachedProperty, literal(42)))
      val innerRBP = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context,
        innerPredicates,
        innerPredicates
      )
      val outerCachedProperty = cachedNodeProp("x", "foo")
      val outerPredicates = Seq(equals(outerCachedProperty, literal(42)))
      val outer = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context,
        outerPredicates,
        outerPredicates
      )
      outer shouldEqual RemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        (innerPredicates ++ outerPredicates).toSet,
        Set(innerCachedProperty, outerCachedProperty)
      )(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(
        outer.id
      ).asSinglePlannerQuery.tailOrSelf.queryGraph.selections.flatPredicatesSet should equal(
        (innerPredicates ++ outerPredicates).toSet
      )
    }
  }

  test("planRemoteBatchPropertiesWithFilter should not fuse if inner is a RemoteBatchPropertiesWithFilter") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerPredicates = Seq(equals(innerCachedProperty, literal(42)))
      val innerRBP = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context,
        innerPredicates,
        innerPredicates
      )
      val outerCachedProperty = cachedNodeProp("y", "foo")
      val outerPredicates = Seq(equals(outerCachedProperty, literal(42)))
      val outer = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context,
        outerPredicates,
        outerPredicates
      )
      outer shouldEqual RemoteBatchPropertiesWithFilter(
        innerRBP,
        outerPredicates.toSet,
        Set(outerCachedProperty)
      )(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(
        outer.id
      ).asSinglePlannerQuery.tailOrSelf.queryGraph.selections.flatPredicatesSet should equal(
        (innerPredicates ++ outerPredicates).toSet
      )
    }
  }

  test("planRemoteBatchPropertiesWithFilter should fuse if inner is a RemoteBatchProperties on the same variable") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerRBP = planCreationContext.producer.planRemoteBatchProperties(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context
      )
      val outerCachedProperty = cachedNodeProp("x", "foo")
      val outerPredicates = Seq(equals(outerCachedProperty, literal(42)))
      val outer = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context,
        outerPredicates,
        outerPredicates
      )
      outer shouldEqual RemoteBatchPropertiesWithFilter(
        planCreationContext.lhs,
        outerPredicates.toSet,
        Set(innerCachedProperty, outerCachedProperty)
      )(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(
        outer.id
      ).asSinglePlannerQuery.tailOrSelf.queryGraph.selections.flatPredicatesSet should equal(
        outerPredicates.toSet
      )
    }
  }

  test(
    "planRemoteBatchPropertiesWithFilter should not fuse if inner is a RemoteBatchProperties with different variables"
  ) {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      val innerCachedProperty = cachedNodeProp("x", "bar")
      val innerRBP = planCreationContext.producer.planRemoteBatchProperties(
        planCreationContext.lhs,
        Set(innerCachedProperty),
        planCreationContext.context
      )
      val outerCachedProperty = cachedNodeProp("y", "foo")
      val outerPredicates = Seq(equals(outerCachedProperty, literal(42)))
      val outer = planCreationContext.producer.planRemoteBatchPropertiesWithFilter(
        innerRBP,
        Set(outerCachedProperty),
        planCreationContext.context,
        outerPredicates,
        outerPredicates
      )
      outer shouldEqual RemoteBatchPropertiesWithFilter(
        innerRBP,
        outerPredicates.toSet,
        Set(outerCachedProperty)
      )(
        SameId(outer.id)
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(outer.id) should equal(
        CachedProperties(Map()).addAll(Iterable(innerCachedProperty, outerCachedProperty))
      )
      context.staticComponents.planningAttributes.solveds.get(
        outer.id
      ).asSinglePlannerQuery.tailOrSelf.queryGraph.selections.flatPredicatesSet should equal(
        outerPredicates.toSet
      )
    }
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
      if (!AssertionRunner.ASSERTIONS_ENABLED) {
        withClue("NOTE: this test requires assertions to be enabled. Set the vm option -ea to enable them.\n") {
          intercept[AssertionError](getPlan(context, createPlan))
        }
      } else {
        intercept[AssertionError](getPlan(context, createPlan))
      }
    }
  }

  private def getPlan(context: LogicalPlanningContext, createPlan: PlanCreationContext => LogicalPlan) =
    createPlan(buildPlanCreationContext(context))

  private def buildPlanCreationContext(
    context: LogicalPlanningContext,
    useProvidedOrder: Boolean = false
  ): PlanCreationContext = {
    val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)
    val lhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
    val providedOrder = DefaultProvidedOrderFactory.asc(v"y").asc(v"x")
    context.staticComponents.planningAttributes.providedOrders.set(lhs.id, providedOrder)

    val rhs = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "a")
    val rhsWithUpdate = lpp.planSetLabel(rhs, SetLabelPattern(v"n", Seq(labelName("N")), Seq.empty), context)

    if (useProvidedOrder) {
      val initialOrder = DefaultProvidedOrderFactory.asc(v"x")
      context.staticComponents.planningAttributes.providedOrders.set(lhs.id, initialOrder)
      context.staticComponents.planningAttributes.providedOrders.set(rhs.id, initialOrder)
    }
    PlanCreationContext(lpp, context, lhs, rhsWithUpdate, rhs)
  }

  test("should mark leveraged order in plans and their origin") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val initialOrder = DefaultProvidedOrderFactory.asc(v"x")
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

      val vx = v"x"
      val x_vx = Map[LogicalVariable, Expression](v"x" -> vx)
      val foo_vx = Map[LogicalVariable, Expression](v"foo" -> vx)
      val foo_collect =
        Map[LogicalVariable, Expression](v"foo" -> FunctionInvocation(vx, functionName(Collect.name)))
      val interesting_vx = InterestingOrder.required(RequiredOrderCandidate.asc(vx))
      val one = literalInt(1)
      val unionMappings =
        List(UnionMapping(v"x", v"x", v"x"), UnionMapping(v"y", v"y", v"y"))

      // when
      val resultsAndNames = Seq(
        (
          "PartialSort",
          lpp.planPartialSort(
            plan(),
            Seq(Ascending(v"x")),
            Seq(Ascending(v"y")),
            initialOrder.asc(v"y").columns,
            InterestingOrder.empty,
            context
          )
        ),
        (
          "OrderedAggregation with grouping",
          lpp.planOrderedAggregation(
            plan(),
            x_vx,
            foo_vx,
            Seq(vx),
            x_vx,
            foo_vx,
            AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
            context
          )
        ),
        ("OrderedDistinct", lpp.planOrderedDistinct(plan(), foo_vx, Seq(vx), foo_vx, context)),
        ("OrderedUnion", lpp.planOrderedUnion(plan(), plan2(), unionMappings, Seq(Ascending(v"x")), context)),
        (
          "OrderedDistinct for Union",
          lpp.planOrderedDistinctForUnion(
            lpp.planOrderedUnion(plan(), plan2(), unionMappings, Seq(Ascending(v"x")), context),
            Seq(vx),
            context
          )
        ),
        (
          "Limit for aggregation",
          lpp.planLimitForAggregation(
            plan(),
            x_vx,
            foo_vx,
            InterestingOrder.empty,
            AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
            context
          ).lhs.get
        ), // Get the Limit under the Optional
        ("Limit", lpp.planLimit(plan(), one, one, interesting_vx, context)),
        ("Skip", lpp.planSkip(plan(), one, interesting_vx, context)),
        (
          "Collect with previous required order",
          lpp.planAggregation(
            plan(),
            Map.empty,
            foo_collect,
            Map.empty,
            foo_collect,
            Some(interesting_vx),
            AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
            AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
            context
          )
        ), {
          val countDistinctAsc = Map[LogicalVariable, Expression](
            v"c" -> FunctionInvocation(functionName(Count.name), distinct = true, IndexedSeq(vx))(pos)
              .withOrder(ArgumentAsc)
          )
          (
            "Aggregation with ordered distinct",
            lpp.planAggregation(
              plan(),
              Map.empty,
              countDistinctAsc,
              Map.empty,
              countDistinctAsc,
              None,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              context
            )
          )
        }, {
          val sumDistinctAsc = Map[LogicalVariable, Expression](
            v"s" -> FunctionInvocation(functionName(Sum.name), distinct = true, IndexedSeq(vx))(pos)
              .withOrder(ArgumentAsc)
          )
          (
            "Aggregation with ordered sum distinct",
            lpp.planAggregation(
              plan(),
              Map.empty,
              sumDistinctAsc,
              Map.empty,
              sumDistinctAsc,
              None,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              context
            )
          )
        }, {
          val collectDistinctAsc = Map[LogicalVariable, Expression](
            v"l" -> FunctionInvocation(functionName(Collect.name), distinct = true, IndexedSeq(vx))(pos)
              .withOrder(ArgumentAsc)
          )
          (
            "Aggregation with ordered collect distinct",
            lpp.planAggregation(
              plan(),
              Map.empty,
              collectDistinctAsc,
              Map.empty,
              collectDistinctAsc,
              None,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              context
            )
          )
        }, {
          val avgDistinctAsc = Map[LogicalVariable, Expression](
            v"a" -> FunctionInvocation(functionName(Avg.name), distinct = true, IndexedSeq(vx))(pos)
              .withOrder(ArgumentAsc)
          )
          (
            "Aggregation with ordered avg distinct",
            lpp.planAggregation(
              plan(),
              Map.empty,
              avgDistinctAsc,
              Map.empty,
              avgDistinctAsc,
              None,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              context
            )
          )
        }, {
          // percentileDisc triggers hasInterestingOrder via the function-name branch,
          // not via the distinct flag. Covers the non-DISTINCT code path.
          val percentileDiscAsc = Map[LogicalVariable, Expression](
            v"p" -> FunctionInvocation(
              functionName(PercentileDisc.name),
              distinct = false,
              IndexedSeq(vx, literalFloat(0.5))
            )(pos).withOrder(ArgumentAsc)
          )
          (
            "Aggregation with ordered percentileDisc",
            lpp.planAggregation(
              plan(),
              Map.empty,
              percentileDiscAsc,
              Map.empty,
              percentileDiscAsc,
              None,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
              context
            )
          )
        },
        ("ProduceResult", lpp.planProduceResult(plan(), Seq(v"x"), Some(interesting_vx), context)),
        ("ValueMergeJoin", lpp.planMergeJoin(plan(), plan2(), equals(v"x", v"y"), equals(v"x", v"y"), context))
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

      val initialOrder = DefaultProvidedOrderFactory.asc(v"x")

      val leaf1 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val leaf2 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val p1 = lpp.planSort(leaf1, Seq(Ascending(v"x")), initialOrder.columns, InterestingOrder.empty, context)
      val p2 = lpp.planEager(p1, context, ListSet.empty)
      val p3 = lpp.planRightOuterHashJoin(Set(v"x"), leaf2, p2, Set.empty, context)

      // when
      val result = lpp.planProduceResult(
        p3,
        Seq(v"x"),
        Some(InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))),
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

      val initialOrder = DefaultProvidedOrderFactory.asc(v"x")

      val leaf1 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val leaf2 = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val sort1 =
        lpp.planSort(leaf1, Seq(Ascending(v"x")), initialOrder.columns, InterestingOrder.empty, context)
      val sort2 =
        lpp.planSort(leaf2, Seq(Ascending(v"x")), initialOrder.columns, InterestingOrder.empty, context)
      val u = lpp.planOrderedUnion(sort1, sort2, List(), Seq(Ascending(v"x")), context)

      // when
      val result = lpp.planProduceResult(
        u,
        Seq(v"x"),
        Some(InterestingOrder.required(RequiredOrderCandidate.asc(v"x"))),
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

  // Regression test for the topology that caused COUNT(DISTINCT) over-count:
  // an intermediate SelectOrSemiApply between an order-providing plan and an
  // ordered-distinct Aggregation must itself be marked as leveraged so the
  // pipelined runtime chooses order-preserving buffers.
  test("should mark leveraged order through SelectOrSemiApply when aggregating ordered distinct") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val initialOrder = DefaultProvidedOrderFactory.asc(v"x")

      val outerLeaf = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      val sort = lpp.planSort(outerLeaf, Seq(Ascending(v"x")), initialOrder.columns, InterestingOrder.empty, context)

      val innerLeaf = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      val selectOrSemi = lpp.planSelectOrSemiApply(sort, innerLeaf, v"x", context)

      val vx = v"x"
      val countDistinctAsc = Map[LogicalVariable, Expression](
        v"c" -> FunctionInvocation(functionName(Count.name), distinct = true, IndexedSeq(vx))(pos)
          .withOrder(ArgumentAsc)
      )
      val result = lpp.planAggregation(
        selectOrSemi,
        Map.empty,
        countDistinctAsc,
        Map.empty,
        countDistinctAsc,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      context.staticComponents.planningAttributes.leveragedOrders.get(result.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(selectOrSemi.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(sort.id) should be(true)
      context.staticComponents.planningAttributes.leveragedOrders.get(outerLeaf.id) should be(false)
      context.staticComponents.planningAttributes.leveragedOrders.get(innerLeaf.id) should be(false)
    }
  }

  test("should not mark leveraged order when aggregation function has no argument order") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val initialOrder = DefaultProvidedOrderFactory.asc(v"x")

      val leaf = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x", "y")
      context.staticComponents.planningAttributes.providedOrders.set(leaf.id, initialOrder)

      val vx = v"x"
      val countDistinctUnordered = Map[LogicalVariable, Expression](
        v"c" -> FunctionInvocation(functionName(Count.name), distinct = true, IndexedSeq(vx))(pos)
      )
      val result = lpp.planAggregation(
        leaf,
        Map.empty,
        countDistinctUnordered,
        Map.empty,
        countDistinctUnordered,
        None,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        context
      )

      context.staticComponents.planningAttributes.leveragedOrders.get(result.id) should be(false)
      context.staticComponents.planningAttributes.leveragedOrders.get(leaf.id) should be(false)
    }
  }

  test("should retain solved hints when planning union for leaf plans") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val lhs = fakeLogicalPlanFor("x", "y")
      val rhs = fakeLogicalPlanFor("x", "y")
      val hint1 = UsingIndexHint(v"foo", labelOrRelTypeName("bar"), Seq())(InputPosition.NONE)
      val hint2 = UsingIndexHint(v"blah", labelOrRelTypeName("meh"), Seq())(InputPosition.NONE)

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

  test("should retain solved hints even when the or leaf planner plans a single plan without any unions") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val singleNonUnionPlan = fakeLogicalPlanFor("x", "y")
      val hint1 = UsingIndexHint(v"foo", labelOrRelTypeName("bar"), Seq())(InputPosition.NONE)
      val hint2 = UsingIndexHint(v"blah", labelOrRelTypeName("meh"), Seq())(InputPosition.NONE)

      val solveds = context.staticComponents.planningAttributes.solveds
      val spq = SinglePlannerQuery.empty.amendQueryGraph(qg => qg.addHints(Seq(hint1, hint2)))

      solveds.set(singleNonUnionPlan.id, spq)
      context.staticComponents.planningAttributes.providedOrders.set(singleNonUnionPlan.id, ProvidedOrder.empty)

      val updatedPlan = lpp.updateSolvedForOr(singleNonUnionPlan, QueryGraph(), context)

      solveds.get(updatedPlan.id).allHints shouldBe Set(hint1, hint2)
      context.staticComponents.planningAttributes.providedOrders.get(updatedPlan.id) shouldBe ProvidedOrder.empty
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
        nodeVariableGroupings = Set(variableGrouping(v"n", v"n"), variableGrouping(v"m", v"m")),
        relationshipVariableGroupings = Set(variableGrouping(v"r", v"r"))
      )

      the[InternalException] thrownBy producer.planRepeat(
        source = sourcePlan,
        pattern = quantifiedPathPattern,
        startBinding = quantifiedPathPattern.leftBinding,
        endBinding = quantifiedPathPattern.rightBinding,
        context = context,
        innerPlan = innerPlan,
        predicates = Nil,
        previouslyBoundRelationships = Set.empty,
        previouslyBoundRelationshipGroups = Set.empty,
        previouslyBoundNodes = Set.empty,
        previouslyBoundNodeGroups = Set.empty,
        reverseGroupVariableProjections = false,
        ExpandAll,
        TraversalPathMode.Trail,
        Set.empty,
        hints = Seq.empty
      ) should have message "The provided inner plan doesn't conform with the quantified path pattern being planned"
    }
  }

  test("providedOrderOfApply: is correct") {
    val po_empty = ProvidedOrder.empty
    val po_a = DefaultProvidedOrderFactory.asc(v"a")
    val po_b = DefaultProvidedOrderFactory.asc(v"b")
    val po_c = DefaultProvidedOrderFactory.asc(v"c")
    val po_ab = DefaultProvidedOrderFactory.asc(v"a").asc(v"b")
    val po_abc = DefaultProvidedOrderFactory.asc(v"a").asc(v"b").asc(v"c")
    val d_a = DistinctColumns(v"a")
    val d_ab = DistinctColumns(v"a", v"b")

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
          leftDistinctness,
          null,
          DefaultProvidedOrderFactory
        ) should equal(expectedProvidedOrder)
    }
  }

  test("Planning OrderedDistinct should not assume that leveraged order attributes are 'unseen'") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val lpp = LogicalPlanProducer(context.cardinality, context.staticComponents.planningAttributes, idGen)

      val varX = v"x"
      val fooToX = Map[LogicalVariable, Expression](v"foo" -> varX)
      val initialOrder = DefaultProvidedOrderFactory.asc(varX)

      val leaf = fakeLogicalPlanFor(context.staticComponents.planningAttributes, "x")

      val p = lpp.planSort(leaf, Seq(Ascending(varX)), initialOrder.columns, InterestingOrder.empty, context)
      val p1 = lpp.planOrderedDistinct(p, fooToX, Seq(varX), fooToX, context)
      context.staticComponents.planningAttributes.leveragedOrders.get(p1.id) // observe leveraged order
      lpp.planOrderedDistinct(p1, fooToX, Seq(varX), fooToX, context) // should not crash
    }
  }

  test("should retain previously cached properties for planAllNodesScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planAllNodesScan(v"x", Set(v"x"), ctx.context)
    )
  }

  test("should retain previously cached properties for planAllRelationshipsScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planAllRelationshipsScan(
        v"x",
        mockPatternRelationship(v"x"),
        mockPatternRelationship(v"x"),
        Seq.empty,
        Set(v"x"),
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planRelationshipByTypeScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planRelationshipByTypeScan(
        v"x",
        relTypeName("foo"),
        mockPatternRelationship(v"x"),
        mockPatternRelationship(v"x"),
        Seq.empty,
        None,
        Set(v"x"),
        ProvidedOrder.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planUnionRelationshipByTypeScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planUnionRelationshipByTypeScan(
        v"x",
        Seq(relTypeName("foo"), relTypeName("bar")),
        mockPatternRelationship(v"x"),
        mockPatternRelationship(v"x"),
        Seq.empty,
        Seq.empty,
        Set(v"x"),
        ProvidedOrder.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planNodeByLabelScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planNodeByLabelScan(
        v"x",
        labelName("foo"),
        Seq.empty,
        None,
        Set(v"x"),
        ProvidedOrder.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planUnionNodeByLabelsScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planUnionNodeByLabelsScan(
        v"x",
        Seq(labelName("foo"), labelName("bar")),
        Seq.empty,
        Seq.empty,
        Set(v"x"),
        ProvidedOrder.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planIntersectNodeByLabelsScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planIntersectNodeByLabelsScan(
        v"x",
        Seq(labelName("foo"), labelName("bar")),
        Seq.empty,
        Seq.empty,
        Set(v"x"),
        ProvidedOrder.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planSubtractionNodeByLabelsScan") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planSubtractionNodeByLabelsScan(
        v"x",
        Seq(labelName("foo")),
        Seq(labelName("bar")),
        Seq.empty,
        Seq.empty,
        Set(v"x"),
        ProvidedOrder.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planQueryArgument") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planQueryArgument(
        ctx.context.staticComponents.planningAttributes.solveds.get(ctx.lhs.id).asSinglePlannerQuery.queryGraph,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planArgument") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planArgument(
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planCountStoreNodeAggregation") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planCountStoreNodeAggregation(
        ctx.context.staticComponents.planningAttributes.solveds.get(ctx.lhs.id).asSinglePlannerQuery,
        v"x",
        List.empty,
        Set.empty,
        ctx.context
      )
    )
  }

  test("should retain previously cached properties for planCountStoreRelationshipAggregation") {
    shouldUsePreviouslyCachedProperties((ctx: PlanCreationContext) =>
      ctx.producer.planCountStoreRelationshipAggregation(
        ctx.context.staticComponents.planningAttributes.solveds.get(ctx.lhs.id).asSinglePlannerQuery,
        v"x",
        None,
        Seq.empty,
        None,
        Set.empty,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from rhs for planApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planApply(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planSubquery") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planSubquery(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ctx.context,
        correlated = false,
        yielding = false,
        None,
        optional = false,
        importedVariables = Set.empty,
        importedSymbolsFromLastCallSubquery = Set.empty
      )
    )
  }

  test("should propagate cached properties from rhs for planTailApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planTailApply(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planOptional") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planOptional(ctx.rhsWithoutUpdate, Set(v"x"), ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planOptionalMatch") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planOptionalMatch(
        ctx.rhsWithoutUpdate,
        Set(v"x"),
        ctx.context,
        ctx.context.staticComponents.planningAttributes.solveds.get(
          ctx.rhsWithoutUpdate.id
        ).asSinglePlannerQuery.queryGraph
      )
    )
  }

  test("should propagate cached properties from rhs for planLetAntiSemiApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planLetAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planLetSemiApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planLetSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planAntiSemiApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planSemiApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planSemiApplyInHorizon") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planSemiApplyInHorizon(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planAntiSemiApplyInHorizon") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planAntiSemiApplyInHorizon(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planEmptyProjection") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planEmptyProjection(ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planTriadicSelection") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planTriadicSelection(
        positivePredicate = true,
        ctx.lhs,
        v"x",
        v"y",
        v"z",
        ctx.rhsWithoutUpdate,
        hasLabels("y", "foo"),
        ctx.context
      )
    )
  }

  test("should propagate cached properties from rhs for planConditionalApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planConditionalApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq(v"a"), ctx.context)
    )
  }

  test("should propagate cached properties from rhs for planAntiConditionalApply") {
    shouldUseCachedPropertiesFromRHS((ctx: PlanCreationContext) =>
      ctx.producer.planAntiConditionalApply(ctx.lhs, ctx.rhsWithoutUpdate, Seq(v"a"), ctx.context)
    )
  }

  test("should propagate cached properties from lhs for planSimpleExpand") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planSimpleExpand(
        ctx.lhs,
        v"x",
        v"y",
        mockPatternRelationship(v"x"),
        ExpandAll,
        ctx.context,
        ListSet.empty
      )
    )
  }

  test("should propagate cached properties from lhs for planVarExpand") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planVarExpand(
        ctx.lhs,
        v"x",
        v"y",
        mockPatternRelationship(v"x").copy(length = VarPatternLength(1, None)),
        ListSet.empty,
        ListSet.empty,
        ListSet.empty,
        ExpandAll,
        TraversalPathMode.Trail,
        ctx.context,
        ListSet.empty
      )
    )
  }

  test("should propagate cached properties from lhs for planSelectOrAntiSemiApply") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planSelectOrAntiSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from lhs for planLetSelectOrAntiSemiApply") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planLetSelectOrAntiSemiApply(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        v"x",
        propGreaterThan("x", "prop", 42),
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planSelectOrSemiApply") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planSelectOrSemiApply(ctx.lhs, ctx.rhsWithoutUpdate, v"x", ctx.context)
    )
  }

  test("should propagate cached properties from lhs for planLetSelectOrSemiApply") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planLetSelectOrSemiApply(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        v"x",
        propGreaterThan("x", "prop", 42),
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for updateSolvedForSortedItems") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.updateSolvedForSortedItems(
        ctx.lhs,
        InterestingOrder.empty,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planSkip") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planSkip(ctx.lhs, literalInt(1), InterestingOrder.empty, ctx.context)
    )
  }

  test("should propagate cached properties from lhs for planLimit") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planLimit(ctx.lhs, literalInt(1), literalInt(2), InterestingOrder.empty, ctx.context)
    )
  }

  test("should propagate cached properties from lhs for planExhaustiveLimit") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planExhaustiveLimit(ctx.lhs, literalInt(1), literalInt(2), InterestingOrder.empty, ctx.context)
    )
  }

  test("should propagate cached properties from lhs for planSkipAndLimit") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planSkipAndLimit(
        ctx.lhs,
        literalInt(1),
        literalInt(2),
        InterestingOrder.empty,
        ctx.context,
        useExhaustiveLimit = false
      )
    )
  }

  test("should propagate cached properties from lhs for planLimitForAggregation") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planLimitForAggregation(
        ctx.lhs,
        Map(v"x" -> hasLabels("x", "foo")),
        Map.empty,
        InterestingOrder.empty,
        AggregatingQueryProjection.OptionalPreprocessing.Passthrough,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planSort") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planSort(
        ctx.lhs,
        Seq.empty,
        Seq.empty,
        InterestingOrder.empty,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planTop") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planTop(
        ctx.lhs,
        literalInt(42),
        Seq.empty,
        Seq.empty,
        InterestingOrder.empty,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planTop1WithTies") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planTop1WithTies(
        ctx.lhs,
        Seq.empty,
        Seq.empty,
        InterestingOrder.empty,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planPartialSort") {
    shouldUseCachedPropertiesFromLHS(
      (ctx: PlanCreationContext) =>
        ctx.producer.planPartialSort(
          ctx.lhs,
          Seq(Ascending(v"x")),
          Seq(Ascending(v"y")),
          DefaultProvidedOrderFactory.asc(v"y").columns,
          InterestingOrder.empty,
          ctx.context
        ),
      useProvidedOrder = true
    )
  }

  test("should propagate cached properties from lhs for planShortestRelationships") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planShortestRelationship(
        ctx.lhs,
        ShortestRelationshipPattern(
          None,
          rel = PatternRelationship(
            v"x",
            boundaryNodes = (v"r", v"s"),
            dir = OUTGOING,
            types = Seq.empty,
            length = VarPatternLength(1, None)
          ),
          single = false
        )(null),
        nodePredicates = Set.empty,
        relPredicates = Set.empty,
        pathPredicates = Set.empty,
        solvedPredicates = Set.empty,
        withFallBack = false,
        disallowSameNode = true,
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planStatefulShortest") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planStatefulShortest(
        ctx.lhs,
        startNode = v"x",
        endNode = v"y",
        nfa = new TestNFABuilder(0, "x")
          .addTransition(0, 1, "(x)-[r_expr]->(y)")
          .setFinalState(1)
          .build(),
        mode = ExpandAll,
        nonInlinedPreFilters = None,
        nodeVariableGroupings = Set.empty,
        relationshipVariableGroupings = Set.empty,
        singletonNodeVariables = Set.empty,
        singletonRelationshipVariables = Set.empty,
        selector = Shortest(CountInteger(3)),
        solvedExpressionAsString = "",
        solvedSpp = SelectivePathPattern(
          pathPattern = ExhaustivePathPattern.NodeConnections(NonEmptyList(PatternRelationship(
            v"x",
            (v"foo", v"start"),
            SemanticDirection.OUTGOING,
            Seq.empty,
            SimplePatternLength
          ))),
          selections = Selections.from(List(
            unique(v"r")
          )),
          selector = SelectivePathPattern.Selector.Shortest(CountInteger(1))
        ),
        solvedPredicates = Seq.empty,
        reverseGroupVariableProjections = false,
        hints = ListSet.empty,
        context = ctx.context,
        pathLength = PathLength.none,
        Trail
      )
    )
  }

  test("should propagate cached properties from lhs for planProjectEndpoints") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planProjectEndpoints(
        ctx.lhs,
        v"x",
        startInScope = false,
        v"y",
        endInScope = false,
        PatternRelationship(
          v"x",
          (v"foo", v"start"),
          SemanticDirection.OUTGOING,
          Seq.empty,
          SimplePatternLength
        ),
        ctx.context
      )
    )
  }

  test("should propagate cached properties from lhs for planProjectionForUnionMapping") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planProjectionForUnionMapping(
        ctx.lhs,
        Map.empty,
        ctx.context
      )
    )
  }

  test("should only retain cached properties from grouping expressions for planDistinct") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      letLHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo"), propertyKeyName("bar"))),
          v"y" -> CachedProperties.Entry(v"y", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )
      val result = planCreationContext.producer.planDistinct(
        planCreationContext.lhs,
        Map(v"x" -> prop("x", "foo")),
        Map.empty,
        planCreationContext.context
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )
    }
  }

  test("should propagate cached properties from lhs for planOrderedDistinct") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      letLHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo"), propertyKeyName("bar"))),
          v"y" -> CachedProperties.Entry(v"y", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )
      val result = planCreationContext.producer.planOrderedDistinct(
        planCreationContext.lhs,
        Map(v"x" -> prop("x", "foo")),
        Seq.empty,
        Map.empty,
        planCreationContext.context
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )
    }
  }

  test("should propagate cached properties from lhs for planEager") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planEager(
        ctx.lhs,
        ctx.context,
        ListSet.empty
      )
    )
  }

  test("should reset cached properties for planInputApply") {
    shouldResetCachedPropertiesToEmpty((ctx: PlanCreationContext) =>
      ctx.producer.planInputApply(ctx.lhs, ctx.rhsWithUpdate, Seq(v"x"), ctx.context)
    )
  }

  test("should reset cached properties for planLoadCSV") {
    shouldResetCachedPropertiesToEmpty((ctx: PlanCreationContext) =>
      ctx.producer.planLoadCSV(ctx.lhs, v"x", literal("foo"), NoHeaders, None, ctx.context, Set.empty)
    )
  }

  test("should reset cached properties for planInput") {
    shouldResetCachedPropertiesToEmpty((ctx: PlanCreationContext) =>
      ctx.producer.planInput(Seq(v"x"), ctx.context)
    )
  }

  test("should reset cached properties for planCreate") {
    shouldResetCachedPropertiesToEmpty((ctx: PlanCreationContext) =>
      ctx.producer.planCreate(ctx.lhs, CreatePattern(Seq(CreateNode(v"n", Set(), Set(), None))), ctx.context)
    )
  }

  test("should reset cached properties for planMerge") {
    shouldResetCachedPropertiesToEmpty((ctx: PlanCreationContext) =>
      ctx.producer.planMerge(
        ctx.lhs,
        Seq(CreateNode(v"n", Set(), Set(), None)),
        Seq.empty,
        Seq(SetNodePropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1))),
        Seq.empty,
        Set.empty,
        ctx.context
      )
    )
  }

  test("should reset cached properties for planProcedureCall withUpdates") {
    shouldResetCachedPropertiesToEmpty((ctx: PlanCreationContext) =>
      ctx.producer.planProcedureCall(
        ctx.lhs,
        ResolvedNonLocalCall(
          signature = ProcedureSignature(
            name = procedureName("foo"),
            inputSignature = IndexedSeq.empty,
            outputSignature = Option.empty,
            deprecationInfo = Option.empty,
            accessMode = ProcedureReadWriteAccess,
            id = 42
          ),
          callArguments = Seq.empty,
          callResults = IndexedSeq.empty
        )(InputPosition.NONE),
        ctx.context,
        Set.empty
      )
    )
  }

  test("should reset cached properties for planProcedureCall without updates") {
    shouldUseCachedPropertiesFromLHS((ctx: PlanCreationContext) =>
      ctx.producer.planProcedureCall(
        ctx.lhs,
        ResolvedNonLocalCall(
          signature = ProcedureSignature(
            name = procedureName("foo"),
            inputSignature = IndexedSeq.empty,
            outputSignature = Option.empty,
            deprecationInfo = Option.empty,
            accessMode = ProcedureReadOnlyAccess,
            id = 42
          ),
          callArguments = Seq.empty,
          callResults = IndexedSeq.empty
        )(InputPosition.NONE),
        ctx.context,
        Set.empty
      )
    )
  }

  test("should reset cached properties for planProcedureCall planDeleteNode") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planDeleteNode(ctx.lhs, DeleteExpression(v"n", false), ctx.context)
    )
  }

  test("should reset cached properties for planDeleteRelationships") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planDeleteRelationship(ctx.lhs, DeleteExpression(v"r", false), ctx.context)
    )
  }

  test("should reset cached properties for planDeletePath") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planDeletePath(ctx.lhs, DeleteExpression(v"p", false), ctx.context)
    )
  }

  test("should reset cached properties for planDeleteExpression") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planDeleteExpression(ctx.lhs, DeleteExpression(v"x", false), ctx.context)
    )
  }

  test("should reset cached properties for planSetLabel") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetLabel(ctx.lhs, SetLabelPattern(v"n", Seq(labelName("N")), Seq.empty), ctx.context)
    )
  }

  test("should reset cached properties for planRemoveLabel") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planRemoveLabel(ctx.lhs, RemoveLabelPattern(v"n", Seq(labelName("N")), Seq.empty), ctx.context)
    )
  }

  test("should reset cached properties for planSetProperty") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetProperty(
        ctx.lhs,
        SetPropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("should reset cached properties for planProcedureCall planSetPropertiesFromMap") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetPropertiesFromMap(
        ctx.lhs,
        SetPropertiesFromMapPattern(v"x", mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("should reset cached properties for planSetNodeProperty") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetNodeProperty(
        ctx.lhs,
        SetNodePropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("should reset cached properties for planSetNodePropertiesFromMap") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetNodePropertiesFromMap(
        ctx.lhs,
        SetNodePropertiesFromMapPattern(v"x", mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("should reset cached properties for planSetRelationshipProperty") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetRelationshipProperty(
        ctx.lhs,
        SetRelationshipPropertyPattern(v"x", PropertyKeyName("p")(pos), literalInt(1)),
        ctx.context
      )
    )
  }

  test("should reset cached properties for planSetRelationshipPropertiesFromMap") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planSetRelationshipPropertiesFromMap(
        ctx.lhs,
        SetRelationshipPropertiesFromMapPattern(v"r", mapOfInt("p" -> 1), false),
        ctx.context
      )
    )
  }

  test("should reset cached properties for planForeachApply") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planForeachApply(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        ForeachPattern(v"x", v"x", SinglePlannerQuery.empty),
        ctx.context,
        v"x"
      )
    )
  }

  test("should reset cached properties for planForeach") {
    shouldResetCachedPropertiesToEmpty(ctx =>
      ctx.producer.planForeach(
        ctx.lhs,
        ForeachPattern(v"x", v"x", SinglePlannerQuery.empty),
        ctx.context,
        v"x"
      )
    )
  }

  test("should only cache common variables and properties for planNodeHashJoin") {
    shouldIntersectCachedEntries(ctx =>
      ctx.producer.planNodeHashJoin(Set.empty, ctx.lhs, ctx.rhsWithoutUpdate, Set.empty, ctx.context)
    )
  }

  test("should only cache common variables and properties for planAssertSameNode") {
    shouldIntersectCachedEntries(ctx =>
      ctx.producer.planAssertSameNode(v"x", ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("should only cache common variables and properties for planAssertSameRelationship") {
    shouldIntersectCachedEntries(ctx =>
      ctx.producer.planAssertSameRelationship(mockPatternRelationship(v"x"), ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("should only cache common variables and properties for planLeftOuterHashJoin") {
    shouldIntersectCachedEntries(ctx =>
      ctx.producer.planLeftOuterHashJoin(Set.empty, ctx.lhs, ctx.rhsWithoutUpdate, Set.empty, ctx.context)
    )
  }

  test("should only cache common variables and properties for planRightOuterHashJoin") {
    shouldIntersectCachedEntries(ctx =>
      ctx.producer.planRightOuterHashJoin(Set.empty, ctx.lhs, ctx.rhsWithoutUpdate, Set.empty, ctx.context)
    )
  }

  test("should only cache common variables and properties for planOrderedUnion") {
    shouldIntersectCachedEntries(
      ctx =>
        ctx.producer.planOrderedUnion(ctx.lhs, ctx.rhsWithoutUpdate, List.empty, Seq(Ascending(v"x")), ctx.context),
      useProvidedOrder = true
    )
  }

  test(
    "should propagate common properties for planUnion and propagate cached properties from lhs for planDistinctForUnion"
  ) {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)

      letRHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(v"x" -> CachedProperties.Entry(
          v"x",
          NODE_TYPE,
          Set(propertyKeyName("foo"), propertyKeyName("bar"))
        )))
      )

      letLHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo"))),
          v"y" -> CachedProperties.Entry(v"y", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )

      val union = planCreationContext.producer.planUnion(
        planCreationContext.lhs,
        planCreationContext.rhsWithoutUpdate,
        List.empty,
        planCreationContext.context
      )
      val expectedCachedProperties =
        CachedProperties(Map(v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo")))))
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(union.id) should equal(
        expectedCachedProperties
      )

      val distinctForUnion = planCreationContext.producer.planDistinctForUnion(
        union,
        planCreationContext.context
      )
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(distinctForUnion.id) should equal(
        expectedCachedProperties
      )
    }
  }

  test("should only cache common properties for a given variable for planCartesianProduct") {
    shouldIntersectCachedProperties(ctx =>
      ctx.producer.planCartesianProduct(ctx.lhs, ctx.rhsWithoutUpdate, ctx.context)
    )
  }

  test("should only cache common properties for a given variable for planValueHashJoin") {
    shouldIntersectCachedProperties(ctx =>
      ctx.producer.planValueHashJoin(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        equals(prop("x", "foo"), prop("y", "foo")),
        equals(prop("x", "foo"), prop("y", "foo")),
        ctx.context
      )
    )
  }

  test("should only cache common properties for a given variable for planValueMergeJoin") {
    shouldIntersectCachedProperties { ctx =>
      val attrs = ctx.context.staticComponents.planningAttributes
      attrs.providedOrders.set(ctx.lhs.id, DefaultProvidedOrderFactory.asc(v"x"))
      attrs.providedOrders.set(ctx.rhsWithoutUpdate.id, DefaultProvidedOrderFactory.asc(v"y"))
      ctx.producer.planMergeJoin(
        ctx.lhs,
        ctx.rhsWithoutUpdate,
        equals(prop("x", "foo"), prop("y", "foo")),
        equals(prop("x", "foo"), prop("y", "foo")),
        ctx.context
      )
    }
  }

  test("should throw when unsupported expression is added to a logical plan (when assertions are enabled, -ea)") {
    val badExpressions: Seq[Expression] = Seq(
      patternComprehension(relationshipChain(nodePat(), relPat(), nodePat()), literal(1))
        .withComputedScopeDependencies(Set.empty),
      patternExpression(v"x", v"x")
        .withComputedScopeDependencies(Set.empty),
      MapProjection(v"x", Seq.empty)(pos),
      CountIRExpression(SinglePlannerQuery.empty, v"result", "")(pos, Some(Set.empty), Some(Set.empty)),
      PartialPredicate(isNotNull(prop("x", "prop")), equals(prop("x", "prop"), prop("y", "prop"))),
      ImpliedLabel(hasLabels("x", "A"))(pos)
    )

    new givenConfig().withLogicalPlanningContext { (_, context) =>
      for (expr <- badExpressions) withClue(s"Expression $expr") {
        val planWithBadExpression = {
          val attrs = context.staticComponents.planningAttributes
          val sourcePlan = fakeLogicalPlanFor(attrs, "x")
          Selection(Seq(expr), sourcePlan)(attrs.asAttributes(idGen).copy(from = sourcePlan.id))
        }

        val ex = intercept[InternalException] {
          context.staticComponents.logicalPlanProducer.planSkip(
            planWithBadExpression,
            literalInt(0),
            InterestingOrder.empty,
            context
          )
        }
        ex.getMessage should include("This expression should not be added to a logical plan")
      }
    }
  }

  private def mockCachedProperties =
    CachedProperties(Map(v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo")))))

  private def propertyKeyName(name: String): PropertyKeyName = PropertyKeyName(name)(InputPosition.NONE)

  private def mockPatternRelationship(variable: LogicalVariable): PatternRelationship = PatternRelationship(
    variable,
    (v"n", v"m"),
    SemanticDirection.OUTGOING,
    Nil,
    SimplePatternLength
  )

  private def shouldUsePreviouslyCachedProperties(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val contextToUse = contextWithPreviouslyCachedProperties(context)
      val result = getPlan(contextToUse, createPlan)
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        contextToUse.plannerState.previouslyCachedProperties
      )
    }
  }

  private def contextWithPreviouslyCachedProperties(context: LogicalPlanningContext) = {
    context.copy(plannerState =
      context.plannerState.copy(previouslyCachedProperties =
        mockCachedProperties
      )
    )
  }

  private def shouldUseCachedPropertiesFromRHS(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)
      letRHSPlanHaveTheCachedProperties(context, planCreationContext, mockCachedProperties)

      val result = createPlan(planCreationContext)
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(
          planCreationContext.rhsWithoutUpdate.id
        )
      )
    }
  }

  private def shouldUseCachedPropertiesFromLHS(
    createPlan: PlanCreationContext => LogicalPlan,
    useProvidedOrder: Boolean = false
  ) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context, useProvidedOrder)
      letLHSPlanHaveTheCachedProperties(context, planCreationContext, mockCachedProperties)

      val result = createPlan(planCreationContext)
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(
          planCreationContext.lhs.id
        )
      )
    }
  }

  private def shouldResetCachedPropertiesToEmpty(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val contextToUse = contextWithPreviouslyCachedProperties(context)
      val planCreationContext = buildPlanCreationContext(contextToUse)
      letRHSPlanHaveTheCachedProperties(contextToUse, planCreationContext, mockCachedProperties)
      letLHSPlanHaveTheCachedProperties(contextToUse, planCreationContext, mockCachedProperties)
      val result = getPlan(contextToUse, createPlan)
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        CachedProperties.empty
      )
    }
  }

  private def shouldIntersectCachedEntries(
    createPlan: PlanCreationContext => LogicalPlan,
    useProvidedOrder: Boolean = false
  ) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context, useProvidedOrder)

      letRHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(v"x" -> CachedProperties.Entry(
          v"x",
          NODE_TYPE,
          Set(propertyKeyName("foo"), propertyKeyName("bar"))
        )))
      )

      letLHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo"))),
          v"y" -> CachedProperties.Entry(v"y", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )

      val result = createPlan(planCreationContext)

      val expectedCachedProperties =
        CachedProperties(Map(v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo")))))
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        expectedCachedProperties
      )
    }
  }

  private def shouldIntersectCachedProperties(createPlan: PlanCreationContext => LogicalPlan) = {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val planCreationContext = buildPlanCreationContext(context)

      letRHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(v"x" -> CachedProperties.Entry(
          v"x",
          NODE_TYPE,
          Set(propertyKeyName("foo"), propertyKeyName("bar"))
        )))
      )

      letLHSPlanHaveTheCachedProperties(
        context,
        planCreationContext,
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo"))),
          v"y" -> CachedProperties.Entry(v"y", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      )

      val result = createPlan(planCreationContext)

      val expectedCachedProperties =
        CachedProperties(Map(
          v"x" -> CachedProperties.Entry(v"x", NODE_TYPE, Set(propertyKeyName("foo"))),
          v"y" -> CachedProperties.Entry(v"y", NODE_TYPE, Set(propertyKeyName("foo")))
        ))
      context.staticComponents.planningAttributes.cachedPropertiesPerPlan.get(result.id) should equal(
        expectedCachedProperties
      )
    }
  }

  private def letLHSPlanHaveTheCachedProperties(
    context: LogicalPlanningContext,
    planCreationContext: PlanCreationContext,
    cp: CachedProperties
  ): Unit = {
    context.staticComponents.planningAttributes.cachedPropertiesPerPlan.set(
      planCreationContext.lhs.id,
      cp
    )
  }

  private def letRHSPlanHaveTheCachedProperties(
    context: LogicalPlanningContext,
    planCreationContext: PlanCreationContext,
    cachedProperties: CachedProperties
  ): Unit = {
    context.staticComponents.planningAttributes.cachedPropertiesPerPlan.set(
      planCreationContext.rhsWithoutUpdate.id,
      cachedProperties
    )
  }
}
