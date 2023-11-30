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
package org.neo4j.cypher.internal.logical.generator

import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.compiler.planner.logical.SimpleMetricsFactory
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier
import org.scalatest.Assertions.fail

trait CardinalityCalculator[-T <: LogicalPlan] {

  def apply(
    plan: T,
    state: LogicalPlanGenerator.State,
    planContext: PlanContext,
    labelsWithIds: Map[String, Int]
  ): Cardinality
}

object CardinalityCalculator {

  private val SAME_AS_LEFT: CardinalityCalculator[LogicalPlan] =
    (plan, state, _, _) => state.cardinalities(plan.lhs.get.id)

  private val LEAF_CARDINALITY: CardinalityCalculator[LogicalPlan] =
    (_, state, _, _) => state.leafCardinalityMultiplier

  implicit val produceResultCardinality: CardinalityCalculator[ProduceResult] =
    SAME_AS_LEFT

  implicit val allNodesScanCardinality: CardinalityCalculator[AllNodesScan] =
    (_, state, planContext, _) => state.leafCardinalityMultiplier * planContext.statistics.nodesAllCardinality()

  implicit val undirectedRelationshipByIdSeek: CardinalityCalculator[UndirectedRelationshipByIdSeek] =
    (plan, state, _, _) => {
      val numberOfRels = plan.relIds.sizeHint.map(Cardinality(_))
        .getOrElse(PlannerDefaults.DEFAULT_LIST_CARDINALITY)

      numberOfRels * Multiplier(2) * state.leafCardinalityMultiplier
    }

  implicit val directedRelationshipByIdSeek: CardinalityCalculator[DirectedRelationshipByIdSeek] =
    (plan, state, _, _) => {
      val numberOfRels = plan.relIds.sizeHint.map(Cardinality(_))
        .getOrElse(PlannerDefaults.DEFAULT_LIST_CARDINALITY)

      numberOfRels * state.leafCardinalityMultiplier
    }

  implicit val nodeByLabelScanCardinality: CardinalityCalculator[NodeByLabelScan] = {
    (plan, state, planContext, labelsWithIds) =>
      val labelId = Some(LabelId(labelsWithIds(plan.label.name)))
      state.leafCardinalityMultiplier * planContext.statistics.nodesWithLabelCardinality(labelId)
  }

  implicit val argumentCardinality: CardinalityCalculator[Argument] =
    LEAF_CARDINALITY

  implicit val eagerCardinality: CardinalityCalculator[Eager] =
    SAME_AS_LEFT

  implicit val expandCardinality: CardinalityCalculator[Expand] = {
    (plan, state, planContext, _) =>
      val Expand(source, from, dir, relTypes, to, relName, _) = plan
      val inboundCardinality = state.cardinalities.get(source.id)
      val qg = QueryGraph(
        patternNodes = Set(from.name, to.name),
        patternRelationships =
          Set(PatternRelationship(relName, (from, to), dir, relTypes, SimplePatternLength)),
        argumentIds = state.arguments.map(_.name)
      )
      val qgCardinalityModel = new AssumeIndependenceQueryGraphCardinalityModel(
        planContext,
        SimpleMetricsFactory.newSelectivityCalculator(planContext),
        IndependenceCombiner,
        false
      )
      val expandCardinality =
        qgCardinalityModel(
          qg,
          state.labelInfo,
          state.relTypeInfo,
          state.semanticTable,
          IndexCompatiblePredicatesProviderContext.default,
          cardinalityModel = null // We don't have SubqueryExpressions
        )
      expandCardinality * inboundCardinality
  }

  implicit val skipCardinality: CardinalityCalculator[Skip] = {
    (plan, state, _, _) =>
      val Skip(source, count: Expression) = plan
      val sourceCardinality = state.cardinalities.get(source.id)
      Cardinality.max(Cardinality.EMPTY, sourceCardinality + Cardinality(-toIntegerLiteral(count).value))
  }

  implicit val limitCardinality: CardinalityCalculator[Limit] = {
    (plan, state, _, _) =>
      val Limit(source, count: Expression) = plan
      val sourceCardinality = state.cardinalities.get(source.id)
      Cardinality.min(sourceCardinality, Cardinality(toIntegerLiteral(count).value.toDouble))
  }

  implicit val exhaustiveLimitCardinality: CardinalityCalculator[ExhaustiveLimit] = {
    (plan, state, _, _) =>
      val ExhaustiveLimit(source, count: Expression) = plan
      val sourceCardinality = state.cardinalities.get(source.id)
      Cardinality.min(sourceCardinality, Cardinality(toIntegerLiteral(count).value.toDouble))
  }

  implicit val projectionCardinality: CardinalityCalculator[Projection] =
    (plan, state, _, _) => state.cardinalities.get(plan.source.id)

  implicit val aggregationCardinality: CardinalityCalculator[Aggregation] = {
    (plan, state, _, _) =>
      val in = state.cardinalities.get(plan.source.id)
      StatisticsBackedCardinalityModel.aggregateCardinalityEstimation(
        in,
        plan.groupingExpressions
      )
  }

  implicit val applyCardinality: CardinalityCalculator[Apply] =
    (plan, state, _, _) => state.cardinalities.get(plan.right.id)

  implicit val semiApplyCardinality: CardinalityCalculator[SemiApply] =
    (plan, state, _, _) => {
      val lhsCardinality = state.cardinalities.get(plan.left.id)

      lhsCardinality * PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY
    }

  implicit val antiSemiApplyCardinality: CardinalityCalculator[AntiSemiApply] =
    (plan, state, _, _) => {
      val lhsCardinality = state.cardinalities.get(plan.left.id)

      lhsCardinality * PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY
    }

  implicit val cartesianProductCardinality: CardinalityCalculator[CartesianProduct] =
    (plan, state, _, _) => state.cardinalities.get(plan.left.id) * state.cardinalities.get(plan.right.id)

  implicit val distinctCardinality: CardinalityCalculator[Distinct] =
    (plan, state, _, _) => state.cardinalities.get(plan.source.id) * PlannerDefaults.DEFAULT_DISTINCT_SELECTIVITY

  implicit val optionalCardinality: CardinalityCalculator[Optional] =
    (plan, state, _, _) => Cardinality.max(state.leafCardinalityMultiplier, state.cardinalities.get(plan.source.id))

  implicit val sortCardinality: CardinalityCalculator[Sort] =
    SAME_AS_LEFT

  implicit val topCardinality: CardinalityCalculator[Top] = {
    (plan, state, _, _) =>
      val Top(source, _, count: Expression) = plan
      val sourceCardinality = state.cardinalities.get(source.id)
      val applyLHSCardinality = state.leafCardinalityMultiplier
      val limit = Multiplier(toIntegerLiteral(count).value.toDouble)
      Cardinality.min(sourceCardinality, applyLHSCardinality * limit)
  }

  implicit val selectionCardinality: CardinalityCalculator[Selection] =
    (plan, state, _, _) => state.cardinalities.get(plan.source.id) * PlannerDefaults.DEFAULT_PREDICATE_SELECTIVITY

  implicit val unwindCollectionCardinality: CardinalityCalculator[UnwindCollection] =
    (plan, state, _, _) => state.cardinalities.get(plan.source.id) * PlannerDefaults.DEFAULT_LIST_CARDINALITY

  implicit val nodeCountFromCountStoreCardinality: CardinalityCalculator[NodeCountFromCountStore] =
    LEAF_CARDINALITY

  implicit val relationshipCountFromCountStoreCardinality: CardinalityCalculator[RelationshipCountFromCountStore] =
    LEAF_CARDINALITY

  implicit val valueHashJoinCardinality: CardinalityCalculator[ValueHashJoin] =
    (plan, state, _, _) =>
      state.cardinalities.get(plan.left.id) *
        state.cardinalities.get(plan.right.id) *
        PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY

  implicit val unionCardinality: CardinalityCalculator[Union] =
    (plan, state, _, _) => state.cardinalities.get(plan.left.id) + state.cardinalities.get(plan.right.id)

  private def toIntegerLiteral(count: Expression): IntegerLiteral = {
    count match {
      case x: IntegerLiteral => x
      case x                 => fail(s"Expected IntegerLiteral but got ${x.getClass}")
    }
  }
}
