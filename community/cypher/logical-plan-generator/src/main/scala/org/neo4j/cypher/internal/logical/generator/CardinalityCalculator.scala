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
package org.neo4j.cypher.internal.logical.generator

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.IndependenceCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.AssumeIndependenceQueryGraphCardinalityModel
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId

trait CardinalityCalculator[-T <: LogicalPlan] {
  def apply(plan: T, state: LogicalPlanGenerator.State, stats: GraphStatistics, labelsWithIds: Map[String, Int]): Cardinality
}

object CardinalityCalculator {

  private val SAME_AS_LEFT: CardinalityCalculator[LogicalPlan] = (plan, state, _, _) => state.cardinalities(plan.lhs.get.id)

  implicit val produceResultCardinality: CardinalityCalculator[ProduceResult] =
    SAME_AS_LEFT

  implicit val allNodesScanCardinality: CardinalityCalculator[AllNodesScan] =
    (_, state, stats, _) => state.leafCardinalityMultipliers.head * stats.nodesAllCardinality()

  implicit val nodeByLabelScanCardinality: CardinalityCalculator[NodeByLabelScan] = {
    (plan, state, stats, labelsWithIds) =>
      val labelId = Some(LabelId(labelsWithIds(plan.label.name)))
      state.leafCardinalityMultipliers.head * stats.nodesWithLabelCardinality(labelId)
  }

  implicit val argumentCardinality: CardinalityCalculator[Argument] =
    (_, state, _, _) => state.leafCardinalityMultipliers.head

  implicit val eagerCardinality: CardinalityCalculator[Eager] =
    SAME_AS_LEFT

  implicit val expandCardinality: CardinalityCalculator[Expand] = {
    (plan, state, stats, _) =>
      val Expand(source, from, dir, relTypes, to, relName, _, _) = plan
      val qg = QueryGraph(
        patternNodes = Set(from, to),
        patternRelationships = Set(PatternRelationship(relName, (from, to), dir, relTypes, SimplePatternLength)),
        argumentIds = state.arguments
      )
      val qgsi = QueryGraphSolverInput(
        labelInfo = state.labelInfo,
        inboundCardinality = state.cardinalities.get(source.id),
        strictness = None
      )
      val qgCardinalityModel = AssumeIndependenceQueryGraphCardinalityModel(stats, IndependenceCombiner)
      qgCardinalityModel(qg, qgsi, state.semanticTable)
  }

  implicit val skipCardinality: CardinalityCalculator[Skip] = {
    (plan, state, _, _) =>
      val Skip(source, count: IntegerLiteral) = plan
      val sourceCardinality = state.cardinalities.get(source.id)
      Cardinality.max(Cardinality.EMPTY, sourceCardinality + Cardinality(-count.value))
  }

  implicit val limitCardinality: CardinalityCalculator[Limit] = {
    (plan, state, _, _) =>
      val Limit(source, count: IntegerLiteral, _) = plan
      val sourceCardinality = state.cardinalities.get(source.id)
      Cardinality.min(sourceCardinality, Cardinality(count.value.toDouble))
  }

  implicit val projectionCardinality: CardinalityCalculator[Projection] =
    (plan, state, _, _) => state.cardinalities.get(plan.source.id)

  implicit val aggregationCardinality: CardinalityCalculator[Aggregation] = {
    (plan, state, _, _) =>
      val in = state.cardinalities.get(plan.source.id)
      StatisticsBackedCardinalityModel.aggregateCardinalityBeforeSelection(in, plan.groupingExpressions)
  }

  implicit val applyCardinality: CardinalityCalculator[Apply] =
    (plan, state, _, _) => state.cardinalities.get(plan.right.id)

  implicit val cartesianProductCardinality: CardinalityCalculator[CartesianProduct] =
    (plan, state, _, _) => state.cardinalities.get(plan.left.id) * state.cardinalities.get(plan.right.id)
}
