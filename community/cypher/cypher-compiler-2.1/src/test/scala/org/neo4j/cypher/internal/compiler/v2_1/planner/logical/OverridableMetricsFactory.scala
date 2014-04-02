/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import Metrics._
import org.neo4j.cypher.internal.compiler.v2_1.spi.GraphHeuristics

// Helper class mainly used via LogicalPlanningTestSupport
case class OverridableMetricsFactory(
  metricsFactory: MetricsFactory,
  altNewSelectivityEstimator: Option[(GraphHeuristics) => SelectivityEstimator] = None,
  altNewCardinalityEstimator: Option[(GraphHeuristics, SelectivityEstimator) => CardinalityEstimator] = None,
  altNewCostModel: Option[(CardinalityEstimator) => CostModel] = None,
  altGraphHeuristics: Option[GraphHeuristics => GraphHeuristics] = None) extends MetricsFactory {

  def newCostModel(cardinality: CardinalityEstimator): CostModel =
    altNewCostModel.getOrElse(metricsFactory.newCostModel(_))(cardinality)

  def newCardinalityEstimator(heuristics: GraphHeuristics, selectivity: SelectivityEstimator): CardinalityEstimator =
    altNewCardinalityEstimator.getOrElse(metricsFactory.newCardinalityEstimator(_, _))(heuristics, selectivity)

  def newSelectivityEstimator(heuristics: GraphHeuristics): SelectivityEstimator =
    altNewSelectivityEstimator.getOrElse(metricsFactory.newSelectivityEstimator(_))(heuristics)

  def replaceCostModel(pf: PartialFunction[LogicalPlan, Double]) =
    copy(altNewCostModel = Some((_: CardinalityEstimator) => mapToDouble(pf).lift.andThen(_.getOrElse(Double.MaxValue))))

  def replaceCardinalityEstimator(pf: PartialFunction[LogicalPlan, Double]) =
    copy(altNewCardinalityEstimator = Some((_: GraphHeuristics, _: SelectivityEstimator) => mapToDouble(pf).lift.andThen(_.getOrElse(Double.MaxValue))))

  def amendCardinalityEstimator(pf: PartialFunction[LogicalPlan, Double]) =
    copy(altNewCardinalityEstimator = Some({    (heuristics: GraphHeuristics, selectivity: SelectivityEstimator) =>
      val fallback: PartialFunction[LogicalPlan, Double] = {
        case plan => metricsFactory.newCardinalityEstimator(heuristics, selectivity)(plan)
      }
      (mapToDouble(pf) `orElse` fallback).lift.andThen(_.getOrElse(Double.MaxValue))
    }))

  def replaceSelectivityEstimator(pf: PartialFunction[Expression, Double]) =
    copy(altNewSelectivityEstimator = Some((_: GraphHeuristics) => mapToDouble(pf).lift.andThen(_.getOrElse(1.0d))))

  override def newMetrics(implicit heuristics: GraphHeuristics): Metrics = altGraphHeuristics match {
    case Some(mapF) => super.newMetrics(mapF(heuristics))
    case None       => super.newMetrics(heuristics)
  }

  private def mapToDouble[T](pf: PartialFunction[T, Any]): PartialFunction[T, Double] =
    pf.andThen[Double] {
      case x: Number => x.doubleValue()
    }
}
