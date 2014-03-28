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

// Helper class mainly used via LogicalPlanningTestSupport
case class OverridableMetricsFactory(
  metricsFactory: MetricsFactory,
  altNewSelectivityEstimator: Option[() => selectivityEstimator] = None,
  altNewCardinalityEstimator: Option[(selectivityEstimator) => cardinalityEstimator] = None,
  altNewCostModel: Option[(cardinalityEstimator) => costModel] = None) extends MetricsFactory {

  def withCostModel(pf: PartialFunction[LogicalPlan, Int]) =
    copy(altNewCostModel = Some((_: cardinalityEstimator) => pf.lift.andThen(_.getOrElse(Int.MaxValue))))

  def withCardinalityEstimator(pf: PartialFunction[LogicalPlan, Int]) =
    copy(altNewCardinalityEstimator = Some((_: selectivityEstimator) => pf.lift.andThen(_.getOrElse(Int.MaxValue))))

  def withSelectivityEstimator(pf: PartialFunction[Expression, Double]) =
    copy(altNewSelectivityEstimator = Some(() => pf.lift.andThen(_.getOrElse(1.0d))))

  def newCostModel(cardinality: cardinalityEstimator): costModel =
    altNewCostModel.getOrElse(metricsFactory.newCostModel(_))(cardinality)

  def newCardinalityEstimator(selectivity: selectivityEstimator): cardinalityEstimator =
    altNewCardinalityEstimator.getOrElse(metricsFactory.newCardinalityEstimator(_))(selectivity)

  def newSelectivityEstimator: selectivityEstimator =
    altNewSelectivityEstimator.getOrElse(() => metricsFactory.newSelectivityEstimator)()
}
