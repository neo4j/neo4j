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

import org.neo4j.cypher.internal.compiler.v2_1.ast.{Identifier, HasLabels, Expression}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.spi.GraphStatistics

object GuessingEstimation {
  val LABEL_NOT_FOUND_SELECTIVITY: Double = 0.0
  val PREDICATE_SELECTIVITY: Double = 0.2
  val INDEX_SEEK_SELECTIVITY: Double = 0.08
  val UNIQUE_INDEX_SEEK_SELECTIVITY: Double = 0.05
  val DEFAULT_EXPAND_RELATIONSHIP_DEGREE: Double = 2.0
}

class StatisticsBackedCardinalityModel(statistics: GraphStatistics,
                                   selectivity: Metrics.SelectivityModel) extends Metrics.CardinalityModel {
  import GuessingEstimation._

  def apply(plan: LogicalPlan): Double = plan match {
    case AllNodesScan(_) =>
      statistics.nodesCardinality

    case NodeByLabelScan(_, Left(_)) =>
      statistics.nodesCardinality * LABEL_NOT_FOUND_SELECTIVITY

    case NodeByLabelScan(_, Right(labelId)) =>
      statistics.nodesWithLabelCardinality(labelId)

    case NodeByIdSeek(_, nodeIds) =>
      nodeIds.size

    case NodeIndexSeek(_, _, _, _) =>
      statistics.nodesCardinality * INDEX_SEEK_SELECTIVITY

    case NodeIndexUniqueSeek(_, _, _, _) =>
      statistics.nodesCardinality * UNIQUE_INDEX_SEEK_SELECTIVITY

    case NodeHashJoin(_, left, right) =>
      (cardinality(left) + cardinality(right)) / 2

    case expand @ Expand(left, _, dir, types, _, _) =>
      val degree = if (types.size <= 0)
        DEFAULT_EXPAND_RELATIONSHIP_DEGREE
      else
        types.foldLeft(0.0)((sum, t) => sum + statistics.degreeByLabelTypeAndDirection(t.id.get, dir)) / types.size
      cardinality(left) * degree

    case Selection(predicates, left) =>
      cardinality(left) * predicates.map(selectivity).foldLeft(1.0)(_ * _)

    case CartesianProduct(left, right) =>
      cardinality(left) * cardinality(right)

    case DirectedRelationshipByIdSeek(_, relIds, _, _) =>
      relIds.size

    case UndirectedRelationshipByIdSeek(_, relIds, _, _) =>
      relIds.size * 2

    case Projection(left, _) =>
      cardinality(left)

    case SingleRow() =>
      1
  }

  private def cardinality(plan: LogicalPlan) = apply(plan)
}

class StatisticsBasedSelectivityModel(statistics: GraphStatistics) extends Metrics.SelectivityModel {

  import GuessingEstimation._

  def apply(predicate: Expression): Double = predicate match {
    case HasLabels(_, Seq(label)) =>
      if (label.id.isDefined)
        statistics.nodesWithLabelSelectivity(label.id.get)
      else
        LABEL_NOT_FOUND_SELECTIVITY

    case _  =>
      PREDICATE_SELECTIVITY
  }
}
