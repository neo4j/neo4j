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

import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan

object GuessingEstimation {
  val ALL_NODES_SCAN_CARDINALITY: Int = 1000
  val LABEL_NOT_FOUND_SELECTIVITY: Double = 0.0
  val LABEL_SELECTIVITY: Double = 0.1
  val PREDICATE_SELECTIVITY: Double = 0.2
  val INDEX_SEEK_SELECTIVITY: Double = 0.08
  val UNIQUE_INDEX_SEEK_SELECTIVITY: Double = 0.05
  val EXPAND_RELATIONSHIP_SELECTIVITY: Double = 0.02
}

class GuessingCardinalityEstimator(selectivity: Metrics.selectivityEstimator) extends Metrics.cardinalityEstimator {

  import GuessingEstimation._

  def apply(plan: LogicalPlan): Int = plan match {
    case AllNodesScan(_) =>
      ALL_NODES_SCAN_CARDINALITY

    case NodeByLabelScan(_, Left(_)) =>
      (ALL_NODES_SCAN_CARDINALITY * LABEL_NOT_FOUND_SELECTIVITY).toInt

    case NodeByLabelScan(_, Right(_)) =>
      (ALL_NODES_SCAN_CARDINALITY * LABEL_SELECTIVITY).toInt

    case NodeByIdSeek(_, nodeIds) =>
      nodeIds.size

    case NodeIndexSeek(_, _, _, _) =>
      (ALL_NODES_SCAN_CARDINALITY * INDEX_SEEK_SELECTIVITY).toInt

    case NodeIndexUniqueSeek(_, _, _, _) =>
      (ALL_NODES_SCAN_CARDINALITY * UNIQUE_INDEX_SEEK_SELECTIVITY).toInt

    case NodeHashJoin(_, left, right) =>
      (cardinality(left) + cardinality(right)) / 2

    case Expand(left, _, _, _, _, _) =>
      (cardinality(left) * EXPAND_RELATIONSHIP_SELECTIVITY).toInt

    case Selection(predicates, left) =>
      (cardinality(left) * predicates.map(selectivity).foldLeft(1.0)(_ * _)).toInt

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

class GuessingSelectivityEstimator extends Metrics.selectivityEstimator {

  import GuessingEstimation._

  def apply(predicate: Expression): Double = predicate match {
    case HasLabels(_, Seq(label)) => if (label.id.isDefined) LABEL_SELECTIVITY else LABEL_NOT_FOUND_SELECTIVITY
    case _ => PREDICATE_SELECTIVITY
  }
}
