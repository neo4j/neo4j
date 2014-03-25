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

class GuessingEstimator extends CardinalityEstimator {
  private val ALL_NODES_SCAN_CARDINALITY: Int = 1000
  private val LABEL_NOT_FOUND_SELECTIVITY: Double = 0.0
  private val LABEL_SELECTIVITY: Double = 0.1
  private val PREDICATE_SELECTIVITY: Double = 0.2
  private val INDEX_SEEK_SELECTIVITY: Double = 0.08
  private val UNIQUE_INDEX_SEEK_SELECTIVITY: Double = 0.05
  private val EXPAND_RELATIONSHIP_SELECTIVITY: Double = 0.02

  def estimate(plan: LogicalPlan): Int = plan match {
    case AllNodesScan(_) =>
      ALL_NODES_SCAN_CARDINALITY

    case NodeByLabelScan(_, Left(_)) =>
      (ALL_NODES_SCAN_CARDINALITY * LABEL_NOT_FOUND_SELECTIVITY).toInt

    case NodeByLabelScan(_, Right(_)) =>
      (ALL_NODES_SCAN_CARDINALITY * LABEL_SELECTIVITY).toInt

    case NodeByIdSeek(_, _, numberOfNodeIdsEstimate) =>
      numberOfNodeIdsEstimate

    case NodeIndexSeek(_, _, _, _) =>
      (ALL_NODES_SCAN_CARDINALITY * INDEX_SEEK_SELECTIVITY).toInt

    case NodeIndexUniqueSeek(_, _, _, _) =>
      (ALL_NODES_SCAN_CARDINALITY * UNIQUE_INDEX_SEEK_SELECTIVITY).toInt

    case NodeHashJoin(_, left, right) =>
      (estimate(left) + estimate(right)) / 2

    case Expand(left, _, _, _, _, _) =>
      (estimate(left) * EXPAND_RELATIONSHIP_SELECTIVITY).toInt

    case Selection(predicates, left) =>
      (estimate(left) * predicates.map(predicateSelectivity).foldLeft(1.0)(_ * _)).toInt

    case CartesianProduct(left, right) =>
      estimate(left) * estimate(right)

    case DirectedRelationshipByIdSeek(_, _, numberOfRelIdsEstimate, _, _) =>
      numberOfRelIdsEstimate

    case UndirectedRelationshipByIdSeek(_, _, numberOfRelIdsEstimate, _, _) =>
      numberOfRelIdsEstimate * 2

    case Projection(left, _) =>
      estimate(left)

    case SingleRow() =>
      1
  }

  private def predicateSelectivity(predicate: Expression): Double = predicate match {
    case HasLabels(_, Seq(label)) => if (label.id.isDefined) LABEL_SELECTIVITY else LABEL_NOT_FOUND_SELECTIVITY
    case _ => PREDICATE_SELECTIVITY
  }
}
