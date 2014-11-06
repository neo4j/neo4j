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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast.{IntegerLiteral, LabelName}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

object GuessingEstimation {
  val LABEL_NOT_FOUND_SELECTIVITY       : Selectivity = 0.0
  val PREDICATE_SELECTIVITY             : Selectivity = 0.2
  val INDEX_SEEK_SELECTIVITY            : Selectivity = 0.02
  val DEFAULT_EXPAND_RELATIONSHIP_DEGREE: Multiplier  = 2.0
  val DEFAULT_CONNECTIVITY_CHANCE       : Multiplier  = 1.0
}

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel)
  extends Metrics.CardinalityModel {
  def apply(plan: LogicalPlan): Cardinality = computeCardinality(plan.solved)

  private def computeCardinality(query: PlannerQuery): Cardinality = {
    val (cardinality, _) = query.fold[(Cardinality, Map[IdName, Seq[LabelName]])]((Cardinality(1), Map.empty)) {
      case ((acc, labels), PlannerQuery(graph, horizon, _)) =>
        val (graphCardinality, newLabels) = calculateCardinalityForQueryGraph(graph, labels)
        val horizonCardinality = calculateCardinalityForQueryHorizon(graphCardinality * acc, horizon)
        (horizonCardinality, newLabels)
    }
    cardinality
  }

  private def calculateCardinalityForQueryHorizon(in: Cardinality, horizon: QueryHorizon): Cardinality = horizon match {
    case RegularQueryProjection(_, QueryShuffle(_, None, Some(limit: IntegerLiteral))) =>
      Cardinality(Math.min(in.amount.toLong, limit.value))

    case _: AggregatingQueryProjection =>
      Cardinality(Math.sqrt(in.amount))

    case _: RegularQueryProjection =>
      in
  }

  private def calculateCardinalityForQueryGraph(graph: QueryGraph, labels: Map[IdName, Seq[LabelName]]): (Cardinality, Map[IdName, Seq[LabelName]]) = {
    val cardinality = queryGraphCardinalityModel(graph, labels)
    val graphLabels = graph.patternNodeLabels.mapValues(_.toSeq)
    val newLabels = labels.fuse(graphLabels)(_ ++ _)
    (cardinality, newLabels)
  }

  implicit class PowerMap[A, B](m: Map[A, B]) {
    def fuse(other: Map[A, B])(f: (B, B) => B): Map[A, B] = {
      other.foldLeft(m) {
        case (acc, (k, v)) if acc.contains(k) => acc + (k -> f(acc(k), v))
        case (acc, entry)                     => acc + entry
      }
    }
  }
}
