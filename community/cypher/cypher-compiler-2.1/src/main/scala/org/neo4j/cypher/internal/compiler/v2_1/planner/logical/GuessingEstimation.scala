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

import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.spi.GraphStatistics
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticTable

object GuessingEstimation {
  val LABEL_NOT_FOUND_SELECTIVITY: Double = 0.0
  val PREDICATE_SELECTIVITY: Double = 0.2
  val INDEX_SEEK_SELECTIVITY: Double = 0.08
  val UNIQUE_INDEX_SEEK_SELECTIVITY: Double = 0.05
  val DEFAULT_EXPAND_RELATIONSHIP_DEGREE: Double = 2.0
}

class StatisticsBackedCardinalityModel(statistics: GraphStatistics,
                                       selectivity: Metrics.SelectivityModel)
                                      (implicit semanticTable: SemanticTable) extends Metrics.CardinalityModel {
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
      math.min(cardinality(left), cardinality(right))

    case expand @ Expand(left, _, dir, types, _, _, length) =>
      val degree = degreeByRelationshipTypesAndDirection(types, dir)
      cardinality(left) * math.pow(degree, averagePathLength(length))

    case expand @ OptionalExpand(left, _, dir, types, _, _, length, predicates) =>
      val degree = degreeByRelationshipTypesAndDirection(types, dir)
      cardinality(left) * math.pow(degree, averagePathLength(length)) * predicateSelectivity(predicates)

    case Selection(predicates, left) =>
      cardinality(left) * predicateSelectivity(predicates)

    case CartesianProduct(left, right) =>
      cardinality(left) * cardinality(right)

    case Apply(outer, inner) =>
      cardinality(outer) * cardinality(inner)

    case semiApply @ SemiApply(outer, inner) =>
      semiApplyCardinality(outer, semiApply.subQuery.predicate.exp)

    case semiApply @ AntiSemiApply(outer, inner) =>
      semiApplyCardinality(outer, semiApply.subQuery.predicate.exp)

    case DirectedRelationshipByIdSeek(_, relIds, _, _) =>
      relIds.size

    case UndirectedRelationshipByIdSeek(_, relIds, _, _) =>
      relIds.size * 2

    case Projection(left, _) =>
      cardinality(left)

    case Optional(_, input) =>
      cardinality(input)

    case SingleRow(_) =>
      1

    case Sort(input, _) =>
      cardinality(input)

    case Skip(input, skip: ast.NumberLiteral) =>
      Math.max(0.0, cardinality(input) - skip.value.asInstanceOf[Number].doubleValue())

    case Skip(input, _) =>
      cardinality(input)

    case Limit(input, limit: ast.NumberLiteral) =>
      Math.min(cardinality(input), limit.value.asInstanceOf[Number].doubleValue())

    case Limit(input, _) =>
      cardinality(input)

    case SortedLimit(input, limit: ast.NumberLiteral, _) =>
      Math.min(cardinality(input), limit.value.asInstanceOf[Number].doubleValue())

    case SortedLimit(input, _, _) =>
      cardinality(input)

  }

  private def semiApplyCardinality(outer: LogicalPlan, exp: ast.Expression) =
    cardinality(outer) * predicateSelectivity(Seq(exp))

  def averagePathLength(length:PatternLength) = length match {
    case SimplePatternLength              => 1
    case VarPatternLength(_, Some(depth)) => depth
    case VarPatternLength(_, None)        => 42
  }

  private def degreeByRelationshipTypesAndDirection(types: Seq[ast.RelTypeName], dir: Direction) =
    if (types.size <= 0)
      DEFAULT_EXPAND_RELATIONSHIP_DEGREE
    else
      types.foldLeft(0.0)((sum, t) => sum + degreeByRelationshipTypeAndDirection(t.id, dir)) / types.size

  private def predicateSelectivity(predicates: Seq[ast.Expression]): Double =
    predicates.map(selectivity).foldLeft(1.0)(_ * _)

  private def degreeByRelationshipTypeAndDirection(optId: Option[RelTypeId], direction: Direction) = optId match {
    case Some(id) => statistics.degreeByRelationshipTypeAndDirection(id, direction)
    case None     => DEFAULT_EXPAND_RELATIONSHIP_DEGREE
  }

  private def cardinality(plan: LogicalPlan) = apply(plan)
}

class StatisticsBasedSelectivityModel(statistics: GraphStatistics)
                                     (implicit semanticTable: SemanticTable) extends Metrics.SelectivityModel {

  import GuessingEstimation._

  def apply(predicate: ast.Expression): Double = predicate match {
    case ast.HasLabels(_, Seq(label)) =>
      if (label.id.isDefined)
        statistics.nodesWithLabelSelectivity(label.id.get)
      else
        LABEL_NOT_FOUND_SELECTIVITY

    case ast.Not(inner) =>
      1.0 - apply(inner)

    case _  =>
      PREDICATE_SELECTIVITY
  }
}
