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

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality._
import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, ast}
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.spi.{TokenContext, GraphStatistics}
import org.neo4j.graphdb.Direction

object GuessingEstimation {
  val LABEL_NOT_FOUND_SELECTIVITY = Selectivity(0.0)
  val PREDICATE_SELECTIVITY = Selectivity(0.2)
  val INDEX_SEEK_SELECTIVITY = Selectivity(0.02)
  val DEFAULT_EXPAND_RELATIONSHIP_DEGREE = Multiplier(2.0)
  val DEFAULT_CONNECTIVITY_CHANCE = Multiplier(1.0)
}

class StatisticsBackedCardinalityModel(statistics: GraphStatistics,
                                       selectivity: Metrics.SelectivityModel)
                                      (implicit semanticTable: SemanticTable) extends Metrics.CardinalityModel {
  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.GuessingEstimation._

  private val queryGraphCardinalityModel = QueryGraphCardinalityModel(
    statistics,
    producePredicates,
    groupPredicates(estimateSelectivity(statistics, semanticTable)),
    combinePredicates
  )

  def apply(plan: LogicalPlan): Cardinality = plan match {
    case
      _: AllNodesScan | _: NodeByLabelScan | _: NodeByIdSeek | _: NodeIndexUniqueSeek | _: NodeHashJoin |
      _: Expand | _: OuterHashJoin | _: OptionalExpand | _: FindShortestPaths | _: Selection | _: Apply |
      _: Apply | _: SemiApply | _: LetSemiApply | _: LetAntiSemiApply | _: SelectOrSemiApply | _: LetSelectOrSemiApply |
      _: SelectOrAntiSemiApply | _: LetSelectOrAntiSemiApply | _: DirectedRelationshipByIdSeek |
      _: UndirectedRelationshipByIdSeek | _: DirectedRelationshipByIdSeek | _: Optional =>

      queryGraphCardinalityModel(plan.solved.lastQueryGraph)

    case CartesianProduct(left, right) =>
      cardinality(left) * cardinality(right)

    case Projection(left, _) =>
      cardinality(left)

    case ProjectEndpoints(left, _, _, _, directed, _) =>
      if (directed) cardinality(left) else cardinality(left) * Multiplier(2)

    case SingleRow(_) =>
      Cardinality(1)

    case Sort(input, _) =>
      cardinality(input)

    case Skip(input, skip: ast.NumberLiteral) =>
      Cardinality(
        Math.max(
          0.0,
          cardinality(input).amount - skip.value.asInstanceOf[Number].doubleValue()
        )
      )

    case Skip(input, _) =>
      cardinality(input)

    case Limit(input, limit: ast.NumberLiteral) =>
      Cardinality(
        Math.min(
          cardinality(input).amount,
          limit.value.asInstanceOf[Number].doubleValue()
        )
      )

    case Limit(input, _) =>
      cardinality(input)

    case SortedLimit(input, limit: ast.NumberLiteral, _) =>
      Cardinality(
        Math.min(
          cardinality(input).amount,
          limit.value.asInstanceOf[Number].doubleValue()
        )
      )

    case SortedLimit(input, _, _) =>
      cardinality(input)
  }

  def averagePathLength(length:PatternLength) = length match {
    case SimplePatternLength              => 1
    case VarPatternLength(_, Some(depth)) => depth
    case VarPatternLength(_, None)        => 42
  }

  private def degreeByRelationshipTypesAndDirection(types: Seq[ast.RelTypeName], dir: Direction): Multiplier =
    if (types.size <= 0)
      DEFAULT_EXPAND_RELATIONSHIP_DEGREE
    else
      types.map(_.id).map(degreeByRelationshipTypeAndDirection(_, dir)).reduce(_ + _)

  private def predicateSelectivity(predicates: Seq[ast.Expression]): Selectivity =
    predicates.map(selectivity).foldLeft(Selectivity(1))(_ * _)

  private def degreeByRelationshipTypeAndDirection(optId: Option[RelTypeId], direction: Direction): Multiplier = optId match {
    case Some(id) =>
      // FIXME: Make Cardinality type safe with regards to the type of entity (Node/Relationship).
      // FIXME: Make / produce a Multiplier for different units and a Selectivity for the same unit.
      Multiplier(statistics.cardinalityByLabelsAndRelationshipType(None, Some(id), None).amount / statistics.nodesWithLabelCardinality(None).amount)
    case None =>
      DEFAULT_EXPAND_RELATIONSHIP_DEGREE
  }

  private def cardinality(plan: LogicalPlan) = apply(plan)
}

class StatisticsBasedSelectivityModel(statistics: GraphStatistics)
                                     (implicit semanticTable: SemanticTable) extends Metrics.SelectivityModel {

  import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.GuessingEstimation._

  def apply(predicate: ast.Expression): Selectivity = predicate match {
    case ast.HasLabels(_, Seq(label)) =>
      if (label.id.isDefined)
        statistics.nodesWithLabelCardinality(label.id) / statistics.nodesWithLabelCardinality(None)
      else
        LABEL_NOT_FOUND_SELECTIVITY

    case ast.Not(inner) =>
      apply(inner).inverse

    case _  =>
      PREDICATE_SELECTIVITY
  }
}
