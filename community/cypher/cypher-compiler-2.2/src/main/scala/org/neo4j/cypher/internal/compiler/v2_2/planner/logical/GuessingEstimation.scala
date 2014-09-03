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

import org.neo4j.cypher.internal.compiler.v2_2.{RelTypeId, ast}
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics
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

  def apply(plan: LogicalPlan): Cardinality = plan match {
    case AllNodesScan(_, _) =>
      statistics.nodesWithLabelCardinality(None)

    case NodeByLabelScan(_, Left(_), _) =>
      statistics.nodesWithLabelCardinality(None) * LABEL_NOT_FOUND_SELECTIVITY

    case NodeByLabelScan(_, Right(labelId), _) =>
      statistics.nodesWithLabelCardinality(Some(labelId))

    case NodeByIdSeek(_, EntityByIdParameter(_), _) =>
      Cardinality(1)

    case NodeByIdSeek(_, EntityByIdExprs(exprs), _) =>
      Cardinality(exprs.size)

    case NodeIndexSeek(_, _, _, _, _) =>
      statistics.nodesWithLabelCardinality(None) * INDEX_SEEK_SELECTIVITY

    case NodeIndexUniqueSeek(_, _, _, _, _) =>
      Cardinality(1)

    case NodeHashJoin(_, left, right) =>
      Cardinality(math.min(cardinality(left).amount, cardinality(right).amount))

    case OuterHashJoin(_, left, right) =>
      Cardinality(math.min(cardinality(left).amount, cardinality(right).amount))

    case expand @ Expand(left, _, dir, _, types, _, _, length) =>
      val degree = degreeByRelationshipTypesAndDirection(types, dir).coefficient
      cardinality(left) * Multiplier(math.pow(degree, averagePathLength(length)))

    case expand @ OptionalExpand(left, _, dir, types, _, _, length, predicates) =>
      val degree = degreeByRelationshipTypesAndDirection(types, dir).coefficient
      cardinality(left) * Multiplier(math.pow(degree, averagePathLength(length))) * predicateSelectivity(predicates)

    case FindShortestPaths(left, ShortestPathPattern(_, rel, true)) =>
      cardinality(left) * DEFAULT_CONNECTIVITY_CHANCE

    case FindShortestPaths(left, ShortestPathPattern(_, rel, false)) =>
      val degree = degreeByRelationshipTypesAndDirection(rel.types, rel.dir).coefficient
      cardinality(left) * Multiplier(math.pow(degree, averagePathLength(rel.length)))

    case Selection(predicates, left) =>
      cardinality(left) * predicateSelectivity(predicates)

    case CartesianProduct(left, right) =>
      cardinality(left) * cardinality(right)

    case Apply(outer, inner) =>
      cardinality(outer) * cardinality(inner)

    case semiApply @ SemiApply(outer, inner) =>
      cardinality(outer) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case semiApply @ LetSemiApply(outer, inner, _) =>
      cardinality(outer) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case semiApply @ AntiSemiApply(outer, inner) =>
      cardinality(outer)
      // TODO: This is not true. We should calculate cardinality on QG and not LP
//    private def semiApplyCardinality(outer: LogicalPlan, exp: ast.Expression) = cardinality(outer) * predicateSelectivity(Seq(exp))

    case semiApply @ LetAntiSemiApply(outer, inner, _) =>
      cardinality(outer) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case selectOrSemiApply @ SelectOrSemiApply(outer, inner, expr) =>
      cardinality(outer) * predicateSelectivity(Seq(expr)) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case selectOrSemiApply @ LetSelectOrSemiApply(outer, inner, _, expr) =>
      cardinality(outer) * predicateSelectivity(Seq(expr)) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case selectOrSemiApply @ SelectOrAntiSemiApply(outer, inner, expr) =>
      cardinality(outer) * predicateSelectivity(Seq(expr)) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case selectOrSemiApply @ LetSelectOrAntiSemiApply(outer, inner, _, expr) =>
      cardinality(outer) * predicateSelectivity(Seq(expr)) // TODO: This is not true. We should calculate cardinality on QG and not LP

    case DirectedRelationshipByIdSeek(_, EntityByIdParameter(_), _, _, _) =>
      Cardinality(1)

    case DirectedRelationshipByIdSeek(_, EntityByIdExprs(exprs), _, _, _) =>
      Cardinality(exprs.size)

    case UndirectedRelationshipByIdSeek(_, EntityByIdParameter(_), _, _, _) =>
      Cardinality(2)

    case UndirectedRelationshipByIdSeek(_, EntityByIdExprs(exprs), _, _, _) =>
      Cardinality(exprs.size) * Multiplier(2)

    case Projection(left, _) =>
      cardinality(left)

    case ProjectEndpoints(left, _, _, _, directed, _) =>
      if (directed) cardinality(left) else cardinality(left) * Multiplier(2)

    case Optional(input) =>
      cardinality(input)

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
