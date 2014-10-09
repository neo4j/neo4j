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

import org.neo4j.cypher.internal.compiler.v2_2.ast
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._

object GuessingEstimation {
  val LABEL_NOT_FOUND_SELECTIVITY: Selectivity = 0.0
  val PREDICATE_SELECTIVITY: Selectivity = 0.2
  val INDEX_SEEK_SELECTIVITY: Selectivity = 0.02
  val DEFAULT_EXPAND_RELATIONSHIP_DEGREE: Multiplier = 2.0
  val DEFAULT_CONNECTIVITY_CHANCE: Multiplier = 1.0
}

class StatisticsBackedCardinalityModel(queryGraphCardinalityModel: QueryGraphCardinalityModel)
  extends Metrics.CardinalityModel {
  def apply(plan: LogicalPlan): Cardinality = plan match {
    case
      _: AllNodesScan | _: NodeByLabelScan | _: NodeByIdSeek | _: NodeIndexUniqueSeek | _: NodeHashJoin |
      _: Expand | _: OuterHashJoin | _: OptionalExpand | _: FindShortestPaths | _: Selection | _: Apply |
      _: SemiApply | _: LetSemiApply | _: LetAntiSemiApply | _: SelectOrSemiApply | _: LetSelectOrSemiApply |
      _: SelectOrAntiSemiApply | _: LetSelectOrAntiSemiApply | _: DirectedRelationshipByIdSeek |
      _: UndirectedRelationshipByIdSeek | _: DirectedRelationshipByIdSeek | _: Optional | _: NodeIndexSeek  |
      _: AntiSemiApply | _: LegacyIndexSeek =>

      queryGraphCardinalityModel(plan.solved.lastQueryGraph)

    case CartesianProduct(left, right) =>
      cardinality(left) * cardinality(right)

    case Projection(left, _) =>
      cardinality(left)

    case ProjectEndpoints(left, _, _, _, directed, _) =>
      if (directed) cardinality(left) else cardinality(left) * Multiplier(2)

    case SingleRow(_) =>
      1.0

    case Sort(input, _) =>
      cardinality(input)

    case Skip(input, skip: ast.NumberLiteral) =>
      Math.max(
          0.0,
          cardinality(input).amount - skip.value.asInstanceOf[Number].doubleValue()
        )


    case Skip(input, _) =>
      cardinality(input)

    case Limit(input, limit: ast.NumberLiteral) =>
      Math.min(
        cardinality(input).amount,
        limit.value.asInstanceOf[Number].doubleValue()
      )

    case Limit(input, _) =>
      cardinality(input)

    case SortedLimit(input, limit: ast.NumberLiteral, _) =>
      Math.min(
        cardinality(input).amount,
        limit.value.asInstanceOf[Number].doubleValue()
      )

    case _: Aggregation =>
      1.0
    case _: UnwindCollection =>
      1.0

    case SortedLimit(input, _, _) =>
      cardinality(input)

    case Union(l, r) =>
      cardinality(l) + cardinality(r)
  }

  private def cardinality(plan: LogicalPlan): Cardinality = apply(plan)
}
