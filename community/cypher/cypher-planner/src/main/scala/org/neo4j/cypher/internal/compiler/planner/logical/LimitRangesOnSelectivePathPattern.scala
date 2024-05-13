/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.PredicateConverter
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.UpperBound

/**
 * Update large quantified ranges in Selective Path Patterns to instead be a size restricted pre-filter predicate to limit size of built NFA and add path length restriction.
 */
case object LimitRangesOnSelectivePathPattern {
  private val topMin = 100
  private val topMax = 100

  def apply(spp: SelectivePathPattern): SelectivePathPattern = {
    val (boundaryPredicates, newConnections) =
      spp.pathPattern.connections.foldMap(Set.empty[Expression]) {
        case (extractedPredicates, nodeConnection) =>
          nodeConnection match {
            case patternRelationship: PatternRelationship =>
              patternRelationship.length match {
                case VarPatternLength(min, max) =>
                  limitRangesOnPatternRelationship(extractedPredicates, patternRelationship, min, max)
                case _ => (extractedPredicates, patternRelationship)
              }
            case qpp: QuantifiedPathPattern =>
              limitRangesOnQPP(extractedPredicates, qpp)
          }
      }

    spp.copy(
      pathPattern = spp.pathPattern.copy(connections = newConnections),
      selections = spp.selections ++ Selections(boundaryPredicates.flatMap(_.asPredicates))
    )
  }

  private def limitRangesOnPatternRelationship(
    extractedPredicates: Set[Expression],
    patternRelationship: PatternRelationship,
    min: Int,
    max: Option[Int]
  ): (Set[Expression], PatternRelationship) = {
    val startRelationship = patternRelationship.relationships.headOption
    val (newMin, maybeMinExpression) =
      if (min < topMin)
        (min, None)
      else
        (1, startRelationship.map(minRestrictionPredicate(_, min.toString)))
    val (newMax, maybeMaxExpression) = max match {
      case None =>
        (None, None)
      case Some(limit) =>
        if (limit < topMax)
          (Some(limit), None)
        else
          (None, startRelationship.map(maxRestrictionPredicate(_, limit.toString)))
    }
    val prWithUpdatedRange = patternRelationship.copy(length = VarPatternLength(newMin, newMax))

    (extractedPredicates ++ maybeMaxExpression ++ maybeMinExpression, prWithUpdatedRange)
  }

  private def limitRangesOnQPP(
    extractedPredicates: Set[Expression],
    qpp: QuantifiedPathPattern
  ): (Set[Expression], QuantifiedPathPattern) = {
    val amountOfRelationships = qpp.relationshipVariableGroupings.size
    val startRelationship = qpp.relationshipVariableGroupings.headOption.map(_.group)
    val (newMin, maybeMinExpression) =
      if (qpp.repetition.min * amountOfRelationships < topMin)
        (qpp.repetition.min.toInt, None)
      else
        (1, startRelationship.map(minRestrictionPredicate(_, qpp.repetition.min.toString)))
    val (newMax, maybeMaxExpression) = qpp.repetition.max match {
      case UpperBound.Unlimited =>
        (UpperBound.Unlimited, None)
      case UpperBound.Limited(limit) =>
        if (limit * amountOfRelationships < topMax)
          (UpperBound.Limited(limit), None)
        else
          (UpperBound.Unlimited, startRelationship.map(maxRestrictionPredicate(_, limit.toString)))
    }
    val qppWithUpdatedRange = qpp.copy(repetition = qpp.repetition.copy(min = newMin, max = newMax))

    (extractedPredicates ++ maybeMaxExpression ++ maybeMinExpression, qppWithUpdatedRange)
  }

  private def minRestrictionPredicate(variable: LogicalVariable, min: String): GreaterThanOrEqual = {
    val pos = InputPosition.NONE
    GreaterThanOrEqual(
      FunctionInvocation(FunctionName("size")(pos), variable)(pos),
      SignedDecimalIntegerLiteral(min)(pos)
    )(pos)
  }

  private def maxRestrictionPredicate(variable: LogicalVariable, max: String): LessThanOrEqual = {
    val pos = InputPosition.NONE
    LessThanOrEqual(
      FunctionInvocation(FunctionName("size")(pos), variable)(pos),
      SignedDecimalIntegerLiteral(max)(pos)
    )(pos)
  }
}
