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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern.Selector
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Fby
import org.neo4j.cypher.internal.util.Last
import org.neo4j.cypher.internal.util.Multiplier

trait SelectivePathPatternCardinalityModel
    extends NodeConnectionManipulation
    with NodeCardinalityModel
    with PatternRelationshipCardinalityModel
    with QuantifiedPathPatternCardinalityModel {

  def getSelectivePathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    selectivePathPattern: SelectivePathPattern
  ): Cardinality = {
    val leftNodeCardinality =
      getNodeCardinality(context, labelInfo, selectivePathPattern.left).getOrElse(Cardinality.EMPTY)
    val rightNodeCardinality =
      getNodeCardinality(context, labelInfo, selectivePathPattern.right).getOrElse(Cardinality.EMPTY)
    // Here we need to inline back the predicates to the QPPs since we cannot handle ForAllRepetitions in cardinality estimation.
    val sppWithInlinedPredicates = expandSolverStep.inlineQPPPredicates(selectivePathPattern, Set.empty)

    sppWithInlinedPredicates.selector match {
      case Selector.Any(k) =>
        anyPathPatternCardinality(
          context = context,
          labelInfo = labelInfo,
          pathPattern = sppWithInlinedPredicates.pathPattern,
          selections = sppWithInlinedPredicates.selections,
          leftNodeCardinality = leftNodeCardinality,
          rightNodeCardinality = rightNodeCardinality,
          k = k
        )
      case Selector.Shortest(k) =>
        // whether we want any paths or the shortest paths doesn't change the cardinality, it only dictates which paths are going to be returned
        anyPathPatternCardinality(
          context = context,
          labelInfo = labelInfo,
          pathPattern = sppWithInlinedPredicates.pathPattern,
          selections = sppWithInlinedPredicates.selections,
          leftNodeCardinality = leftNodeCardinality,
          rightNodeCardinality = rightNodeCardinality,
          k = k
        )
      case Selector.ShortestGroups(k) =>
        shortestGroupsPathPatternCardinality(
          context = context,
          labelInfo = labelInfo,
          pathPattern = sppWithInlinedPredicates.pathPattern,
          selections = sppWithInlinedPredicates.selections,
          leftNodeCardinality = leftNodeCardinality,
          rightNodeCardinality = rightNodeCardinality,
          k = k
        )
    }
  }

  private def anyPathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    pathPattern: NodeConnections[ExhaustiveNodeConnection],
    selections: Selections,
    leftNodeCardinality: Cardinality,
    rightNodeCardinality: Cardinality,
    k: Long
  ): Cardinality = {
    val patternCardinality = pathPatternCardinality(context, labelInfo, pathPattern, selections)
    Cardinality.min(patternCardinality, leftNodeCardinality * rightNodeCardinality * Multiplier(k))
  }

  private def shortestGroupsPathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    pathPattern: NodeConnections[ExhaustiveNodeConnection],
    selections: Selections,
    leftNodeCardinality: Cardinality,
    rightNodeCardinality: Cardinality,
    k: Long
  ): Cardinality = {
    val increasinglyLargerPatternsCardinalities = increasinglyLargerPatterns(pathPattern).map(resizedPattern =>
      pathPatternCardinality(context, labelInfo, resizedPattern, selections)
    )

    increasinglyLargerPatternsCardinalities
      .find { cardinality =>
        // This is a very rough approximation, we want to find the smallest pattern so that we have at least `k` paths per partition on average.
        // The average number of paths connecting two arbitrary endpoints in the graph is given by: `cardinality / lhsCardinality / rhsCardinality`.
        cardinality >= leftNodeCardinality * rightNodeCardinality * Multiplier(k)
      }.getOrElse(increasinglyLargerPatternsCardinalities.last)
  }

  private def pathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    pathPattern: NodeConnections[ExhaustiveNodeConnection],
    selections: Selections
  ): Cardinality = {
    val predicates = QueryGraphPredicates.partitionSelections(labelInfo, selections)
    val patternCardinality = pathPattern.connections match {
      case Fby(head, tail) =>
        val headCardinality =
          getExhaustiveNodeConnectionCardinality(context, predicates.allLabelInfo, predicates.uniqueRelationships, head)
        tail.foldLeft(headCardinality) { case (cardinality, connection) =>
          val connectionCardinality =
            getExhaustiveNodeConnectionCardinality(context, labelInfo, predicates.uniqueRelationships, connection)
          val leftNodeCardinality =
            getNodeCardinality(context, labelInfo, connection.left).getOrElse(Cardinality.EMPTY)
          val connectionMultiplier =
            Multiplier.ofDivision(
              dividend = connectionCardinality,
              divisor = leftNodeCardinality
            ).getOrElse(Multiplier.ZERO)
          cardinality * connectionMultiplier
        }
      case Last(head) =>
        getExhaustiveNodeConnectionCardinality(context, predicates.allLabelInfo, predicates.uniqueRelationships, head)
    }

    val otherPredicatesSelectivity = context.predicatesSelectivity(predicates.allLabelInfo, predicates.otherPredicates)

    patternCardinality * otherPredicatesSelectivity
  }

  private def getExhaustiveNodeConnectionCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    uniqueRelationships: Set[LogicalVariable],
    exhaustiveNodeConnection: ExhaustiveNodeConnection
  ): Cardinality =
    exhaustiveNodeConnection match {
      case relationship: PatternRelationship =>
        getRelationshipCardinality(
          context,
          labelInfo,
          relationship,
          uniqueRelationships.contains(relationship.variable)
        )
      case quantifiedPathPattern: QuantifiedPathPattern =>
        getQuantifiedPathPatternCardinality(context, labelInfo, quantifiedPathPattern, uniqueRelationships)
    }
}
