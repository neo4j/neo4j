/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel.CardinalityAndInput
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.ExpressionSelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier
import org.neo4j.cypher.internal.util.Selectivity

case class AssumeIndependenceQueryGraphCardinalityModel(stats: GraphStatistics, combiner: SelectivityCombiner) extends QueryGraphCardinalityModel {

  private implicit val numericCardinality: NumericCardinality.type = NumericCardinality
  private implicit val numericMultiplier: NumericMultiplier.type = NumericMultiplier

  override val expressionSelectivityCalculator: ExpressionSelectivityCalculator = ExpressionSelectivityCalculator(stats, combiner)
  private val relMultiplierCalculator = PatternRelationshipMultiplierCalculator(stats, combiner)

  def apply(queryGraph: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    val cardinalityAndInput = CardinalityAndInput(Cardinality.SINGLE, input)
    // Fold over query graph and optional query graphs, aggregating cardinality and label info using QueryGraphSolverInput
    val afterOuter = visitQueryGraph(queryGraph, cardinalityAndInput, semanticTable)
    val afterOptionalMatches = visitOptionalMatchQueryGraphs(queryGraph.optionalMatches, afterOuter, semanticTable)
    afterOptionalMatches.cardinality
  }

  private def visitQueryGraph(outer: QueryGraph, cardinalityAndInput: CardinalityAndInput, semanticTable: SemanticTable): CardinalityAndInput = {
    cardinalityAndInput.copy(cardinality = cardinalityForQueryGraph(outer, cardinalityAndInput.input, semanticTable))
  }

  private def visitOptionalMatchQueryGraphs(optionals: Seq[QueryGraph], cardinalityAndInput: CardinalityAndInput, semanticTable: SemanticTable): CardinalityAndInput = {
    optionals.foldLeft(cardinalityAndInput) { case (current, optional) =>
      visitOptionalQueryGraph(optional, current, semanticTable)
    }
  }

  private def visitOptionalQueryGraph(optional: QueryGraph, cardinalityAndInput: CardinalityAndInput, semanticTable: SemanticTable): CardinalityAndInput = {
    val inputWithKnownLabelInfo = cardinalityAndInput.input.withFusedLabelInfo(optional.selections.labelInfo)
    val optionalCardinality = cardinalityAndInput.cardinality * cardinalityForQueryGraph(optional, inputWithKnownLabelInfo, semanticTable)
    // OPTIONAL MATCH can't decrease cardinality
    cardinalityAndInput.copy(
      cardinality = Cardinality.max(cardinalityAndInput.cardinality, optionalCardinality),
      input = inputWithKnownLabelInfo
    )
  }

  private def cardinalityForQueryGraph(qg: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    val patternMultiplier = calculateMultiplier(qg, input.labelInfo, semanticTable)
    val numberOfPatternNodes = qg.patternNodes.count(!qg.argumentIds.contains(_))
    val numberOfGraphNodes = stats.nodesAllCardinality()

    (numberOfGraphNodes ^ numberOfPatternNodes) * patternMultiplier
  }

  private def calculateMultiplier(qg: QueryGraph, labels: LabelInfo, semanticTable: SemanticTable): Multiplier = {
    val expressions = qg.selections.flatPredicates
    val expressionSelectivities = expressions.map(expressionSelectivityCalculator(_, labels)(semanticTable))
    val expressionSelectivity = combiner.andTogetherSelectivities(expressionSelectivities)
                                        .getOrElse(Selectivity.ONE)

    val patternRelationships = qg.patternRelationships.toIndexedSeq
    val patternMultipliers = patternRelationships.map(relMultiplierCalculator.relationshipMultiplier(_, labels)(semanticTable))

    patternMultipliers.product * expressionSelectivity
  }
}
