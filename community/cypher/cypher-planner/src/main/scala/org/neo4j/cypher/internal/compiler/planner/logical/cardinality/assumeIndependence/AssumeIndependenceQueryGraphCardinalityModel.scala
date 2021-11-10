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
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel.CardinalityAndInput
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier

case class AssumeIndependenceQueryGraphCardinalityModel(planContext: PlanContext,
                                                        selectivityCalculator: SelectivityCalculator,
                                                        combiner: SelectivityCombiner) extends QueryGraphCardinalityModel {

  private implicit val numericMultiplier: NumericMultiplier.type = NumericMultiplier

  private val relMultiplierCalculator = PatternRelationshipMultiplierCalculator(planContext.statistics, combiner)

  def apply(queryGraph: QueryGraph,
            input: QueryGraphSolverInput,
            semanticTable: SemanticTable,
            indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Cardinality = {
    val cardinalityAndInput = CardinalityAndInput(Cardinality.SINGLE, input)
    // Fold over query graph and optional query graphs, aggregating cardinality and label info using QueryGraphSolverInput
    val afterOuter = visitQueryGraph(queryGraph, cardinalityAndInput, semanticTable, indexPredicateProviderContext)
    val afterOptionalMatches = visitOptionalMatchQueryGraphs(queryGraph.optionalMatches, afterOuter, semanticTable, indexPredicateProviderContext)
    afterOptionalMatches.cardinality
  }

  private def visitQueryGraph(outer: QueryGraph,
                              cardinalityAndInput: CardinalityAndInput,
                              semanticTable: SemanticTable,
                              indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): CardinalityAndInput = {
    cardinalityAndInput.copy(cardinality = cardinalityForQueryGraph(outer, cardinalityAndInput.input, semanticTable, indexPredicateProviderContext))
  }

  private def visitOptionalMatchQueryGraphs(optionals: Seq[QueryGraph],
                                            cardinalityAndInput: CardinalityAndInput,
                                            semanticTable: SemanticTable,
                                            indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): CardinalityAndInput = {
    optionals.foldLeft(cardinalityAndInput) { case (current, optional) =>
      visitOptionalQueryGraph(optional, current, semanticTable, indexPredicateProviderContext)
    }
  }

  private def visitOptionalQueryGraph(optional: QueryGraph,
                                      cardinalityAndInput: CardinalityAndInput,
                                      semanticTable: SemanticTable,
                                      indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): CardinalityAndInput = {
    val inputWithKnownLabelInfo = cardinalityAndInput.input.withFusedLabelInfo(optional.selections.labelInfo)
    val optionalCardinality = cardinalityAndInput.cardinality * cardinalityForQueryGraph(optional, inputWithKnownLabelInfo, semanticTable, indexPredicateProviderContext)
    // OPTIONAL MATCH can't decrease cardinality
    cardinalityAndInput.copy(
      cardinality = Cardinality.max(cardinalityAndInput.cardinality, optionalCardinality),
      input = inputWithKnownLabelInfo
    )
  }

  private def cardinalityForQueryGraph(qg: QueryGraph,
                                       input: QueryGraphSolverInput,
                                       semanticTable: SemanticTable,
                                       indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Cardinality = {
    val patternMultiplier = calculateMultiplier(qg, input.labelInfo, input.relTypeInfo, semanticTable, indexPredicateProviderContext)
    val numberOfPatternNodes = qg.patternNodes.count { n =>
      !qg.argumentIds.contains(n) && !qg.patternRelationships.exists(r =>
        qg.argumentIds.contains(r.name) && Seq(r.left, r.right).contains(n)
      )
    }

    val numberOfGraphNodes = planContext.statistics.nodesAllCardinality()

    (numberOfGraphNodes ^ numberOfPatternNodes) * patternMultiplier
  }

  private def calculateMultiplier(qg: QueryGraph,
                                  labels: LabelInfo,
                                  relTypes: RelTypeInfo,
                                  semanticTable: SemanticTable,
                                  indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Multiplier = {
    val expressionSelectivity = selectivityCalculator(qg.selections, labels, relTypes, semanticTable, indexPredicateProviderContext)

    val patternRelationships = qg.patternRelationships.toIndexedSeq
    val patternMultipliers = patternRelationships.map(r =>
      if (qg.argumentIds.contains(r.name)) Multiplier.ONE
      else relMultiplierCalculator.relationshipMultiplier(r, labels)(semanticTable)
    )

    patternMultipliers.product * expressionSelectivity
  }
}
