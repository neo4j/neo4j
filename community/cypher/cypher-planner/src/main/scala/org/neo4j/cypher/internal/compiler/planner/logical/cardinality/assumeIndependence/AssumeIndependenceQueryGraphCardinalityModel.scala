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
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.SelectivityCalculator
import org.neo4j.cypher.internal.compiler.planner.logical.StatisticsBackedCardinalityModel.CardinalityAndInput
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.SelectivityCombiner
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Multiplier.NumericMultiplier

case class AssumeIndependenceQueryGraphCardinalityModel(
  planContext: PlanContext,
  selectivityCalculator: SelectivityCalculator,
  combiner: SelectivityCombiner
) extends QueryGraphCardinalityModel {

  implicit private val numericMultiplier: NumericMultiplier.type = NumericMultiplier

  private val nodeConnectionMultiplierCalculator = NodeConnectionMultiplierCalculator(planContext.statistics, combiner)

  override def apply(
    queryGraph: QueryGraph,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Cardinality = {
    val cardinalityAndInput = CardinalityAndInput(Cardinality.SINGLE, labelInfo, relTypeInfo)
    // Fold over query graph and optional query graphs, aggregating cardinality and label info using QueryGraphSolverInput
    val afterOuter =
      visitQueryGraph(queryGraph, cardinalityAndInput, semanticTable, indexPredicateProviderContext, cardinalityModel)
    val afterOptionalMatches = visitOptionalMatchQueryGraphs(
      queryGraph.optionalMatches,
      afterOuter,
      semanticTable,
      indexPredicateProviderContext,
      cardinalityModel
    )
    afterOptionalMatches.cardinality
  }

  private def visitQueryGraph(
    outer: QueryGraph,
    cardinalityAndInput: CardinalityAndInput,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): CardinalityAndInput = {
    cardinalityAndInput.copy(cardinality =
      cardinalityForQueryGraph(
        outer,
        cardinalityAndInput.labelInfo,
        cardinalityAndInput.relTypeInfo,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )
    )
  }

  private def visitOptionalMatchQueryGraphs(
    optionals: Seq[QueryGraph],
    cardinalityAndInput: CardinalityAndInput,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): CardinalityAndInput = {
    optionals.foldLeft(cardinalityAndInput) { case (current, optional) =>
      visitOptionalQueryGraph(optional, current, semanticTable, indexPredicateProviderContext, cardinalityModel)
    }
  }

  private def visitOptionalQueryGraph(
    optional: QueryGraph,
    cardinalityAndInput: CardinalityAndInput,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): CardinalityAndInput = {
    val inputWithKnownLabelInfo = cardinalityAndInput.withFusedLabelInfo(optional.selections.labelInfo)
    val optionalCardinality = cardinalityAndInput.cardinality * cardinalityForQueryGraph(
      optional,
      inputWithKnownLabelInfo.labelInfo,
      inputWithKnownLabelInfo.relTypeInfo,
      semanticTable,
      indexPredicateProviderContext,
      cardinalityModel
    )
    // OPTIONAL MATCH can't decrease cardinality
    inputWithKnownLabelInfo.copy(
      cardinality = Cardinality.max(cardinalityAndInput.cardinality, optionalCardinality)
    )
  }

  private def cardinalityForQueryGraph(
    qg: QueryGraph,
    labelInfo: LabelInfo,
    relTypeInfo: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Cardinality = {
    val patternMultiplier =
      calculateMultiplier(qg, labelInfo, relTypeInfo, semanticTable, indexPredicateProviderContext, cardinalityModel)
    val numberOfPatternNodes = qg.patternNodes.count { n =>
      !qg.argumentIds.contains(n) && !qg.patternRelationships.exists(r =>
        qg.argumentIds.contains(r.name) && Seq(r.left, r.right).contains(n)
      )
    }

    val numberOfGraphNodes = planContext.statistics.nodesAllCardinality()

    (numberOfGraphNodes ^ numberOfPatternNodes) * patternMultiplier
  }

  private def calculateMultiplier(
    qg: QueryGraph,
    labels: LabelInfo,
    relTypes: RelTypeInfo,
    semanticTable: SemanticTable,
    indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext,
    cardinalityModel: CardinalityModel
  ): Multiplier = {
    val expressionSelectivity =
      selectivityCalculator(
        qg.selections,
        labels,
        relTypes,
        semanticTable,
        indexPredicateProviderContext,
        cardinalityModel
      )

    val nodeConnections = qg.nodeConnections.toIndexedSeq
    val patternMultipliers = nodeConnections
      .filter {
        case r: PatternRelationship if qg.argumentIds.contains(r.name) => false
        case _                                                         => true
      }.map(nodeConnectionMultiplierCalculator.nodeConnectionMultiplier(_, labels)(semanticTable, cardinalityModel))

    patternMultipliers.product * expressionSelectivity
  }
}
