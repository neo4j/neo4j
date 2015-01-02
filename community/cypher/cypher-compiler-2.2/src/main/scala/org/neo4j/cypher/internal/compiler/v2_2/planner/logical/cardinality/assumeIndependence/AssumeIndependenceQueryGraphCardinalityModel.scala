/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v2_2.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.{QueryGraphCardinalityInput, QueryGraphCardinalityModel}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.{ExpressionSelectivityCalculator, SelectivityCombiner}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{VarPatternLength, SimplePatternLength, IdName}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

case class AssumeIndependenceQueryGraphCardinalityModel(stats: GraphStatistics,
                                                        semanticTable: SemanticTable,
                                                        combiner: SelectivityCombiner)
  extends QueryGraphCardinalityModel  {

  private val expressionSelectivityEstimator = ExpressionSelectivityCalculator(stats, combiner)
  private val patternSelectivityEstimator = PatternSelectivityCalculator(stats, combiner)

  /**
   * When there are optional matches, the cardinality is always the maximum of any matches that exist,
   * because no matches are limiting. So we need to calculate cardinality of all possible combinations
   * of matches, and then take the max.
   */
  def apply(queryGraph: QueryGraph, input: QueryGraphCardinalityInput): Cardinality = {
    val combinations: Seq[QueryGraph] = findQueryGraphCombinations(queryGraph, semanticTable)
    val cardinalities = combinations.map(cardinalityForQueryGraph(_, input)(semanticTable))
    cardinalities.max
  }

  private def findQueryGraphCombinations(queryGraph: QueryGraph, semanticTable: SemanticTable): Seq[QueryGraph] = {
    (0 to queryGraph.optionalMatches.length)
      .map(queryGraph.optionalMatches.combinations)
      .flatten
      .map(_.map(_.withoutArguments()))
      .map(_.foldLeft(QueryGraph.empty)(_.withOptionalMatches(Seq.empty) ++ _.withOptionalMatches(Seq.empty)))
      .map(queryGraph.withOptionalMatches(Seq.empty) ++ _)
  }

  private def calculcateNumberOfPatternNodes(qg: QueryGraph) = {
    val intermediateNodes = qg.patternRelationships.map(_.length match {
      case SimplePatternLength            => 0
      case VarPatternLength(_, optMax)    => Math.max(Math.min(optMax.getOrElse(PatternSelectivityCalculator.MAX_VAR_LENGTH), PatternSelectivityCalculator.MAX_VAR_LENGTH) - 1, 0)
    }).sum

    qg.patternNodes.count(!qg.argumentIds.contains(_)) + intermediateNodes
  }

  private def cardinalityForQueryGraph(qg: QueryGraph, input: QueryGraphCardinalityInput)
                                      (implicit semanticTable: SemanticTable): Cardinality = {
    val selectivity = calculateSelectivity(qg, input.labelInfo)
    val numberOfPatternNodes = calculcateNumberOfPatternNodes(qg)
    val numberOfGraphNodes = stats.nodesWithLabelCardinality(None)

    val c = if (qg.argumentIds.nonEmpty)
      input.inboundCardinality
    else
      Cardinality(1)

    c * (numberOfGraphNodes ^ numberOfPatternNodes) * selectivity
  }

  private def calculateSelectivity(qg: QueryGraph, labels: Map[IdName, Set[LabelName]])
                                  (implicit semanticTable: SemanticTable) = {
    implicit val selections = qg.selections

    val expressionSelectivities = selections.flatPredicates.map(expressionSelectivityEstimator(_))
    val patternSelectivities = qg.patternRelationships.toSeq.map(patternSelectivityEstimator(_, labels))

    val selectivity = combiner.andTogetherSelectivities(expressionSelectivities ++ patternSelectivities)

    selectivity.
      getOrElse(Selectivity(1))
  }
}






