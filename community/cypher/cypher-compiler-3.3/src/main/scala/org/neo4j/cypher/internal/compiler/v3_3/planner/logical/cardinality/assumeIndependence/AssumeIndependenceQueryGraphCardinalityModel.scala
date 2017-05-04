/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics.{QueryGraphCardinalityModel, QueryGraphSolverInput}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.cardinality.{ExpressionSelectivityCalculator, SelectivityCombiner}
import org.neo4j.cypher.internal.compiler.v3_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v3_3.SemanticTable
import org.neo4j.cypher.internal.frontend.v3_3.ast.LabelName
import org.neo4j.cypher.internal.ir.v3_2.{QueryGraph, _}

case class AssumeIndependenceQueryGraphCardinalityModel(stats: GraphStatistics, combiner: SelectivityCombiner)
  extends QueryGraphCardinalityModel {
  import AssumeIndependenceQueryGraphCardinalityModel.MAX_OPTIONAL_MATCH

  private val expressionSelectivityEstimator = ExpressionSelectivityCalculator(stats, combiner)
  private val patternSelectivityEstimator = PatternSelectivityCalculator(stats, combiner)

  /**
   * When there are optional matches, the cardinality is always the maximum of any matches that exist,
   * because no matches are limiting. So in principle we need to calculate cardinality of all possible combinations
   * of matches, and then take the max but since the number of combinations grow exponentially we cap it at a threshold.
   */
  def apply(queryGraph: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable): Cardinality = {
    val combinations: Seq[QueryGraph] = findQueryGraphCombinations(queryGraph, input, semanticTable)

    val cardinalities = combinations.map(cardinalityForQueryGraph(_, input)(semanticTable))
    cardinalities.max
  }

  private def findQueryGraphCombinations(queryGraph: QueryGraph, input: QueryGraphSolverInput, semanticTable: SemanticTable): Seq[QueryGraph] =
    if (queryGraph.optionalMatches.isEmpty)
      Seq(queryGraph)
    else {
      //the number of combinations we need to consider grows exponentially, so above a threshold we only consider all
      //combinations of the most expensive query graphs
      val optionalMatches =
        if (queryGraph.optionalMatches.size <= MAX_OPTIONAL_MATCH) queryGraph.optionalMatches
        else queryGraph.optionalMatches.sortBy(-cardinalityForQueryGraph(_, input)(semanticTable).amount).take(MAX_OPTIONAL_MATCH)

      (0 to optionalMatches.length).flatMap(optionalMatches.combinations)
        .map(_.map(_.withoutArguments()))
        .map(_.foldLeft(QueryGraph.empty)(_.withOptionalMatches(Vector.empty) ++ _.withOptionalMatches(Vector.empty)))
        .map(queryGraph.withOptionalMatches(Vector.empty) ++ _)
    }

  private def calculateNumberOfPatternNodes(qg: QueryGraph) = {
    val intermediateNodes = qg.patternRelationships.map(_.length match {
      case SimplePatternLength            => 0
      case VarPatternLength(_, optMax)    => Math.max(Math.min(optMax.getOrElse(PatternSelectivityCalculator.MAX_VAR_LENGTH), PatternSelectivityCalculator.MAX_VAR_LENGTH) - 1, 0)
    }).sum

    qg.patternNodes.count(!qg.argumentIds.contains(_)) + intermediateNodes
  }

  private def cardinalityForQueryGraph(qg: QueryGraph, input: QueryGraphSolverInput)
                                      (implicit semanticTable: SemanticTable): Cardinality = {
    val (selectivity, numberOfZeroZeroRels) = calculateSelectivity(qg, input.labelInfo)
    val numberOfPatternNodes = calculateNumberOfPatternNodes(qg) - numberOfZeroZeroRels
    val numberOfGraphNodes = stats.nodesWithLabelCardinality(None)

    val c = if (qg.argumentIds.nonEmpty) {
      if ((qg.argumentIds intersect qg.patternNodes).isEmpty) {
        /*
       * If have a node pattern and we have arguments the produced cardinality is at least
       * the one produce of the node pattern solved
       */
        Cardinality.max(Cardinality(1.0), input.inboundCardinality)
      } else
        input.inboundCardinality

    } else
      Cardinality(1.0)

    c * (numberOfGraphNodes ^ numberOfPatternNodes) * selectivity
  }

  private def calculateSelectivity(qg: QueryGraph, labels: Map[IdName, Set[LabelName]])
                                  (implicit semanticTable: SemanticTable): (Selectivity, Int) = {
    implicit val selections = qg.selections

    val expressionSelectivities = selections.flatPredicates.map(expressionSelectivityEstimator(_))

    val patternSelectivities = qg.patternRelationships.toIndexedSeq.map {
      /* This is here to handle the *0..0 case.
         Our solution to the problem is to keep count of how many of these we see, and decrease the number of pattern
         nodes accordingly. The nice solution would have been to rewrite these relationships away at an earlier phase.
         This workaround should work, but might not give the best numbers.
       */
      case r if r.length == VarPatternLength(0, Some(0)) => None
      case r => Some(patternSelectivityEstimator(r, labels))
    }

    val numberOfZeroZeroRels = patternSelectivities.count(_.isEmpty)

    val selectivity = combiner.andTogetherSelectivities(expressionSelectivities ++ patternSelectivities.flatten)

    (selectivity.getOrElse(Selectivity.ONE), numberOfZeroZeroRels)
  }
}

object AssumeIndependenceQueryGraphCardinalityModel {
  //Since the number of combinations of optional matches grows exponentially and the reward of considering
  //more combination diminishes with the number of combinations, we cap it at this threshold.
  //The value chosen is mostly arbitrary but having it at 8 means 256 combinations which is still reasonably fast.
  private val MAX_OPTIONAL_MATCH = 8
}
