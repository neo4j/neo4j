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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{QueryGraph, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

case class AssumeIndependenceQueryGraphCardinalityModel(stats: GraphStatistics,
                                                        semanticTable: SemanticTable,
                                                        combiner: SelectivityCombiner)
  extends QueryGraphCardinalityModel  {

  private val expressionSelectivityEstimator = ExpressionSelectivityCalculator(stats, combiner)
  private val patternSelectivityEstimator = PatternSelectivityCalculator(stats, combiner)

  def apply(queryGraph: QueryGraph): Cardinality = {
    findQueryGraphCombinations(queryGraph, semanticTable)
      .map(cardinalityForQueryGraph(_)(semanticTable))
      .foldLeft(Cardinality(0))((acc, curr) => if (curr > acc) curr else acc)
  }

  /**
   * Finds all combinations of a querygraph with its optional matches,
   * e.g. Given QueryGraph QG with optional matches [opt1, opt2, opt3] we get
   * [QG, QG ++ opt1, QG ++ opt2, QG ++ opt3, QG ++ opt1 ++ opt2, QG ++ opt2 ++ opt3, QG ++ opt1 ++ opt2 ++ opt3]
   */
  private def findQueryGraphCombinations(queryGraph: QueryGraph, semanticTable: SemanticTable): Seq[QueryGraph] = {
    (0 to queryGraph.optionalMatches.length)
      .map(queryGraph.optionalMatches.combinations)
      .flatten
      .map(_.foldLeft(QueryGraph.empty)(_.withOptionalMatches(Seq.empty) ++ _.withOptionalMatches(Seq.empty)))
      .map(queryGraph.withOptionalMatches(Seq.empty) ++ _)
  }

  private def cardinalityForQueryGraph(qg: QueryGraph)(implicit semanticTable: SemanticTable): Cardinality = {
    val selectivity = calculateSelectivity(qg)
    val numberOfPatternNodes = qg.patternNodes.size
    val numberOfGraphNodes = stats.nodesWithLabelCardinality(None)

    (numberOfGraphNodes ^ numberOfPatternNodes) * selectivity
  }

  private def calculateSelectivity(qg: QueryGraph)(implicit semanticTable: SemanticTable) = {
    implicit val selections = qg.selections

    val expressionSelectivities = selections.flatPredicates.map(expressionSelectivityEstimator(_))
    val patternSelectivities = qg.patternRelationships.toSeq.map(patternSelectivityEstimator(_))


    val selectivity: Option[Selectivity] = combiner.andTogetherSelectivities(expressionSelectivities ++ patternSelectivities)

    selectivity.
      getOrElse(Selectivity(1))
  }
}

trait SelectivityCombiner {
  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity]

  // A ∪ B = ¬ ( ¬ A ∩ ¬ B )
  def orTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] = {
    val inverses = selectivities.map(_.negate)
    andTogetherSelectivities(inverses).
      map(_.negate)
  }
}

case object IndependenceCombiner extends SelectivityCombiner {
  // This is the simple and straight forward way of combining two statistically independent probabilities
  //P(A ∪ B) = P(A) * P(B)
  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] =
    selectivities.reduceOption(_ * _)
}

// The estimate is computed the most selective predicate multiplied by the table cardinality, multiplied by the
// square root of the next most selective predicate, and so on with each new selectivity gaining an additional
// square root.
// Recalling that selectivity is a number between 0 and 1, it is clear that applying a square root moves the number
// closer to 1. The effect is to take account of all predicates in the final estimate, but to reduce the impact of
// the less selective predicates exponentially.
// For the ones that need visual aids to grokk it: http://i.imgur.com/V4Fs7AC.png
case object ExponentialBackOff extends SelectivityCombiner {
  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] =
    if (selectivities.isEmpty)
      None
    else {
      val newSelectivity = (selectivities.sorted zipWithIndex).foldLeft(1.0) {
        // P(A ∪ B ∪ C) = P(A) * SQRT(P(B)) * SQRT(SQRT(P(C)))
        // This is encoded using the fact that SQRT(x) is equal to x to the power of 1/2.
        case (acc, (sel, idx)) => acc * Math.pow(sel.factor, 1.0 / Math.pow(2, idx))
      }

      Some(Selectivity(newSelectivity))
    }
}
