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

case class AssumeIndependenceQueryGraphCardinalityModel(stats: GraphStatistics, semanticTable: SemanticTable)
  extends QueryGraphCardinalityModel with SelectivityCombiner {

  private val expressionSelectivityEstimator = ExpressionSelectivityCalculator(stats)
  private val patternSelectivityEstimator = PatternSelectivityCalculator(stats)

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


    val selectivity: Option[Selectivity] = andTogetherSelectivities(expressionSelectivities ++ patternSelectivities)

    selectivity.
      getOrElse(Selectivity(1))
  }
}

trait SelectivityCombiner {
  def andTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] =
    selectivities.reduceOption(_ * _)


  // P ∪ Q = ¬ ( ¬ P ∩ ¬ Q )
  def orTogetherSelectivities(selectivities: Seq[Selectivity]): Option[Selectivity] = {
    val inverses = selectivities.map(_.negate)
    andTogetherSelectivities(inverses).
      map(_.negate)
  }
}
