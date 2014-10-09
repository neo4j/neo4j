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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeDependence

import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Metrics.QueryGraphCardinalityModel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_2.spi.GraphStatistics

case class AssumeDependenceQueryGraphCardinalityModel(statistics: GraphStatistics,
                                                      predicateProducer: QueryGraph => Set[Predicate],
                                                      predicateGrouper: Set[Predicate] => Set[(PredicateCombination, Selectivity)],
                                                      selectivityCombiner: Set[(PredicateCombination, Selectivity)] => (Set[Predicate], Selectivity))
  extends QueryGraphCardinalityModel {

  def apply(queryGraph: QueryGraph): Cardinality = {
    findQueryGraphCombinations(queryGraph)
      .map(cardinalityForQueryGraph)
      .foldLeft(Cardinality(0))((acc, curr) => if (curr > acc) curr else acc)
  }

  /**
   * Finds all combinations of a querygraph with its optional matches,
   * e.g. Given QueryGraph QG with optional matches [opt1, opt2, opt3] we get
   * [QG, QG ++ opt1, QG ++ opt2, QG ++ opt3, QG ++ opt1 ++ opt2, QG ++ opt2 ++ opt3, QG ++ opt1 ++ opt2 ++ opt3]
   */
  private def findQueryGraphCombinations(queryGraph: QueryGraph) = {
    (0 to queryGraph.optionalMatches.length)
      .map(queryGraph.optionalMatches.combinations)
      .flatten
      .map(_.foldLeft(QueryGraph.empty)(_.withOptionalMatches(Seq.empty) ++ _.withOptionalMatches(Seq.empty)))
      .map(queryGraph.withOptionalMatches(Seq.empty) ++ _)
  }

  private def cardinalityForQueryGraph(queryGraph: QueryGraph): Cardinality = {
    val (predicates, selectivity) = estimateSelectivity(queryGraph)
    val numberOfPatternNodes = predicates.flatMap(_.dependencies).count(queryGraph.patternNodes.contains)
    val numberOfGraphNodes = statistics.nodesWithLabelCardinality(None)

    (numberOfGraphNodes ^ numberOfPatternNodes) * selectivity
  }

  private def estimateSelectivity(queryGraph: QueryGraph): (Set[Predicate], Selectivity) = {
    val predicates = predicateProducer(queryGraph)
    val predicatesWithSelectivity = predicateGrouper(predicates)
    selectivityCombiner(predicatesWithSelectivity)
  }
}
