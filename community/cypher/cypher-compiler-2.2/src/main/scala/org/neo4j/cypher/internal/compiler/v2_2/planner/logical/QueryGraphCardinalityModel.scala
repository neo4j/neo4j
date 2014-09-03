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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.LabelId
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.spi.{GraphStatistics, TokenContext}

case class QueryGraphCardinalityModel(statistics: GraphStatistics,
                                      selectivity: Metrics.SelectivityModel,
                                      tokenLookups: TokenContext) {

  def apply(queryGraph: QueryGraph): Cardinality = {
    val cardinalityPerNode: Seq[Cardinality] =
      queryGraph.patternNodes.toSeq.map(cardinalityForNodeByLabel(queryGraph))

    val nodeCardinality = cardinalityPerNode.foldLeft(Cardinality(1)) {
      case (acc, l) => acc * l
    }

    nodeCardinality
  }

  private def cardinalityForNodeByLabel(in: QueryGraph)(nodeId: IdName): Cardinality = {
    val labels: Set[LabelName] = labelsOnNode(in, nodeId)

    if (labels.isEmpty)
      statistics.nodesCardinality
    else
      cardinalityFor(labels)
  }

  private def labelsOnNode(queryGraph: QueryGraph, nodeId: IdName): Set[LabelName] =
    queryGraph.
      selections.
      labelPredicates.getOrElse(nodeId, Set.empty).
      flatMap(_.labels)

  private def accumulatedCardinality(labelStats: Set[Option[Cardinality]]): Cardinality =
    labelStats.reduce[Option[Cardinality]] {
      case (Some(a), Some(b)) if a <= b => Some(a)
      case (Some(a), Some(b)) if a >= b => Some(b)
      case _ => Some(Cardinality(0)) /*Label not present in store*/
    }.getOrElse(Cardinality(0))

  private def cardinalityFor(labels: Set[LabelName]): Cardinality = {
    val maybeCardinalities = labels.
      map(label => tokenLookups.getOptLabelId(label.name).map(LabelId.apply)).
      map(labelId => labelId.map(statistics.nodesWithLabelCardinality))
    maybeCardinalities

    accumulatedCardinality(maybeCardinalities)
  }
}
