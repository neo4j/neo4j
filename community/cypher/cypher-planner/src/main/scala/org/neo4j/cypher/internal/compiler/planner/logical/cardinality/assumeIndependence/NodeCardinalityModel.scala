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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Selectivity

import scala.annotation.tailrec

trait NodeCardinalityModel {

  def getNodeCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    node: LogicalVariable
  ): Option[Cardinality] =
    getResolvedNodeLabels(context, labelInfo, node).map(getLabelsCardinality(context, _))

  def getArgumentSelectivity(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    argumentId: LogicalVariable
  ): Selectivity = {
    val selectivities =
      labelInfo
        .getOrElse(argumentId, Set.empty)
        .toList
        .flatMap(context.semanticTable.id)
        .flatMap(getLabelSelectivity(context, _))
    context.combiner.andTogetherSelectivities(selectivities).getOrElse(Selectivity.ONE)
  }

  def getResolvedNodeLabels(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    node: LogicalVariable
  ): Option[Set[LabelId]] =
    resolveNodeLabels(context, labelInfo.getOrElse(node, Set.empty))

  def resolveNodeLabels(
    context: QueryGraphCardinalityContext,
    labelNames: Set[LabelName]
  ): Option[Set[LabelId]] = resolveNodeLabelsRec(context, labelNames, Set.empty)

  @tailrec
  private def resolveNodeLabelsRec(
    context: QueryGraphCardinalityContext,
    labelNames: Set[LabelName],
    labelIds: Set[LabelId]
  ): Option[Set[LabelId]] =
    if (labelNames.isEmpty)
      Some(labelIds)
    else
      context.semanticTable.id(labelNames.head) match {
        case None          => None
        case Some(labelId) => resolveNodeLabelsRec(context, labelNames.tail, labelIds.incl(labelId))
      }

  def getLabelsCardinality(context: QueryGraphCardinalityContext, labels: Iterable[LabelId]): Cardinality =
    if (labels.isEmpty)
      context.allNodesCardinality
    else {
      val firstLabelCardinality = context.graphStatistics.nodesWithLabelCardinality(Some(labels.head))
      context.combiner.andTogetherSelectivities(labels.tail.flatMap(getLabelSelectivity(context, _))) match {
        case Some(otherLabelsSelectivity) => firstLabelCardinality * otherLabelsSelectivity
        case None                         => firstLabelCardinality
      }
    }

  def getLabelSelectivity(context: QueryGraphCardinalityContext, labelId: LabelId): Option[Selectivity] =
    context.graphStatistics.nodesWithLabelCardinality(Some(labelId)) / context.allNodesCardinality
}
