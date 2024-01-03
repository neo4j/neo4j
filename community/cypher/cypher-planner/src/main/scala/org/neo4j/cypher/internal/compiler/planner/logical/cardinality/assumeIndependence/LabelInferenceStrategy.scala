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

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.options.LabelInferenceOption
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId

trait LabelInferenceStrategy {

  def inferLabels(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, QueryGraphCardinalityContext)
}

object LabelInferenceStrategy {

  def fromConfig(planContext: PlanContext, labelInferenceOption: LabelInferenceOption): LabelInferenceStrategy = {
    if (labelInferenceOption == LabelInferenceOption.enabled)
      new InferOnlyIfNoOtherLabel(planContext)
    else
      NoInference
  }

  case object NoInference extends LabelInferenceStrategy {

    override def inferLabels(
      context: QueryGraphCardinalityContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, QueryGraphCardinalityContext) = (labelInfo, context)
  }

  private class InferOnlyIfNoOtherLabel(planContext: PlanContext) extends LabelInferenceStrategy {

    def inferLabels(
      context: QueryGraphCardinalityContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, QueryGraphCardinalityContext) = {

      val allInferredLabels = for {
        nodeConnection <- nodeConnections
        simpleRelationship <- SimpleRelationship.fromNodeConnection(nodeConnection, context.semanticTable).iterator
        inferredLabel <- simpleRelationship.inferLabels(planContext)
      } yield inferredLabel

      val inferredLabels = allInferredLabels
        .sequentiallyGroupBy(_.node)
        .map { case (nodeName, inferredLabels) =>
          nodeName -> inferredLabels.minBy(x => context.graphStatistics.nodesWithLabelCardinality(Some(x.labelId)))
        }.toMap

      // Update the semantic table with newly resolved label names.
      val newContext = context.copy(
        semanticTable = context.semanticTable.addResolvedLabelNames(
          inferredLabels.values.map(il => il.labelName -> il.labelId)
        )
      )

      def addInferredLabelOnlyIfNoOtherLabel(labelInfo: LabelInfo): LabelInfo = {
        labelInfo.map {
          case (node, labelNames) if labelNames.isEmpty => // only infer if node has no labels
            node -> Set(inferredLabels.get(node).map(x => LabelName(x.labelName)(InputPosition.NONE))).flatten
          case (nodeName, labelNames) =>
            nodeName -> labelNames
        }
      }

      (addInferredLabelOnlyIfNoOtherLabel(labelInfo), newContext)
    }
  }

  private case class SimpleRelationship(
    startNode: LogicalVariable,
    endNode: LogicalVariable,
    relationshipType: RelTypeId
  ) {

    private def nodesWithSameCardinalityWhenAddingLabel(
      planContext: PlanContext,
      labelId: LabelId
    ): List[LogicalVariable] = {
      val relationshipCardinality = planContext.statistics.patternStepCardinality(None, Some(relationshipType), None)

      def hasSameCardinalityWhenAddingLabel(fromLabel: Option[LabelId], toLabel: Option[LabelId]): Boolean = {
        planContext.statistics.patternStepCardinality(
          fromLabel,
          Some(relationshipType),
          toLabel
        ).amount == relationshipCardinality.amount
      }

      List(
        Option.when(hasSameCardinalityWhenAddingLabel(Some(labelId), None))(startNode),
        Option.when(hasSameCardinalityWhenAddingLabel(None, Some(labelId)))(endNode)
      ).flatten
    }

    def inferLabels(planContext: PlanContext): Seq[SimpleRelationship.InferredLabel] = {
      for {
        mostCommonLabelId <- planContext.statistics.mostCommonLabelGivenRelationshipType(this.relationshipType.id)
        nodeName <- this.nodesWithSameCardinalityWhenAddingLabel(planContext, LabelId(mostCommonLabelId))
        labelName = planContext.getLabelName(mostCommonLabelId)
      } yield SimpleRelationship.InferredLabel(nodeName, labelName, LabelId(mostCommonLabelId))
    }
  }

  private object SimpleRelationship {
    case class InferredLabel(node: LogicalVariable, labelName: String, labelId: LabelId)

    def fromNodeConnection(nodeConnection: NodeConnection, semanticTable: SemanticTable): Option[SimpleRelationship] =
      nodeConnection match {
        case relationship @ PatternRelationship(_, _, dir, Seq(relationshipTypeName), SimplePatternLength)
          if dir == SemanticDirection.OUTGOING || dir == SemanticDirection.INCOMING =>
          val (startNode, endNode) = relationship.inOrder
          semanticTable.id(relationshipTypeName).map(SimpleRelationship(startNode, endNode, _))
        case _ => None
      }
  }

}
