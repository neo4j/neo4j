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
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.LabelInfo
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.options.CypherInferSchemaPartsOption
import org.neo4j.cypher.internal.planner.spi.GraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.RelTypeId

trait LabelInferenceStrategy {

  def inferLabels(
    semanticTable: SemanticTable,
    graphStatistics: GraphStatistics,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, SemanticTable)

  def inferLabels(
    context: LogicalPlanningContext,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, LogicalPlanningContext)

  def inferLabels(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    nodeConnections: Seq[NodeConnection]
  ): (LabelInfo, QueryGraphCardinalityContext)
}

object LabelInferenceStrategy {

  def fromConfig(
    planContext: PlanContext,
    labelInferenceOption: CypherInferSchemaPartsOption
  ): LabelInferenceStrategy = {
    if (labelInferenceOption == CypherInferSchemaPartsOption.mostSelectiveLabel)
      new InferOnlyIfNoOtherLabel(planContext)
    else
      NoInference
  }

  case object NoInference extends LabelInferenceStrategy {

    override def inferLabels(
      semanticTable: SemanticTable,
      graphStatistics: GraphStatistics,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, SemanticTable) = {
      (labelInfo, semanticTable)
    }

    override def inferLabels(
      context: LogicalPlanningContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, LogicalPlanningContext) = {
      (labelInfo, context)
    }

    override def inferLabels(
      context: QueryGraphCardinalityContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, QueryGraphCardinalityContext) = (labelInfo, context)
  }

  private class InferOnlyIfNoOtherLabel(planContext: PlanContext) extends LabelInferenceStrategy {

    private val cachedMostCommonLabelGivenRelationshipType: RelTypeId => Seq[Int] = CachedFunction {
      relTypeId => planContext.statistics.mostCommonLabelGivenRelationshipType(relTypeId.id)
    }

    private def inferLabelsForNodeConnection(
      nodeConnection: NodeConnection,
      semanticTable: SemanticTable
    ): Seq[SimpleRelationship.InferredLabel] = {
      val simpleRelationships = SimpleRelationship.fromNodeConnection(nodeConnection, semanticTable)
      simpleRelationships.size match {
        case 0 => Seq.empty
        case 1 => simpleRelationships.head.inferLabels(planContext, cachedMostCommonLabelGivenRelationshipType)
        case _ =>
          // Populate intersectionInferredLabels with the inferred labels from the first simpleRelationship
          var intersectionInferredLabels =
            simpleRelationships.head.inferLabels(planContext, cachedMostCommonLabelGivenRelationshipType)
          // Continue with the other simpleRelationships
          // Stop when the intersection is empty, or when all simpleRelationships have been processed
          (1 until simpleRelationships.size).foreach(i => {
            if (intersectionInferredLabels.nonEmpty) {
              val inferredLabelsForSimpleRelationship =
                simpleRelationships(i).inferLabels(planContext, cachedMostCommonLabelGivenRelationshipType)
              intersectionInferredLabels = intersectionInferredLabels intersect inferredLabelsForSimpleRelationship
            }
          })
          intersectionInferredLabels
      }
    }

    def inferLabels(
      semanticTable: SemanticTable,
      graphStatistics: GraphStatistics,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, SemanticTable) = {

      val allInferredLabels = nodeConnections.flatMap(inferLabelsForNodeConnection(_, semanticTable))

      val inferredLabels: Seq[SimpleRelationship.InferredLabel] = allInferredLabels
        .sequentiallyGroupBy(_.node)
        .map { case (_, inferredLabels) =>
          inferredLabels.minBy(x => graphStatistics.nodesWithLabelCardinality(Some(x.labelId)))
        }

      // Update the semantic table with newly resolved label names.
      val updatedSemanticTable = semanticTable.addResolvedLabelNames(
        inferredLabels.map(il => il.labelName -> il.labelId)
      )

      def addInferredLabelOnlyIfNoOtherLabel(labelInfo: LabelInfo): LabelInfo = {
        inferredLabels.foldLeft(labelInfo) {
          case (labelInfo, inferredLabel) =>
            labelInfo.updatedWith(inferredLabel.node) {
              case x @ Some(labels) if labels.nonEmpty => x
              case _ =>
                val label = LabelName(inferredLabel.labelName)(InputPosition.NONE)
                Some(Set(label))
            }
        }
      }

      (addInferredLabelOnlyIfNoOtherLabel(labelInfo), updatedSemanticTable)
    }

    def inferLabels(
      context: QueryGraphCardinalityContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, QueryGraphCardinalityContext) = {
      val (updatedLabelInfo, updatedSemanticTable) =
        inferLabels(context.semanticTable, context.graphStatistics, labelInfo, nodeConnections)
      (updatedLabelInfo, context.copy(semanticTable = updatedSemanticTable))
    }

    def inferLabels(
      context: LogicalPlanningContext,
      labelInfo: LabelInfo,
      nodeConnections: Seq[NodeConnection]
    ): (LabelInfo, LogicalPlanningContext) = {
      val (updatedLabelInfo, updatedSemanticTable) =
        inferLabels(context.semanticTable, context.statistics, labelInfo, nodeConnections)
      (updatedLabelInfo, context.withUpdatedSemanticTable(updatedSemanticTable))
    }
  }

  private case class SimpleRelationship(
    startNode: LogicalVariable,
    endNode: LogicalVariable,
    relationshipType: RelTypeId,
    isDirected: Boolean
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

    def inferLabels(
      planContext: PlanContext,
      mostCommonLabelGivenRelationshipType: RelTypeId => Seq[Int]
    ): Seq[SimpleRelationship.InferredLabel] = {
      for {
        mostCommonLabelId <- mostCommonLabelGivenRelationshipType(this.relationshipType)

        // Get the nodes where adding the label does not restrict the cardinality
        // (this could be the start node, end node, both or none)
        nodesWithSameCardinality = this.nodesWithSameCardinalityWhenAddingLabel(planContext, LabelId(mostCommonLabelId))

        // For undirected relationships: both start and end nodes should infer the label,
        // otherwise nothing can be inferred w.r.t. this relationship type and 'mostCommonLabelId'
        if isDirected || nodesWithSameCardinality.size == 2

        labelName = planContext.getLabelName(mostCommonLabelId)
        // We can infer the label 'labelName' (id: 'mostCommonLabelId') for all nodes in 'nodesWithSameCardinality'
        nodeName <- nodesWithSameCardinality
      } yield SimpleRelationship.InferredLabel(
        nodeName,
        labelName,
        LabelId(mostCommonLabelId)
      )
    }
  }

  private object SimpleRelationship {
    case class InferredLabel(node: LogicalVariable, labelName: String, labelId: LabelId)

    /**
     * Create simple relationships from a nodeConnection
     * A simple relationship consists of a start node, end node and one type
     * In case the nodeConnection is a PatternRelationship with a disjunction of multiple types, i.e. ()-[:R1|R2|R3]->(),
     * then a single simple relationship will be created for each disjunction: [()-[:R1]->(), ()-[:R2]->(), ()-[:R3]->()]
     * @param nodeConnection the node connection to create simple relationships from
     * @param semanticTable using to obtain the relationship type id from the relationship type name
     * @return a sequence of simple relationships obtained from the nodeConnection
     */
    def fromNodeConnection(nodeConnection: NodeConnection, semanticTable: SemanticTable): Seq[SimpleRelationship] =
      nodeConnection match {
        case relationship @ PatternRelationship(_, _, dir, relationshipTypeNames, _) =>
          // relationshipTypeNames contains possibly a disjunction of multiple relationship types
          val (startNode, endNode) = relationship.inOrder
          relationshipTypeNames
            .flatMap(semanticTable.id)
            .map(SimpleRelationship(
              startNode,
              endNode,
              _,
              dir == SemanticDirection.OUTGOING || dir == SemanticDirection.INCOMING
            ))
        case _ => Seq.empty
      }
  }

}
