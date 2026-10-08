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
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.RelTypeInfo
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.PatternRelationshipCardinalityModel.extractRelevantLabelInfo
import org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence.PatternRelationshipCardinalityModel.extractRelevantRelTypeInfo
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity

import scala.annotation.tailrec

trait PatternRelationshipCardinalityModel extends NodeCardinalityModel {

  def getRelationshipCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    relationship: PatternRelationship,
    isUnique: Boolean
  ): Cardinality = {
    cachedDoGetRelationshipCardinality(
      context.copy(relTypeInfo = extractRelevantRelTypeInfo(context.relTypeInfo, relationship)),
      extractRelevantLabelInfo(labelInfo, relationship),
      relationship,
      isUnique
    )
  }

  private val cachedDoGetRelationshipCardinality = CachedFunction(doGetRelationshipCardinality _)

  private def doGetRelationshipCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    relationship: PatternRelationship,
    isUnique: Boolean
  ): Cardinality =
    relationship.length match {
      case SimplePatternLength =>
        getSimpleRelationshipCardinality(
          context = context,
          labelInfo = labelInfo,
          leftNode = relationship.left,
          rightNode = relationship.right,
          relationshipTypes = relationship.types,
          relationshipDirection = relationship.dir
        )

      case varPatternLength: VarPatternLength =>
        RepetitionCardinalityModel
          .varPatternLengthAsRange(varPatternLength)
          .view
          .map {
            case 0 =>
              getEmptyPathPatternCardinality(context, labelInfo, relationship.left, relationship.right)
            case 1 => getSimpleRelationshipCardinality(
                context = context,
                labelInfo = labelInfo,
                leftNode = relationship.left,
                rightNode = relationship.right,
                relationshipTypes = relationship.types,
                relationshipDirection = relationship.dir
              )
            case i =>
              if (context.graphStatistics.nodesAllCardinality() > Cardinality.EMPTY) {
                // Prior to this call (in getBaseQueryGraphCardinality()), labels on the start and end nodes are already inferred
                // Here, we want to find the labels that can be inferred on the middle nodes.
                val a = Variable("a")(InputPosition.NONE, isIsolated = false)
                val r = PatternRelationship(
                  Variable("r")(InputPosition.NONE, isIsolated = false),
                  (a, a),
                  relationship.dir,
                  relationship.types,
                  SimplePatternLength
                )
                val (inferredLabels, updatedContext) =
                  context.labelInferenceStrategy.inferLabels(context, Map.empty, List(r))
                val inferredLabelsForMiddleNodes = inferredLabels.getOrElse(a, Set.empty)

                // Goes from the left boundary node to a middle node
                // Use labelInfo, but set the inferred labels of relationship.right (middle node) to inferredLabelsForMiddleNodes
                val firstRelationshipCardinality =
                  getSimpleRelationshipCardinality(
                    context = updatedContext,
                    labelInfo = labelInfo.updated(relationship.right, inferredLabelsForMiddleNodes),
                    leftNode = relationship.left,
                    rightNode = relationship.right,
                    relationshipTypes = relationship.types,
                    relationshipDirection = relationship.dir
                  )

                // Goes from a middle node to a middle node
                // Use labelInfo, but set the inferred labels of relationship.left and relationship.right (middle nodes) to inferredLabelsForMiddleNodes
                val intermediateRelationshipMultiplier =
                  getIntermediateRelationshipMultiplier(
                    context = updatedContext,
                    labelInfo =
                      labelInfo
                        .updated(relationship.left, inferredLabelsForMiddleNodes)
                        .updated(relationship.right, inferredLabelsForMiddleNodes),
                    relationship = relationship
                  )

                // Goes from a middle node to the right boundary node
                // Use labelInfo, but set the inferred labels of relationship.left (middle node) to inferredLabelsForMiddleNodes
                val lastRelationshipMultiplier = {
                  getLastRelationshipMultiplier(
                    updatedContext,
                    labelInfo.updated(relationship.left, inferredLabelsForMiddleNodes),
                    relationship
                  )
                }

                val uniqueness =
                  if (isUnique) {
                    // In a var-length relationship, there's only one relationship being repeated:
                    RepetitionCardinalityModel.relationshipUniquenessSelectivity(
                      // no two different relationships within the pattern unlike in quantified path patterns
                      differentRelationships = 0,
                      // the relationship must be unique across iterations.
                      uniqueRelationships = 1,
                      repetitions = i
                    )
                  } else Selectivity.ONE

                firstRelationshipCardinality *
                  (intermediateRelationshipMultiplier ^ (i - 2)) *
                  lastRelationshipMultiplier *
                  uniqueness
              } else {
                Cardinality.EMPTY
              }
          }.sum(NumericCardinality)
    }

  /**
   * When we know that all relationships with type R end in label A, then we can
   * replace the approximation for [:R]->(:A:B) with the exact value for [:R]->(:B).
   *
   * Thus, when estimating the cardinality of a relationship, we can remove the implied labels
   * from relationship endpoint constraints (e.q., type R implies label A)
   * and node label constraints (e.g., label B implies label A).
   */
  private def reduceLabelInfoByImplied(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    fromNode: LogicalVariable,
    toNode: LogicalVariable,
    relationshipTypes: Seq[RelTypeName],
    relationshipDirection: SemanticDirection
  ): LabelInfo = {
    // First remove labels implied by other node-label constraints (cached inside GraphSchemaOptimizations).
    val prunedByNodeLabelConstraints = context.graphSchemaOptimizations.pruneImpliedLabels(labelInfo)

    // Then remove labels that are implied specifically by relationship-type endpoint constraints.
    // Many relationships will have no such implications; short-circuit that common case to avoid
    // computing and applying the more expensive transformation.
    val implications = context.graphSchemaOptimizations.implicationsFromRelationship(
      fromNode,
      toNode,
      relationshipTypes,
      relationshipDirection
    )

    // If there are no implications from relationship types, return the pruned labels unchanged
    if (implications.isEmpty) prunedByNodeLabelConstraints
    else {
      val variableToRelTypeMap = implications.groupMap(_._1)(_._2)
      prunedByNodeLabelConstraints.map {
        case (variable, knownLabels) =>
          variable -> (knownLabels -- variableToRelTypeMap.getOrElse(variable, Set.empty))
      }
    }
  }

  def getEmptyPathPatternCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    left: LogicalVariable,
    right: LogicalVariable
  ): Cardinality = {
    val labels = for {
      labelsOnLeft <- getResolvedNodeLabels(context, labelInfo, left)
      labelsOnRight <- getResolvedNodeLabels(context, labelInfo, right)
    } yield labelsOnLeft.toList ++ labelsOnRight.toList // adding the labels on the lhs and rhs as lists in order to be consistent with the current implementation, we can now easily change it to set union
    labels.map(getLabelsCardinality(context, _)).getOrElse(Cardinality.EMPTY)
  }

  def getSimpleRelationshipCardinality(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    leftNode: LogicalVariable,
    rightNode: LogicalVariable,
    relationshipTypes: Seq[RelTypeName],
    relationshipDirection: SemanticDirection
  ): Cardinality = {
    val (fromNode, toNode) = relationshipDirection match {
      case SemanticDirection.INCOMING => (rightNode, leftNode)
      case _                          => (leftNode, rightNode)
    }
    val reducedLabelInfo =
      reduceLabelInfoByImplied(context, labelInfo, fromNode, toNode, relationshipTypes, relationshipDirection)
    val cardinality =
      for {
        labelsOnLeft <- getResolvedNodeLabels(context, reducedLabelInfo, leftNode)
        labelsOnRight <- getResolvedNodeLabels(context, reducedLabelInfo, rightNode)
      } yield relationshipTypes match {
        case Seq() =>
          getDissectedRelationshipCardinality(
            context,
            labelsOnLeft,
            labelsOnRight,
            relationshipDirection,
            None
          )
        case relationshipTypes => relationshipTypes.map(relationshipType =>
            getDissectedRelationshipCardinality(
              context,
              labelsOnLeft,
              labelsOnRight,
              relationshipDirection,
              Some(relationshipType)
            )
          ).sum(NumericCardinality)
      }
    cardinality.getOrElse(Cardinality.EMPTY)
  }

  private def getIntermediateRelationshipMultiplier(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    relationship: PatternRelationship
  ): Multiplier = {
    val relCardinality =
      getSimpleRelationshipCardinality(
        context = context,
        labelInfo = labelInfo,
        leftNode = relationship.left,
        rightNode = relationship.right,
        relationshipTypes = relationship.types,
        relationshipDirection = relationship.dir
      )

    val nodeCardinality =
      getNodeCardinality(context, labelInfo, relationship.left)
        .getOrElse(context.graphStatistics.nodesAllCardinality())

    Multiplier.ofDivision(relCardinality, nodeCardinality)
      .getOrElse(Multiplier.ZERO)
  }

  private def getLastRelationshipMultiplier(
    context: QueryGraphCardinalityContext,
    labelInfo: LabelInfo,
    relationship: PatternRelationship
  ): Multiplier = {
    val relCardinality =
      getSimpleRelationshipCardinality(
        context = context,
        labelInfo = labelInfo,
        leftNode = relationship.left,
        rightNode = relationship.right,
        relationshipTypes = relationship.types,
        relationshipDirection = relationship.dir
      )

    val nodeCardinality =
      getNodeCardinality(
        context,
        labelInfo,
        relationship.left
      )
        .getOrElse(context.graphStatistics.nodesAllCardinality())

    Multiplier.ofDivision(relCardinality, nodeCardinality)
      .getOrElse(Multiplier.ZERO)
  }

  private def getDissectedRelationshipCardinality(
    context: QueryGraphCardinalityContext,
    labelsOnLeft: Set[LabelId],
    labelsOnRight: Set[LabelId],
    dir: SemanticDirection,
    relationshipType: Option[RelTypeName]
  ): Cardinality = {
    val cardinalities =
      for {
        (labelOnLeft, otherLabelsOnLeft) <- endpointLabelsCombinations(labelsOnLeft)
        (labelOnRight, otherLabelsOnRight) <- endpointLabelsCombinations(labelsOnRight)
      } yield {
        lazy val outgoing = patternStepCardinality(context, labelOnLeft, relationshipType, labelOnRight)
        lazy val incoming = patternStepCardinality(context, labelOnRight, relationshipType, labelOnLeft)
        val relationshipCardinality = dir match {
          case SemanticDirection.OUTGOING => outgoing
          case SemanticDirection.INCOMING => incoming
          case SemanticDirection.BOTH     => outgoing + incoming
        }
        val otherLabels = otherLabelsOnLeft ++ otherLabelsOnRight
        val otherLabelsSelectivity =
          context.combiner.andTogetherSelectivities(otherLabels.flatMap(getLabelSelectivity(context, _)))
        otherLabelsSelectivity.fold(relationshipCardinality)(relationshipCardinality * _)
      }
    cardinalities.min
  }

  private def endpointLabelsCombinations(
    labels: Set[LabelId]
  ): Vector[(Option[LabelId], Vector[LabelId])] =
    if (labels.isEmpty)
      Vector((None, Vector.empty))
    else
      buildEndpointLabelsCombinations(labels.toVector, Vector.empty, Vector.empty)

  @tailrec
  private def buildEndpointLabelsCombinations(
    labels: Vector[LabelId],
    previousLabels: Vector[LabelId],
    combinations: Vector[(Option[LabelId], Vector[LabelId])]
  ): Vector[(Option[LabelId], Vector[LabelId])] =
    if (labels.isEmpty)
      combinations
    else {
      val head = labels.head
      val tail = labels.tail
      val combination = (Some(head), previousLabels.appendedAll(tail))
      buildEndpointLabelsCombinations(tail, previousLabels.prepended(head), combinations.appended(combination))
    }

  private def patternStepCardinality(
    context: QueryGraphCardinalityContext,
    labelOnLeft: Option[LabelId],
    relationshipType: Option[RelTypeName],
    labelOnRight: Option[LabelId]
  ): Cardinality =
    relationshipType match {
      case None => context.graphStatistics.patternStepCardinality(labelOnLeft, None, labelOnRight)
      case Some(relationshipTypeName) => context.semanticTable.id(relationshipTypeName) match {
          case None => MinimumGraphStatistics.MIN_PATTERN_STEP_CARDINALITY
          case Some(relationshipTypeId) =>
            context.graphStatistics.patternStepCardinality(labelOnLeft, Some(relationshipTypeId), labelOnRight)
        }
    }
}

object PatternRelationshipCardinalityModel {

  def extractRelevantLabelInfo(labelInfo: LabelInfo, nodeConnection: NodeConnection): LabelInfo = {
    labelInfo.view.filterKeys(nodeConnection.nodes.contains).toMap
  }

  def extractRelevantRelTypeInfo(relTypeInfo: RelTypeInfo, nodeConnection: NodeConnection): RelTypeInfo = {
    relTypeInfo.view.filterKeys(nodeConnection.relationships.contains).toMap
  }
}
