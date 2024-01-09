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
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.Multiplier
import org.neo4j.cypher.internal.util.Selectivity

import scala.annotation.tailrec
import scala.util.chaining.scalaUtilChainingOps

trait PatternRelationshipCardinalityModel extends NodeCardinalityModel {

  def getRelationshipCardinality(
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
              if (context.allNodesCardinality > Cardinality.EMPTY) {

                val inferLabels: LabelInfo => (LabelInfo, QueryGraphCardinalityContext) =
                  context.labelInferenceStrategy.inferLabels(
                    context,
                    _,
                    Seq(relationship.copy(length = SimplePatternLength))
                  )

                val firstRelationshipCardinality =
                  inferLabels(labelInfo.updated(relationship.right, Set.empty)) pipe {
                    case (labelInfo, context) =>
                      getSimpleRelationshipCardinality(
                        context = context,
                        labelInfo = labelInfo,
                        leftNode = relationship.left,
                        rightNode = relationship.right,
                        relationshipTypes = relationship.types,
                        relationshipDirection = relationship.dir
                      )
                  }

                val intermediateRelationshipMultiplier =
                  inferLabels(LabelInfo(relationship.left -> Set.empty, relationship.right -> Set.empty)) pipe {
                    case (labelInfo, context) =>
                      getIntermediateRelationshipMultiplier(
                        context = context,
                        labelInfo = labelInfo,
                        relationship = relationship
                      )
                  }

                val lastRelationshipMultiplier = {
                  val labelInfoForEndPoints = inferLabels(labelInfo.updated(relationship.left, Set.empty))

                  // we either divide by allNodesCardinality to calculate the multiplier,
                  // or by inferred most-common label
                  val labelInfoForNodeCardinality = inferLabels(LabelInfo(relationship.right -> Set.empty))

                  getLastRelationshipMultiplier(
                    labelInfoAndContextForEndPoints = labelInfoForEndPoints,
                    labelInfoAndContextForNodeCardinality = labelInfoForNodeCardinality,
                    relationship = relationship
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
    val cardinality =
      for {
        labelsOnLeft <- getResolvedNodeLabels(context, labelInfo, leftNode)
        labelsOnRight <- getResolvedNodeLabels(context, labelInfo, rightNode)
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
      getNodeCardinality(context, labelInfo, relationship.right)
        .getOrElse(context.allNodesCardinality)

    Multiplier.ofDivision(relCardinality, nodeCardinality)
      .getOrElse(Multiplier.ZERO)
  }

  private def getLastRelationshipMultiplier(
    labelInfoAndContextForEndPoints: (LabelInfo, QueryGraphCardinalityContext),
    labelInfoAndContextForNodeCardinality: (LabelInfo, QueryGraphCardinalityContext),
    relationship: PatternRelationship
  ): Multiplier = {
    val relCardinality =
      getSimpleRelationshipCardinality(
        context = labelInfoAndContextForEndPoints._2,
        labelInfo = labelInfoAndContextForEndPoints._1,
        leftNode = relationship.left,
        rightNode = relationship.right,
        relationshipTypes = relationship.types,
        relationshipDirection = relationship.dir
      )

    val nodeCardinality =
      getNodeCardinality(
        labelInfoAndContextForNodeCardinality._2,
        labelInfoAndContextForNodeCardinality._1,
        relationship.right
      )
        .getOrElse(labelInfoAndContextForNodeCardinality._2.allNodesCardinality)

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
