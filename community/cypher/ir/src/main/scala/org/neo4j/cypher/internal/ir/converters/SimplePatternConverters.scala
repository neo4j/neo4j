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
package org.neo4j.cypher.internal.ir.converters

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern
import org.neo4j.cypher.internal.ir.PatternLength
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.label_expressions.LabelExpression.getRelTypes
import org.neo4j.cypher.internal.util.NonEmptyList

import scala.annotation.tailrec

object SimplePatternConverters {

  /**
   * Convert node and relationship patterns into a fully-fledged path pattern.
   */
  def convertSimplePattern(simplePattern: SimplePattern): ExhaustivePathPattern[PatternRelationship] =
    simplePattern match {
      case nodePattern: NodePattern =>
        ExhaustivePathPattern.SingleNode(getNodePatternVariableName(nodePattern))
      case relationshipChain: RelationshipChain =>
        ExhaustivePathPattern.NodeConnections(convertRelationshipChain(relationshipChain))
    }

  /**
   * Recursively thread relationship patterns together, preserving the order in which they appear.
   */
  def convertRelationshipChain(relationshipChain: RelationshipChain): NonEmptyList[PatternRelationship] =
    convertRelationshipChainRec(relationshipChain, None)

  @tailrec
  private def convertRelationshipChainRec(
    relationshipChain: RelationshipChain,
    relationships: Option[NonEmptyList[PatternRelationship]]
  ): NonEmptyList[PatternRelationship] =
    relationshipChain.element match {
      case node: NodePattern =>
        val relationship = makePatternRelationship(
          leftNode = node,
          relationship = relationshipChain.relationship,
          rightNode = relationshipChain.rightNode
        )
        relationships.map(list => relationship +: list).getOrElse(NonEmptyList(relationship))

      case nested: RelationshipChain =>
        val relationship = makePatternRelationship(
          leftNode = nested.rightNode,
          relationship = relationshipChain.relationship,
          rightNode = relationshipChain.rightNode
        )
        val acc = relationships.map(list => relationship +: list).getOrElse(NonEmptyList(relationship))
        convertRelationshipChainRec(nested, Some(acc))
    }

  /**
   * Combine a relationship pattern with its end nodes.
   */
  def makePatternRelationship(
    leftNode: NodePattern,
    relationship: RelationshipPattern,
    rightNode: NodePattern
  ): PatternRelationship =
    PatternRelationship(
      variable = getRelationshipPatternVariableName(relationship),
      boundaryNodes = (getNodePatternVariableName(leftNode), getNodePatternVariableName(rightNode)),
      dir = relationship.direction,
      types = getRelTypes(relationship.labelExpression),
      length = convertRelationshipLength(relationship.length)
    )

  private def getRelationshipPatternVariableName(relationshipPattern: RelationshipPattern): LogicalVariable =
    relationshipPattern
      .variable
      .getOrElse(throw new IllegalArgumentException("Missing variable in relationship pattern"))

  private def getNodePatternVariableName(nodePattern: NodePattern): LogicalVariable =
    nodePattern
      .variable
      .getOrElse(throw new IllegalArgumentException("Missing variable in node pattern"))

  private[converters] def convertRelationshipLength(relationshipLength: Option[Option[Range]]): PatternLength =
    relationshipLength match {
      case Some(Some(Range(Some(left), Some(right)))) => VarPatternLength(left.value.toInt, Some(right.value.toInt))
      case Some(Some(Range(Some(left), None)))        => VarPatternLength(left.value.toInt, None)
      case Some(Some(Range(None, Some(right))))       => VarPatternLength(1, Some(right.value.toInt))
      case Some(Some(Range(None, None)))              => VarPatternLength.unlimited
      case Some(None)                                 => VarPatternLength.unlimited
      case None                                       => SimplePatternLength
    }
}
