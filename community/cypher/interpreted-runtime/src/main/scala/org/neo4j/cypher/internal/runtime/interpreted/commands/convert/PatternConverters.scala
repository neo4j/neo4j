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
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.label_expressions.LabelExpression.containsGpmSpecificRelType
import org.neo4j.cypher.internal.label_expressions.LabelExpression.getRelTypes
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.SingleNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.SyntaxException

object PatternConverters {

  implicit class ShortestPathsConverter(val part: internal.expressions.ShortestPathsPatternPart) extends AnyVal {

    def asLegacyPatterns(
      id: Id,
      maybePathName: Option[String],
      converter: ExpressionConverters,
      anonymousVariableNameGenerator: AnonymousVariableNameGenerator
    ): Seq[commands.ShortestPath] = {
      val pathName = maybePathName.getOrElse(anonymousVariableNameGenerator.nextName)
      val (leftName, rel, rightName) = part.element match {
        case internal.expressions.RelationshipChain(
            leftNode: internal.expressions.NodePattern,
            relationshipPattern,
            rightNode
          ) =>
          (
            leftNode.asLegacyNode(id, converter, anonymousVariableNameGenerator),
            relationshipPattern,
            rightNode.asLegacyNode(id, converter, anonymousVariableNameGenerator)
          )
        case _ =>
          throw new IllegalStateException("This should be caught during semantic checking")
      }
      if (containsGpmSpecificRelType(rel.labelExpression)) {
        // LabelExpressionsInPatternsNormalizer should have rewritten all GPM relationship type expressions to predicates
        throw new IllegalStateException(
          s"Relationship type expressions should be rewritten at this point but was ${rel.labelExpression}."
        )
      }
      val relTypes = getRelTypes(rel.labelExpression).map(_.name)
      val relIteratorName = rel.variable.map(_.name)
      val (allowZeroLength, maxDepth) = rel.length match {
        case Some(Some(internal.expressions.Range(lower, max))) => (lower.exists(_.value == 0L), max.map(_.value.toInt))
        case None                                               => (false, Some(1)) // non-varlength case
        case _                                                  => (false, None)
      }
      Seq(commands.ShortestPath(
        pathName,
        leftName,
        rightName,
        relTypes,
        rel.direction,
        allowZeroLength,
        maxDepth,
        part.single,
        relIteratorName
      ))
    }
  }

  implicit class NodePatternConverter(val node: internal.expressions.NodePattern) extends AnyVal {

    def asLegacyNode(
      id: Id,
      converter: ExpressionConverters,
      anonymousVariableNameGenerator: AnonymousVariableNameGenerator
    ): SingleNode = {
      val labelTokens: Seq[KeyToken] = labels.map(x => commands.values.UnresolvedLabel(x.name))
      val properties: Map[String, commands.expressions.Expression] = node.legacyProperties(id, converter)
      val value = node.variable.map(converter.toCommandExpression(id, _))
      commands.SingleNode(node.legacyName(anonymousVariableNameGenerator), value, labelTokens, properties = properties)
    }

    def legacyName(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): String =
      node.variable.fold(anonymousVariableNameGenerator.nextName)(_.name)

    private def labels: Seq[KeyToken.Unresolved] =
      for {
        expression <- node.labelExpression.toSeq
        _ = require(!expression.containsGpmSpecificLabelExpression)
        label <- expression.flatten
      } yield commands.values.KeyToken.Unresolved(label.name, commands.values.TokenType.Label)

    def legacyProperties(id: Id, converter: ExpressionConverters): Map[String, commands.expressions.Expression] =
      node.properties match {
        case Some(m: internal.expressions.MapExpression) =>
          m.items.map(p => (p._1.name, converter.toCommandExpression(id, p._2))).toMap
        case Some(p: internal.expressions.Parameter) =>
          Map[String, commands.expressions.Expression]("*" -> converter.toCommandExpression(id, p))
        case Some(p) => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.position})")
        case None    => Map[String, commands.expressions.Expression]()
      }
  }
}
