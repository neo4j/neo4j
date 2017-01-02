/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v3_2.commands.{values => commandvalues}
import org.neo4j.cypher.internal.compiler.v3_2.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v3_2.{SyntaxException, ast}

object PatternConverters {

  implicit class RelationshipsPatternConverter(val pattern: ast.RelationshipsPattern) extends AnyVal {
    def asLegacyPatterns = pattern.element.asLegacyPatterns
  }

  implicit class ShortestPathsConverter(val part: ast.ShortestPaths) extends AnyVal {
    def asLegacyPatterns(maybePathName: Option[String]): Seq[commands.ShortestPath] = {
      val pathName = maybePathName.getOrElse(UnNamedNameGenerator.name(part.position))
      val (leftName, rel, rightName) = part.element match {
        case ast.RelationshipChain(leftNode: ast.NodePattern, relationshipPattern, rightNode) =>
          (leftNode.asLegacyNode, relationshipPattern, rightNode.asLegacyNode)
        case _                                                                        =>
          throw new IllegalStateException("This should be caught during semantic checking")
      }
      val reltypes = rel.types.map(_.name)
      val relIteratorName = rel.variable.map(_.name)
      val (allowZeroLength, maxDepth) = rel.length match {
        case Some(Some(ast.Range(lower, max))) => (lower.exists(_.value == 0L),  max.map(_.value.toInt))
        case None                              => (false, Some(1))//non-varlength case
        case _                                 => (false, None)
      }
      Seq(commands.ShortestPath(pathName, leftName, rightName, reltypes, rel.direction, allowZeroLength, maxDepth, part.single, relIteratorName))
    }
  }

  implicit class RelationshipChainConverter(val chain: ast.RelationshipChain) extends AnyVal {
    def asLegacyPatterns: Seq[commands.Pattern] = {
      val (patterns, leftNode) = chain.element match {
        case node: ast.NodePattern            =>
          (Vector(), node)
        case leftChain: ast.RelationshipChain =>
          (leftChain.asLegacyPatterns, leftChain.rightNode)
      }

      patterns :+ chain.relationship.asLegacyPattern(leftNode, chain.rightNode)
    }
  }

  implicit class NodePatternConverter(val node: ast.NodePattern) extends AnyVal {


    def asLegacyNode = commands.SingleNode(node.legacyName, labels.map(x => commandvalues.UnresolvedLabel(x.name)), properties = node.legacyProperties)

    def legacyName = node.variable.fold(UnNamedNameGenerator.name(node.position))(_.name)

    private def labels = node.labels.map(t => commandvalues.KeyToken.Unresolved(t.name, commandvalues.TokenType.Label))

    def legacyProperties = node.properties match {
      case Some(m: ast.MapExpression) => m.items.map(p => (p._1.name, toCommandExpression(p._2))).toMap
      case Some(p: ast.Parameter)     => Map[String, CommandExpression]("*" -> toCommandExpression(p))
      case Some(p)                    => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.position})")
      case None                       => Map[String, CommandExpression]()
    }
  }

  implicit class RelationshipPatternConverter(val relationship: ast.RelationshipPattern) extends AnyVal {
    def asLegacyPattern(leftNode: ast.NodePattern, rightNode: ast.NodePattern): commands.Pattern = {
      relationship.length match {
        case Some(maybeRange) =>
          val pathName = UnNamedNameGenerator.name(relationship.position)
          val (min, max) = maybeRange match {
            case Some(range) => (for (i <- range.lower) yield i.value.toInt, for (i <- range.upper) yield i.value.toInt)
            case None        => (None, None)
          }
          val relIterator = relationship.variable.map(_.name)
          commands.VarLengthRelatedTo(pathName, leftNode.asLegacyNode, rightNode.asLegacyNode, min, max,
            relationship.types.map(_.name).distinct, relationship.direction, relIterator, properties = legacyProperties)
        case None             =>
          commands.RelatedTo(leftNode.asLegacyNode, rightNode.asLegacyNode, relationship.legacyName,
            relationship.types.map(_.name).distinct, relationship.direction, legacyProperties)
      }
    }

    def legacyName = relationship.variable.fold(UnNamedNameGenerator.name(relationship.position))(_.name)

    def legacyProperties: Map[String, CommandExpression] = relationship.properties match {
      case None                       => Map.empty[String, CommandExpression]
      case Some(m: ast.MapExpression) => m.items.map(p => p._1.name -> toCommandExpression(p._2))(collection.breakOut)
      case Some(p: ast.Parameter)     => Map("*" -> toCommandExpression(p))
      case Some(p)                    => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.position})")
    }
  }
}
