/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, SyntaxException, ast}
import org.neo4j.helpers.ThisShouldNotHappenError

object PatternConverters {

  implicit class PatternConverter(val pattern: ast.Pattern) extends AnyVal {
    def asLegacyPatterns: Seq[commands.Pattern] = pattern.patternParts.flatMap(_.asLegacyPatterns)
    def asLegacyNamedPaths: Seq[commands.NamedPath] = pattern.patternParts.flatMap(_.asLegacyNamedPath)
    def asLegacyCreates: Seq[mutation.UpdateAction] = pattern.patternParts.flatMap(_.asLegacyCreates)
    def asAbstractPatterns: Seq[AbstractPattern] = pattern.patternParts.flatMap(_.asAbstractPatterns)
  }

  implicit class RelationshipsPatternConverter(val pattern: ast.RelationshipsPattern) extends AnyVal {
    def asLegacyPatterns = pattern.element.asLegacyPatterns
    def asLegacyNamedPath = None
    def asLegacyCreates = pattern.element.asLegacyCreates
    def asAbstractPatterns = pattern.element.asAbstractPatterns
  }

  implicit class PatternPartConverter(val part: ast.PatternPart) extends AnyVal {
    def asLegacyPatterns: Seq[commands.Pattern] = part match {
      case ast.NamedPatternPart(identifier, anonPart) =>
        anonPart.asLegacyPatterns(Some(identifier.name))
      case anonPart: ast.AnonymousPatternPart =>
        anonPart.asLegacyPatterns(None)
    }
    def asLegacyNamedPath: Option[commands.NamedPath] = part match {
      case ast.NamedPatternPart(identifier, anonPart) =>
        anonPart.asLegacyNamedPath(identifier.name)
      case anonPart: ast.AnonymousPatternPart =>
        None
    }
    def asLegacyCreates: Seq[mutation.UpdateAction] = part match {
      case ast.NamedPatternPart(identifier, anonPart) =>
        anonPart.asLegacyCreates
      case anonPart: ast.AnonymousPatternPart =>
        anonPart.asLegacyCreates
    }
    def asAbstractPatterns: Seq[AbstractPattern] = part match {
      case ast.NamedPatternPart(identifier, anonPart) =>
        anonPart.asAbstractPatterns(Some(identifier.name))
      case anonPart: ast.AnonymousPatternPart =>
        anonPart.asAbstractPatterns(None)
    }
  }

  implicit class AnonymousPatternPartConverter(val part: ast.AnonymousPatternPart) extends AnyVal {
    def asLegacyPatterns(maybePathName: Option[String]): Seq[commands.Pattern] = part match {
      case part: ast.EveryPath =>
        EveryPathConverter(part).asLegacyPatterns(maybePathName)
      case part: ast.ShortestPaths =>
        ShortestPathsConverter(part).asLegacyPatterns(maybePathName)
    }

    def asLegacyNamedPath(pathName: String): Option[commands.NamedPath] = part match {
      case part: ast.EveryPath =>
        EveryPathConverter(part).asLegacyNamedPath(pathName)
      case part: ast.ShortestPaths =>
        ShortestPathsConverter(part).asLegacyNamedPath(pathName)
    }
    def asLegacyCreates: Seq[mutation.UpdateAction] = part match {
      case part: ast.EveryPath =>
        EveryPathConverter(part).asLegacyCreates
      case part: ast.ShortestPaths =>
        throw new ThisShouldNotHappenError("cleishm", "Unexpected conversion of ShortestPaths to UpdateAction")
    }
    def asAbstractPatterns(maybePathName: Option[String]): Seq[AbstractPattern] = part match {
      case part: ast.EveryPath =>
        EveryPathConverter(part).asAbstractPatterns(maybePathName)
      case part: ast.ShortestPaths =>
        throw new ThisShouldNotHappenError("cleishm", "Unexpected conversion of ShortestPaths to AbstractPattern")
    }
  }

  implicit class EveryPathConverter(val part: ast.EveryPath) extends AnyVal {
    def asLegacyPatterns(maybePathName: Option[String]): Seq[commands.Pattern] =
      part.element.asLegacyPatterns

    def asLegacyNamedPath(pathName: String) =
      Some(commands.NamedPath(pathName, part.element.asAbstractPatterns:_*))

    def asLegacyCreates = part.element.asLegacyCreates

    def asAbstractPatterns(maybePathName: Option[String]) = {
      val patterns = part.element.asAbstractPatterns
      maybePathName.fold(patterns)(n => Seq(ParsedNamedPath(n, patterns)))
    }
  }

  implicit class ShortestPathsConverter(val part: ast.ShortestPaths) extends AnyVal {
    def asLegacyPatterns(maybePathName: Option[String]): Seq[commands.ShortestPath] = {
      val pathName = maybePathName.getOrElse(UnNamedNameGenerator.name(part.position))
      val (leftName, rel, rightName) = part.element match {
        case ast.RelationshipChain(leftNode: ast.NodePattern, relationshipPattern, rightNode) =>
          (leftNode.asLegacyNode, relationshipPattern, rightNode.asLegacyNode)
        case _                                                                        =>
          throw new ThisShouldNotHappenError("Chris", "This should be caught during semantic checking")
      }
      val reltypes = rel.types.map(_.name)
      val relIteratorName = rel.identifier.map(_.name)
      val (allowZeroLength, maxDepth) = rel.length match {
        case Some(Some(ast.Range(lower, max))) => (lower.exists(_.value == 0L),  max.map(_.value.toInt))
        case None                              => (false, Some(1))//non-varlength case
        case _                                 => (false, None)
      }
      Seq(commands.ShortestPath(pathName, leftName, rightName, reltypes, rel.direction, allowZeroLength, maxDepth, part.single, relIteratorName))
    }

    def asLegacyNamedPath(pathName: String) = None
  }

  implicit class PatternElementConverter(val element: ast.PatternElement) extends AnyVal {
    def asLegacyPatterns: Seq[commands.Pattern] = element match {
      case element: ast.RelationshipChain =>
        RelationshipChainConverter(element).asLegacyPatterns
      case element: ast.NodePattern =>
        NodePatternConverter(element).asLegacyPatterns
    }

    def asLegacyCreates: Seq[mutation.UpdateAction] = element match {
      case element: ast.RelationshipChain =>
        RelationshipChainConverter(element).asLegacyCreates
      case element: ast.NodePattern =>
        NodePatternConverter(element).asLegacyCreates
    }

    def asAbstractPatterns: Seq[AbstractPattern] = element match {
      case element: ast.RelationshipChain =>
        RelationshipChainConverter(element).asAbstractPatterns
      case element: ast.NodePattern =>
        NodePatternConverter(element).asAbstractPatterns
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

    def asLegacyCreates: Seq[mutation.CreateRelationship] = {
      val (creates, leftEndpoint) = chain.element match {
        case leftNode: ast.NodePattern        =>
          (Vector(), leftNode.asRelationshipEndpoint)
        case leftChain: ast.RelationshipChain =>
          val creates = leftChain.asLegacyCreates
          (creates, leftChain.rightNode.asRelationshipEndpoint)
      }

      creates :+ chain.relationship.asLegacyCreates(leftEndpoint, chain.rightNode.asRelationshipEndpoint)
    }

    def asAbstractPatterns: Seq[AbstractPattern] = {
      def createParsedRelationship(node: ast.NodePattern): AbstractPattern = {
        val props = chain.relationship.legacyProperties
        val startNode = node.asAbstractPatterns.head.asInstanceOf[ParsedEntity]
        val endNode = chain.rightNode.asAbstractPatterns.head.asInstanceOf[ParsedEntity]

        val maxDepth = chain.relationship.length match {
          case Some(Some(ast.Range(_, Some(i)))) => Some(i.value.toInt)
          case _                                 => None
        }

        val minDepth = chain.relationship.length match {
          case Some(Some(ast.Range(Some(i), _))) => Some(i.value.toInt)
          case _                                 => None
        }

        chain.relationship.length match {
          case None =>
            ParsedRelation(
              chain.relationship.legacyName, props, startNode, endNode,
              chain.relationship.types.map(_.name), chain.relationship.direction, chain.relationship.optional)

          case _    =>
            val (relName, relIterator) = chain.relationship.identifier match {
              case Some(_) =>
                (UnNamedNameGenerator.name(chain.relationship.position), Some(chain.relationship.legacyName))
              case None =>
                (chain.relationship.legacyName, None)
            }

            ParsedVarLengthRelation(relName, props, startNode, endNode, chain.relationship.types.map(_.name),
              chain.relationship.direction, chain.relationship.optional, minDepth, maxDepth, relIterator)
        }
      }

      chain.element match {
        case node: ast.NodePattern      => Seq(createParsedRelationship(node))
        case rel: ast.RelationshipChain => rel.asAbstractPatterns :+ createParsedRelationship(rel.rightNode)
      }
    }
  }

  implicit class NodePatternConverter(val node: ast.NodePattern) extends AnyVal {
    def asLegacyPatterns = Seq(asLegacyNode)

    def asLegacyCreates =
      Seq(mutation.CreateNode(node.legacyName, node.legacyProperties, labels))

    def asAbstractPatterns: Seq[AbstractPattern] =
      Seq(ParsedEntity(node.legacyName, commandexpressions.Identifier(node.legacyName), node.legacyProperties, labels))

    def asRelationshipEndpoint: mutation.RelationshipEndpoint =
      mutation.RelationshipEndpoint(commandexpressions.Identifier(node.legacyName), node.legacyProperties, labels)

    def asLegacyNode = commands.SingleNode(node.legacyName, labels.map(x => commandvalues.UnresolvedLabel(x.name)), properties = node.legacyProperties)

    def legacyName = node.identifier.fold(UnNamedNameGenerator.name(node.position))(_.name)

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
          val relIterator = relationship.identifier.map(_.name)
          commands.VarLengthRelatedTo(pathName, leftNode.asLegacyNode, rightNode.asLegacyNode, min, max,
            relationship.types.map(_.name).distinct, relationship.direction, relIterator, properties = legacyProperties)
        case None             =>
          commands.RelatedTo(leftNode.asLegacyNode, rightNode.asLegacyNode, relationship.legacyName,
            relationship.types.map(_.name).distinct, relationship.direction, legacyProperties)
      }
    }

    def asLegacyCreates(fromEnd: mutation.RelationshipEndpoint, toEnd: mutation.RelationshipEndpoint): mutation.CreateRelationship = {
      val (from, to) = relationship.direction match {
        case SemanticDirection.INCOMING => (toEnd, fromEnd)
        // Direction.{OUTGOING|BOTH}
        case _                  => (fromEnd, toEnd)
      }
      val typeName = relationship.types match {
        case Seq(i) => i.name
        case _ => throw new SyntaxException(s"A single relationship type must be specified for CREATE (${relationship.position})")
      }
      mutation.CreateRelationship(relationship.legacyName, from, to, typeName, legacyProperties)
    }

    def legacyName = relationship.identifier.fold(UnNamedNameGenerator.name(relationship.position))(_.name)

    def legacyProperties: Map[String, CommandExpression] = relationship.properties match {
      case None                       => Map.empty[String, CommandExpression]
      case Some(m: ast.MapExpression) => m.items.map(p => p._1.name -> toCommandExpression(p._2))(collection.breakOut)
      case Some(p: ast.Parameter)     => Map("*" -> toCommandExpression(p))
      case Some(p)                    => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.position})")
    }
  }
}
