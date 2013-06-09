/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.symbols.{NodeType, RelationshipType, PathType}
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.mutation
import org.neo4j.cypher.internal.parser.{ParsedEntity, ParsedNamedPath, AbstractPattern}
import org.neo4j.cypher.internal.commands.values.TokenType.Label

object Pattern {
  sealed trait SemanticContext
  object SemanticContext {
    case object Match extends SemanticContext
    case object Update extends SemanticContext
    case object Expression extends SemanticContext
  }

  implicit class SemanticCheckablePatternTraversable(patterns: TraversableOnce[Pattern]) {
    def semanticCheck(context: SemanticContext): SemanticCheck = {
      patterns.foldLeft(SemanticCheckResult.success) { (f, p) => f >>= p.semanticCheck(context) }
    }
  }
}
import Pattern._


sealed abstract class Pattern extends AstNode {
  def semanticCheck(context: SemanticContext): SemanticCheck

  def toLegacyPatterns: Seq[commands.Pattern]
  def toLegacyNamedPath: Option[commands.NamedPath]
  def toLegacyCreates: Seq[mutation.UpdateAction]
  def toLegacyPredicates: Seq[commands.Predicate]
  def toAbstractPatterns: Seq[AbstractPattern]
}

case class AnonymousPattern(path: PathPattern) extends Pattern {
  def token = path.token

  def semanticCheck(context: SemanticContext) = path.semanticCheck(context)

  lazy val toLegacyPatterns = path.toLegacyPatterns(None)
  val toLegacyNamedPath = None
  lazy val toLegacyCreates = path.toLegacyCreates(None)
  lazy val toLegacyPredicates = path.toLegacyPredicates(None)
  lazy val toAbstractPatterns = path.toAbstractPatterns(None)
}

case class NamedPattern(identifier: Identifier, path: PathPattern, token: InputToken) extends Pattern {
  def semanticCheck(context: SemanticContext) = path.semanticCheck(context) >>= identifier.declare(PathType())

  lazy val toLegacyPatterns = path.toLegacyPatterns(Some(identifier.name))
  lazy val toLegacyNamedPath = path.toLegacyNamedPath(identifier.name)
  lazy val toLegacyCreates = path.toLegacyCreates(Some(identifier.name))
  lazy val toLegacyPredicates = path.toLegacyPredicates(Some(identifier.name))
  lazy val toAbstractPatterns = path.toAbstractPatterns(Some(identifier.name))
}

case class RelationshipsPattern(element: PatternElement, token: InputToken) extends Pattern {
  def semanticCheck(context: SemanticContext) = element.semanticCheck(context)

  lazy val toLegacyPatterns = element.toLegacyPatterns(true)
  val toLegacyNamedPath = None
  lazy val toLegacyCreates = element.toLegacyCreates
  lazy val toLegacyPredicates = element.toLegacyPredicates
  lazy val toAbstractPatterns = element.toAbstractPatterns
}


sealed abstract class PathPattern extends AstNode {
  def semanticCheck(context: SemanticContext): SemanticCheck

  def toLegacyPatterns(pathName: Option[String]) : Seq[commands.Pattern]
  def toLegacyNamedPath(pathName: String) : Option[commands.NamedPath]
  def toLegacyCreates(pathName: Option[String]) : Seq[mutation.UpdateAction]
  def toLegacyPredicates(pathName: Option[String]) : Seq[commands.Predicate]
  def toAbstractPatterns(pathName: Option[String]) : Seq[AbstractPattern]
}

case class EveryPath(element: PatternElement) extends PathPattern {
  def token = element.token

  def semanticCheck(context: SemanticContext) = element.semanticCheck(context)

  def toLegacyPatterns(pathName: Option[String]) = element.toLegacyPatterns(pathName.isEmpty)
  def toLegacyNamedPath(pathName: String) = Some(commands.NamedPath(pathName, element.toLegacyPatterns(pathName.isEmpty):_*))
  def toLegacyCreates(pathName: Option[String]) = element.toLegacyCreates
  def toLegacyPredicates(pathName: Option[String]) = element.toLegacyPredicates
  def toAbstractPatterns(pathName: Option[String]) = {
    val patterns = element.toAbstractPatterns
    pathName match {
      case None       => patterns
      case Some(name) => Seq(ParsedNamedPath(name, patterns))
    }
  }
}

case class ShortestPath(element: PatternElement, token: InputToken) extends PathPattern {
  def semanticCheck(context: SemanticContext) = checkContainsSingle >>= checkNoMinimalLength

  private def checkContainsSingle: SemanticCheck = element match {
    case RelationshipChain(l: NamedNodePattern, _, r: NamedNodePattern, _) => {
      l.identifier.ensureDefined(NodeType()) >>=
        r.identifier.ensureDefined(NodeType())
    }
    case RelationshipChain(l: NodePattern, _, _, _)                        =>
      SemanticError(s"shortestPath requires named nodes", token, l.token)
    case _                                                                 =>
      SemanticError(s"shortestPath requires a pattern containing a single relationship", token, element.token)
  }

  private def checkNoMinimalLength: SemanticCheck = element match {
    case RelationshipChain(_, rel, _, _) => rel.length match {
      case Some(Some(Range(Some(_), _, _))) =>
        SemanticError(s"shortestPath does not support a minimal length", token, element.token)
      case _                                =>
        SemanticCheckResult.success
    }
    case _                               => SemanticCheckResult.success
  }

  def toLegacyPatterns(maybePathName: Option[String]) = {
    val pathName = maybePathName.getOrElse("  UNNAMED" + token.startPosition.offset)

    val (leftName, rel, rightName) = element match {
      case RelationshipChain(leftNode: NodePattern, relationshipPattern, rightNode, _) =>
        (leftNode.legacyName, relationshipPattern, rightNode.legacyName)
      case _                                                           =>
        throw new ThisShouldNotHappenError("Chris", "This should be caught during semantic checking")
    }
    val reltypes = rel.types.map(_.name)
    val maxDepth = rel.length match {
      case Some(Some(Range(None, Some(i), _))) => Some(i.value.toInt)
      case _                                   => None
    }
    Seq(commands.ShortestPath(pathName, leftName, rightName, reltypes, rel.direction, maxDepth, rel.optional, true, None))
  }

  def toLegacyNamedPath(pathName: String) = None
  def toLegacyCreates(pathName: Option[String]) = ???
  def toLegacyPredicates(pathName: Option[String]) = Seq()
  def toAbstractPatterns(pathName: Option[String]): Seq[AbstractPattern] = ???
}


sealed abstract class PatternElement extends AstNode {
  def semanticCheck(context: SemanticContext): SemanticCheck

  def toLegacyPatterns(makeOutgoing: Boolean) : Seq[commands.Pattern]
  def toLegacyCreates : Seq[mutation.UpdateAction]
  def toLegacyPredicates : Seq[commands.Predicate]
  def toAbstractPatterns : Seq[AbstractPattern]
}

case class RelationshipChain(element: PatternElement, relationship: RelationshipPattern, rightNode: NodePattern, token: InputToken) extends PatternElement {
  def semanticCheck(context: SemanticContext) = {
    element.semanticCheck(context) >>=
    relationship.semanticCheck(context) >>=
    rightNode.semanticCheck(context)
  }

  def toLegacyPatterns(makeOutgoing: Boolean) : Seq[commands.Pattern] = {
    val (patterns, leftName) = element match {
      case leftNode: NodePattern        => (Vector(), leftNode.legacyName)
      case leftChain: RelationshipChain => (leftChain.toLegacyPatterns(makeOutgoing), leftChain.rightNode.legacyName)
    }

    patterns :+ relationship.toLegacyPattern(leftName, rightNode.legacyName, makeOutgoing)
  }

  lazy val toLegacyCreates : Seq[mutation.CreateRelationship] = {
    val (creates, leftEndpoint) = element match {
      case leftNode: NodePattern        => (Vector(), leftNode.toLegacyEndpoint)
      case leftChain: RelationshipChain =>
        val creates = leftChain.toLegacyCreates
        (creates, creates.last.to)
    }

    creates :+ relationship.toLegacyCreates(leftEndpoint, rightNode.toLegacyEndpoint)
  }

  def toLegacyPredicates = element.toLegacyPredicates ++ rightNode.toLegacyPredicates

  def toAbstractPatterns: Seq[AbstractPattern] = ???
}


sealed abstract class NodePattern extends PatternElement {
  val labels: Seq[Identifier]
  val properties: Option[Expression]

  def semanticCheck(context: SemanticContext): SemanticCheck = {
    if (properties.isDefined && context != SemanticContext.Update) {
      SemanticError("Node properties cannot be specified in this context", properties.get.token)
    } else {
      properties.semanticCheck(Expression.SemanticContext.Simple)
    }
  }

  def legacyName: String

  def toLegacyPatterns(makeOutgoing: Boolean) = Seq(commands.SingleNode(legacyName))

  def toLegacyCreates = {
    val (props, labels, bare) = legacyDetails
    Seq(mutation.CreateNode(legacyName, props, labels, bare))
  }

  def toLegacyEndpoint: mutation.RelationshipEndpoint = {
    val (props, labels, bare) = legacyDetails
    mutation.RelationshipEndpoint(commandexpressions.Identifier(legacyName), props, labels, bare)
  }

  protected lazy val legacyDetails = {
    val props = properties match {
      case Some(m: MapExpression) => m.items.map(p => (p._1.name, p._2.toCommand)).toMap
      case Some(p: Parameter)     => Map[String, CommandExpression]("*" -> p.toCommand)
      case Some(p)                => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.token.startPosition})")
      case None                   => Map[String, CommandExpression]()
    }
    val bare = labels.isEmpty && (props.isEmpty || !commandexpressions.Identifier.isNamed(legacyName))
    (props, labels.map(t => commandvalues.KeyToken.Unresolved(t.name, commandvalues.TokenType.Label)), bare)
  }

  def toLegacyPredicates = labels.map(t => {
    val id = commandexpressions.Identifier(legacyName)
    commands.HasLabel(id, commandvalues.KeyToken.Unresolved(t.name, Label))
  })

  def toAbstractPatterns: Seq[AbstractPattern] = {
    val (props, labels, bare) = legacyDetails
    Seq(ParsedEntity(legacyName, commandexpressions.Identifier(legacyName), props, labels, bare))
  }

}

case class NamedNodePattern(identifier: Identifier, labels: Seq[Identifier], properties: Option[Expression], token: InputToken) extends NodePattern {
  override def semanticCheck(context: SemanticContext) =
    identifier.implicitDeclaration(NodeType()) >>=
    super.semanticCheck(context)

  val legacyName = identifier.name
}

case class AnonymousNodePattern(labels: Seq[Identifier], properties: Option[Expression], token: InputToken) extends NodePattern {
  val legacyName = "  UNNAMED" + (token.startPosition.offset + 1)
}


sealed abstract class RelationshipPattern extends AstNode {
  val direction : Direction
  val types : Seq[Identifier]
  val length : Option[Option[Range]]
  val optional : Boolean
  val properties : Option[Expression]

  def semanticCheck(context: SemanticContext): SemanticCheck = {
    if (properties.isDefined && context != SemanticContext.Update) {
      SemanticError("Relationship properties cannot be specified in this context", properties.get.token)
    } else if (optional && context == SemanticContext.Expression) {
      SemanticError("Optional relationships cannot be specified in this context", token)
    } else {
      properties.semanticCheck(Expression.SemanticContext.Simple)
    }
  }

  def legacyName : String

  def toLegacyPattern(leftName: String, rightName: String, makeOutgoing: Boolean): commands.Pattern = {
    val (left, right, dir) = if (!makeOutgoing) (leftName, rightName, direction)
    else direction match {
      case Direction.OUTGOING                     => (leftName, rightName, direction)
      case Direction.INCOMING                     => (rightName, leftName, Direction.OUTGOING)
      case Direction.BOTH if leftName < rightName => (leftName, rightName, direction)
      case Direction.BOTH                         => (rightName, leftName, direction)
    }

    length match {
      case Some(maybeRange) => {
        val pathName = "  UNNAMED" + token.startPosition.offset
        val (min, max) = maybeRange match {
          case Some(range) => (for (i <- range.lower) yield i.value.toInt, for (i <- range.upper) yield i.value.toInt)
          case None        => (None, None)
        }
        val relIterator = this match {
          case namedRel: NamedRelationshipPattern => Some(namedRel.identifier.name)
          case _                                  => None
        }
        commands.VarLengthRelatedTo(pathName, left, right, min, max, types.map(_.name).distinct, dir, relIterator, optional)
      }
      case None             => commands.RelatedTo(left, right, legacyName, types.map(_.name).distinct, dir, optional)
    }
  }

  def toLegacyCreates(fromEnd: mutation.RelationshipEndpoint, toEnd: mutation.RelationshipEndpoint) : mutation.CreateRelationship = {
    val (from, to) = direction match {
      case Direction.OUTGOING => (fromEnd, toEnd)
      case Direction.INCOMING => (toEnd, fromEnd)
      case Direction.BOTH => (fromEnd.node, toEnd.node) match {
        case (commandexpressions.Identifier(a), commandexpressions.Identifier(b)) if a >= b => (toEnd, fromEnd)
        case _ => (fromEnd, toEnd)
      }
    }
    val typeName = types match {
      case Seq(i) => i.name
      case _ => throw new SyntaxException(s"A single relationship type must be specified for CREATE (${token.startPosition})")
    }
    val props = properties match {
      case Some(m: MapExpression) => m.items.map(p => (p._1.name, p._2.toCommand)).toMap
      case Some(p: Parameter)     => Map[String, CommandExpression]("*" -> p.toCommand)
      case Some(p)                => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.token.startPosition})")
      case None                   => Map[String, CommandExpression]()
    }
    mutation.CreateRelationship(legacyName, from, to, typeName, props)
  }
}

case class NamedRelationshipPattern(
    identifier: Identifier,
    direction: Direction,
    types: Seq[Identifier],
    length: Option[Option[Range]],
    optional: Boolean,
    properties : Option[Expression],
    token: InputToken) extends RelationshipPattern
{
  override def semanticCheck(context: SemanticContext) = super.semanticCheck(context) >>= identifier.implicitDeclaration(RelationshipType())

  val legacyName = identifier.name
}

case class AnonymousRelationshipPattern(
    direction: Direction,
    types: Seq[Identifier],
    length: Option[Option[Range]],
    optional: Boolean,
    properties : Option[Expression],
    token: InputToken) extends RelationshipPattern
{
  val legacyName = "  UNNAMED" + token.startPosition.offset
}
