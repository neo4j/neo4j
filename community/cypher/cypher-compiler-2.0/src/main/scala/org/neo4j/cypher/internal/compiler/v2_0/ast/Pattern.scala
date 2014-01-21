/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.cypher.internal.compiler.v2_0.commands.{expressions => legacy, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.UnresolvedLabel
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.KeyToken.Unresolved
import org.neo4j.cypher.internal.compiler.v2_0.symbols._
import org.neo4j.cypher.internal.compiler.v2_0.mutation.UpdateAction
import org.neo4j.cypher.{PatternException, SyntaxException}
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.graphdb.Direction

object Pattern {
  sealed trait SemanticContext
  object SemanticContext {
    case object Match extends SemanticContext
    case object Merge extends SemanticContext
    case object Create extends SemanticContext
    case object Expression extends SemanticContext
  }
}
import Pattern._

case class Pattern(patternParts: Seq[PatternPart])(val token: InputToken) extends AstNode {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    patternParts.foldSemanticCheck(_.declareIdentifiers(ctx)) then
    patternParts.foldSemanticCheck(_.semanticCheck(ctx))

  def toLegacyPatterns: Seq[commands.Pattern] = patternParts.flatMap(_.toLegacyPatterns)
  def toLegacyNamedPaths: Seq[commands.NamedPath] = patternParts.flatMap(_.toLegacyNamedPath)
  def toLegacyCreates: Seq[mutation.UpdateAction] = patternParts.flatMap(_.toLegacyCreates)
  def toAbstractPatterns: Seq[AbstractPattern] = patternParts.flatMap(_.toAbstractPatterns)

  // TODO: Kill once we get rid of the legacy commands structure
  // This methods gathers up all MERGE patterns that are not single nodes
  def toMergeObjects: (Seq[commands.Pattern], Seq[mutation.UpdateAction]) = {
    val collect: Seq[(Seq[commands.Pattern], Seq[UpdateAction])] = patternParts.collect {
      case p if p.toAbstractPatterns.exists(_.isInstanceOf[ParsedRelation]) => (p.toLegacyPatterns, p.toLegacyCreates)
    }

    val patterns: Seq[commands.Pattern] = collect.flatMap(_._1)
    val actions: Seq[UpdateAction] = collect.flatMap(_._2)

    (patterns, actions)
  }
}


sealed abstract class PatternPart extends AstNode {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def toLegacyPatterns: Seq[commands.Pattern]
  def toLegacyNamedPath: Option[commands.NamedPath]
  def toLegacyCreates: Seq[mutation.UpdateAction]
  def toAbstractPatterns: Seq[AbstractPattern]
}

case class NamedPatternPart(identifier: Identifier, patternPart: AnonymousPatternPart)(val token: InputToken) extends PatternPart {
  def declareIdentifiers(ctx: SemanticContext) = patternPart.declareIdentifiers(ctx) then identifier.declare(CTPath)
  def semanticCheck(ctx: SemanticContext) = patternPart.semanticCheck(ctx)

  lazy val toLegacyPatterns = patternPart.toLegacyPatterns(Some(identifier.name))
  lazy val toLegacyNamedPath = patternPart.toLegacyNamedPath(identifier.name)
  lazy val toLegacyCreates = patternPart.toLegacyCreates(Some(identifier.name))
  lazy val toAbstractPatterns = patternPart.toAbstractPatterns(Some(identifier.name))
}

sealed trait AnonymousPatternPart extends PatternPart {
  lazy val toLegacyPatterns: Seq[commands.Pattern] = toLegacyPatterns(None)
  val toLegacyNamedPath: Option[commands.NamedPath] = None
  lazy val toLegacyCreates: Seq[mutation.UpdateAction] = toLegacyCreates(None)
  lazy val toAbstractPatterns: Seq[AbstractPattern] = toAbstractPatterns(None)

  def toLegacyPatterns(pathName: Option[String]): Seq[commands.Pattern]
  def toLegacyNamedPath(pathName: String): Option[commands.NamedPath]
  def toLegacyCreates(pathName: Option[String]): Seq[mutation.UpdateAction]
  def toAbstractPatterns(pathName: Option[String]): Seq[AbstractPattern]
}

case class RelationshipsPattern(element: RelationshipChain)(val token: InputToken) extends AstNode {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.declareIdentifiers(ctx) then
    element.semanticCheck(ctx)

  lazy val toLegacyPatterns = element.toLegacyPatterns(makeOutgoing = true)
  val toLegacyNamedPath = None
  lazy val toLegacyCreates = element.toLegacyCreates
  lazy val toAbstractPatterns = element.toAbstractPatterns
}


case class EveryPath(element: PatternElement) extends AnonymousPatternPart {
  def token = element.token

  def declareIdentifiers(ctx: SemanticContext) = (element, ctx) match {
    case (n: NodePattern, SemanticContext.Match) =>
      element.declareIdentifiers(ctx) // single node identifier is allowed to be already bound in MATCH
    case (n: NodePattern, _)                     =>
      n.identifier.fold(SemanticCheckResult.success)(_.declare(CTNode)) then element.declareIdentifiers(ctx)
    case _                                       =>
      element.declareIdentifiers(ctx)
  }

  def semanticCheck(ctx: SemanticContext) = element.semanticCheck(ctx)

  def toLegacyPatterns(pathName: Option[String]) = element.toLegacyPatterns(pathName.isEmpty)
  def toLegacyNamedPath(pathName: String) = Some(commands.NamedPath(pathName, element.toAbstractPatterns:_*))
  def toLegacyCreates(pathName: Option[String]) = element.toLegacyCreates
  def toAbstractPatterns(pathName: Option[String]) = {
    val patterns = element.toAbstractPatterns
    pathName match {
      case None       => patterns
      case Some(name) => Seq(ParsedNamedPath(name, patterns))
    }
  }
}

abstract class ShortestPath(element: PatternElement)(val token: InputToken) extends AnonymousPatternPart {
  val name: String
  val single: Boolean

  def declareIdentifiers(ctx: SemanticContext) =
    element.declareIdentifiers(ctx)

  def semanticCheck(ctx: SemanticContext) =
    checkContext(ctx) then
    checkContainsSingle then
    checkKnownEnds then
    checkNoMinimalLength then
    element.semanticCheck(ctx)

  private def checkContext(ctx: SemanticContext): SemanticCheck = ctx match {
    case SemanticContext.Merge  => SemanticError(s"${name}(...) cannot be used to MERGE", token, element.token)
    case SemanticContext.Create => SemanticError(s"${name}(...) cannot be used to CREATE", token, element.token)
    case _                      => None
  }

  private def checkContainsSingle: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      None
    case _                                                    =>
      SemanticError(s"${name}(...) requires a pattern containing a single relationship", token, element.token)
  }

  private def checkKnownEnds: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      if (l.identifier.isEmpty)
        SemanticError(s"${name}(...) requires named nodes", token, l.token)
      else if (r.identifier.isEmpty)
        SemanticError(s"${name}(...) requires named nodes", token, r.token)
      else
        None
    case _                                                    =>
      None
  }

  private def checkNoMinimalLength: SemanticCheck = element match {
    case RelationshipChain(_, rel, _) =>
      rel.length match {
        case Some(Some(Range(Some(_), _))) =>
          Some(SemanticError(s"${name}(...) does not support a minimal length", token, element.token))
        case _                             =>
          None
      }
    case _                            =>
      None
  }

  def toLegacyPatterns(maybePathName: Option[String]): Seq[commands.ShortestPath] = {
    val pathName = maybePathName.getOrElse("  UNNAMED" + token.startPosition.offset)

    val (leftName, rel, rightName) = element match {
      case RelationshipChain(leftNode: NodePattern, relationshipPattern, rightNode) =>
        (leftNode.toLegacyNode, relationshipPattern, rightNode.toLegacyNode)
      case _                                                                        =>
        throw new ThisShouldNotHappenError("Chris", "This should be caught during semantic checking")
    }
    val reltypes = rel.types.map(_.name)
    val maxDepth = rel.length match {
      case Some(Some(Range(None, Some(i)))) => Some(i.value.toInt)
      case _                                => None
    }
    Seq(commands.ShortestPath(pathName, leftName, rightName, reltypes, rel.direction, maxDepth, single, None))
  }

  def toLegacyNamedPath(pathName: String) = None
  def toLegacyCreates(pathName: Option[String]) = ???
  def toAbstractPatterns(pathName: Option[String]): Seq[AbstractPattern] = ???
}

case class SingleShortestPath(element: PatternElement)(token: InputToken) extends ShortestPath(element)(token) {
  val name: String = "shortestPath"
  val single: Boolean = true
}

case class AllShortestPaths(element: PatternElement)(token: InputToken) extends ShortestPath(element)(token) {
  val name: String = "allShortestPaths"
  val single: Boolean = false
}


sealed abstract class PatternElement extends AstNode {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def toLegacyPatterns(makeOutgoing: Boolean): Seq[commands.Pattern]
  def toLegacyCreates: Seq[mutation.UpdateAction]
  def toAbstractPatterns: Seq[AbstractPattern]
}

case class RelationshipChain(element: PatternElement, relationship: RelationshipPattern, rightNode: NodePattern)(val token: InputToken) extends PatternElement {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck =
    element.declareIdentifiers(ctx) then
    relationship.declareIdentifiers(ctx) then
    rightNode.declareIdentifiers(ctx)

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.semanticCheck(ctx) then
    relationship.semanticCheck(ctx) then
    rightNode.semanticCheck(ctx)

  def toLegacyPatterns(makeOutgoing: Boolean): Seq[commands.Pattern] = {
    val (patterns, leftNode) = element match {
      case node: NodePattern            => (Vector(), node)
      case leftChain: RelationshipChain => (leftChain.toLegacyPatterns(makeOutgoing), leftChain.rightNode)
    }

    patterns :+ relationship.toLegacyPattern(leftNode, rightNode, makeOutgoing)
  }

  lazy val toLegacyCreates: Seq[mutation.CreateRelationship] = {
    val (creates, leftEndpoint) = element match {
      case leftNode: NodePattern        => (Vector(), leftNode.toLegacyEndpoint)
      case leftChain: RelationshipChain =>
        val creates = leftChain.toLegacyCreates
        (creates, leftChain.rightNode.toLegacyEndpoint)
    }

    creates :+ relationship.toLegacyCreates(leftEndpoint, rightNode.toLegacyEndpoint)
  }

  def toAbstractPatterns: Seq[AbstractPattern] = {

    def createParsedRelationship(node: NodePattern): AbstractPattern = {
      val props: Map[String, CommandExpression] = relationship.toLegacyProperties
      val startNode: ParsedEntity = node.toAbstractPatterns.head.asInstanceOf[ParsedEntity]
      val endNode: ParsedEntity = rightNode.toAbstractPatterns.head.asInstanceOf[ParsedEntity]

      val maxDepth = relationship.length match {
        case Some(Some(Range(_, Some(i)))) => Some(i.value.toInt)
        case _                             => None
      }

      val minDepth = relationship.length match {
        case Some(Some(Range(Some(i), _))) => Some(i.value.toInt)
        case _                             => None
      }

      relationship.length match {
        case None =>
          ParsedRelation(
            relationship.legacyName, props, startNode, endNode,
            relationship.types.map(_.name), relationship.direction, relationship.optional)

        case _    =>
          val (relName, relIterator) = relationship.identifier match {
            case Some(_) =>
              ("  UNNAMED" + relationship.token.startPosition.offset, Some(relationship.legacyName))
            case None =>
              (relationship.legacyName, None)
          }

          ParsedVarLengthRelation(relName, props, startNode, endNode, relationship.types.map(_.name),
            relationship.direction, relationship.optional, minDepth, maxDepth, relIterator)
      }
    }

    element match {
      case node: NodePattern      => Seq(createParsedRelationship(node))
      case rel: RelationshipChain => rel.toAbstractPatterns :+ createParsedRelationship(rel.rightNode)
    }
  }
}


case class NodePattern(
  identifier: Option[Identifier],
  labels: Seq[Identifier],
  properties: Option[Expression],
  naked: Boolean)(val token: InputToken) extends PatternElement with SemanticChecking {

  def declareIdentifiers(ctx: SemanticContext): SemanticCheck =
    identifier.fold(SemanticCheckResult.success) {
      identifier =>
        ctx match {
          case SemanticContext.Expression =>
            identifier.ensureDefined() then
            identifier.expectType(CTNode.covariant)
          case _                          =>
            identifier.implicitDeclaration(CTNode)
        }
    }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    checkParens then
    checkProperties(ctx)

  private def checkParens: SemanticCheck =
    when (naked && (!labels.isEmpty || properties.isDefined)) {
      SemanticError("Parentheses are required to identify nodes in patterns", token)
    }

  private def checkProperties(ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), SemanticContext.Match) =>
      SemanticError("Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.token)
    case (Some(e: Parameter), SemanticContext.Merge) =>
      SemanticError("Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.token)
    case _                                           =>
      properties.semanticCheck(Expression.SemanticContext.Simple) then properties.expectType(CTMap.covariant)
  }

  val legacyName: String = identifier.fold("  UNNAMED" + (token.startPosition.offset + 1))(_.name)

  def toLegacyPatterns(makeOutgoing: Boolean) = Seq(toLegacyNode)

  def toLegacyNode = commands.SingleNode(legacyName, labels.map(x => UnresolvedLabel(x.name)), properties = legacyProps)

  def toLegacyCreates = {
    val (_, _, labels) = legacyDetails
    Seq(mutation.CreateNode(legacyName, legacyProps, labels))
  }

  def toLegacyEndpoint: mutation.RelationshipEndpoint = {
    val (nodeExpression, props, labels) = legacyDetails
    mutation.RelationshipEndpoint(nodeExpression, props, labels)
  }

  def toAbstractPatterns: Seq[AbstractPattern] = {
    val (nodeExpression, props, labels) = legacyDetails
    Seq(ParsedEntity(legacyName, nodeExpression, props, labels))
  }

  protected lazy val legacyProps: Map[String, CommandExpression] = properties match {
    case Some(m: MapExpression) => m.items.map(p => (p._1.name, p._2.toCommand)).toMap
    case Some(p: Parameter)     => Map[String, CommandExpression]("*" -> p.toCommand)
    case Some(p)                => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.token.startPosition})")
    case None                   => Map[String, CommandExpression]()
  }

  protected lazy val legacyDetails: (legacy.Expression, Map[String, legacy.Expression], Seq[Unresolved]) = {
    (legacy.Identifier(legacyName), legacyProps, labels.map(t => commandvalues.KeyToken.Unresolved(t.name, commandvalues.TokenType.Label)))
  }
}


case class RelationshipPattern(
    identifier: Option[Identifier],
    optional: Boolean,
    types: Seq[Identifier],
    length: Option[Option[Range]],
    properties: Option[Expression],
    direction: Direction)(val token: InputToken) extends AstNode with SemanticChecking {

  def declareIdentifiers(ctx: SemanticContext): SemanticCheck =
    identifier.fold(SemanticCheckResult.success) {
      identifier =>
        val possibleType = if (length.isEmpty) CTRelationship else CTCollection(CTRelationship)

        ctx match {
          case SemanticContext.Match      => identifier.implicitDeclaration(possibleType)
          case SemanticContext.Expression => identifier.ensureDefined() then identifier.expectType(possibleType.covariant)
          case _                          => identifier.declare(possibleType)
        }
    }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    checkNoOptionalRelsForAnExpression(ctx) then
    checkNoVarLengthWhenUpdating(ctx) then
    checkNoLegacyOptionals(ctx) then
    checkNoParamMapsWhenMatching(ctx) then
    checkProperties(ctx)

  private def checkNoLegacyOptionals(ctx: SemanticContext): SemanticCheck =
    when (optional) {
      SemanticError("Question mark is no longer used for optional patterns - use OPTIONAL MATCH instead", token)
    }

  private def checkNoOptionalRelsForAnExpression(ctx: SemanticContext): SemanticCheck =
    when (ctx == SemanticContext.Expression && optional) {
      SemanticError("Optional relationships cannot be specified in this context", token)
    }

  private def checkNoVarLengthWhenUpdating(ctx: SemanticContext): SemanticCheck =
    when (!isSingleLength) {
      ctx match {
        case SemanticContext.Merge  => SemanticError("Variable length relationships cannot be used in MERGE", token)
        case SemanticContext.Create => SemanticError("Variable length relationships cannot be used in CREATE", token)
        case _                      => None
      }
    }

  private def checkNoParamMapsWhenMatching(ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), SemanticContext.Match) =>
      SemanticError("Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.token)
    case (Some(e: Parameter), SemanticContext.Merge) =>
      SemanticError("Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.token)
    case _                                           =>
      None
  }

  private def checkProperties(ctx: SemanticContext): SemanticCheck =
    properties.semanticCheck(Expression.SemanticContext.Simple) then properties.expectType(CTMap.covariant)

  def isSingleLength = length.fold(true)(_.fold(false)(_.isSingleLength))

  val legacyName: String = identifier.fold("  UNNAMED" + token.startPosition.offset)(_.name)

  def toLegacyPattern(leftNode: NodePattern, rightNode: NodePattern, makeOutgoing: Boolean): commands.Pattern = {
    val (left, right, dir) = if (!makeOutgoing) (leftNode, rightNode, direction)
    else direction match {
      case Direction.OUTGOING                                           => (leftNode, rightNode, direction)
      case Direction.INCOMING                                           => (rightNode, leftNode, Direction.OUTGOING)
      case Direction.BOTH if leftNode.legacyName < rightNode.legacyName => (leftNode, rightNode, direction)
      case Direction.BOTH                                               => (rightNode, leftNode, direction)
    }

    length match {
      case Some(maybeRange) => {
        val pathName = "  UNNAMED" + token.startPosition.offset
        val (min, max) = maybeRange match {
          case Some(range) => (for (i <- range.lower) yield i.value.toInt, for (i <- range.upper) yield i.value.toInt)
          case None        => (None, None)
        }
        val relIterator = identifier.map(_.name)
        commands.VarLengthRelatedTo(pathName, left.toLegacyNode, right.toLegacyNode, min, max,
          types.map(_.name).distinct, dir, relIterator, properties = toLegacyProperties)
      }
      case None             => commands.RelatedTo(left.toLegacyNode, right.toLegacyNode, legacyName,
        types.map(_.name).distinct, dir, toLegacyProperties)
    }
  }

  def toLegacyProperties: Map[String, CommandExpression] = properties match {
    case Some(m: MapExpression) => m.items.map(p => (p._1.name, p._2.toCommand)).toMap
    case Some(p: Parameter)     => Map[String, CommandExpression]("*" -> p.toCommand)
    case Some(p)                => throw new SyntaxException(s"Properties of a node must be a map or parameter (${p.token.startPosition})")
    case None                   => Map[String, CommandExpression]()
  }


  def toLegacyCreates(fromEnd: mutation.RelationshipEndpoint, toEnd: mutation.RelationshipEndpoint): mutation.CreateRelationship = {
    val (from, to) = direction match {
      case Direction.OUTGOING => (fromEnd, toEnd)
      case Direction.INCOMING => (toEnd, fromEnd)
      case Direction.BOTH     => throw new PatternException("Only directed relationships are supported in CREATE, while MATCH allows to ignore direction.")
    }
    val typeName = types match {
      case Seq(i) => i.name
      case _ => throw new SyntaxException(s"A single relationship type must be specified for CREATE (${token.startPosition})")
    }
    mutation.CreateRelationship(legacyName, from, to, typeName, toLegacyProperties)
  }
}
