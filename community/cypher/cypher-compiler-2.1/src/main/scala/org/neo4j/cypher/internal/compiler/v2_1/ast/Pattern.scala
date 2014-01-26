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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._
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
}

case class RelationshipsPattern(element: RelationshipChain)(val token: InputToken) extends AstNode {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.declareIdentifiers(ctx) then
    element.semanticCheck(ctx)
}


sealed abstract class PatternPart extends AstNode {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck
}

case class NamedPatternPart(identifier: Identifier, patternPart: AnonymousPatternPart)(val token: InputToken) extends PatternPart {
  def declareIdentifiers(ctx: SemanticContext) = patternPart.declareIdentifiers(ctx) then identifier.declare(CTPath)
  def semanticCheck(ctx: SemanticContext) = patternPart.semanticCheck(ctx)
}


sealed trait AnonymousPatternPart extends PatternPart

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
}

case class ShortestPaths(element: PatternElement, single: Boolean)(val token: InputToken) extends AnonymousPatternPart {
  val name: String =
    if (single)
      "shortestPath"
    else
      "allShortestPaths"

  def declareIdentifiers(ctx: SemanticContext) =
    element.declareIdentifiers(ctx)

  def semanticCheck(ctx: SemanticContext) =
    checkContext(ctx) then
    checkContainsSingle then
    checkKnownEnds then
    checkNoMinimalLength then
    element.semanticCheck(ctx)

  private def checkContext(ctx: SemanticContext): SemanticCheck = ctx match {
    case SemanticContext.Merge  => SemanticError(s"$name(...) cannot be used to MERGE", token, element.token)
    case SemanticContext.Create => SemanticError(s"$name(...) cannot be used to CREATE", token, element.token)
    case _                      => None
  }

  private def checkContainsSingle: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      None
    case _                                                    =>
      SemanticError(s"$name(...) requires a pattern containing a single relationship", token, element.token)
  }

  private def checkKnownEnds: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      if (l.identifier.isEmpty)
        SemanticError(s"$name(...) requires named nodes", token, l.token)
      else if (r.identifier.isEmpty)
        SemanticError(s"$name(...) requires named nodes", token, r.token)
      else
        None
    case _                                                    =>
      None
  }

  private def checkNoMinimalLength: SemanticCheck = element match {
    case RelationshipChain(_, rel, _) =>
      rel.length match {
        case Some(Some(Range(Some(_), _))) =>
          Some(SemanticError(s"$name(...) does not support a minimal length", token, element.token))
        case _                             =>
          None
      }
    case _                            =>
      None
  }
}


sealed abstract class PatternElement extends AstNode {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck
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
}
