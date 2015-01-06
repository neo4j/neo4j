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
import org.neo4j.cypher.InternalException

object Pattern {
  sealed trait SemanticContext

  object SemanticContext {
    case object Match extends SemanticContext
    case object Merge extends SemanticContext
    case object Create extends SemanticContext
    case object CreateUnique extends SemanticContext
    case object Expression extends SemanticContext
  }

  object findDuplicateRelationships extends (Pattern => Set[Seq[Identifier]]) {
    import Foldable._

    def apply(pattern: Pattern): Set[Seq[Identifier]] = {
      val (seen, duplicates) = pattern.fold((Set.empty[Identifier], Seq.empty[Identifier])) {
        case RelationshipChain(_, RelationshipPattern(Some(rel), _, _, None, _, _), _) =>
          (acc) =>
            val (seen, duplicates) = acc

            val newDuplicates = if (seen.contains(rel)) duplicates :+ rel else duplicates
            val newSeen = seen + rel

            (newSeen, newDuplicates)

        case _ =>
          identity
      }

      val m0: Map[String, Seq[Identifier]] = duplicates.groupBy(_.name)

      val resultMap = seen.foldLeft(m0) {
        case (m, ident @ Identifier(name)) if m.contains(name) => m.updated(name, Seq(ident) ++ m(name))
        case (m, _)                                            => m
      }

      resultMap.values.toSet
    }
  }
}

import Pattern._

case class Pattern(patternParts: Seq[PatternPart])(val position: InputPosition) extends ASTNode {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    patternParts.foldSemanticCheck(_.declareIdentifiers(ctx)) chain
    patternParts.foldSemanticCheck(_.semanticCheck(ctx)) chain
    ensureNoDuplicateRelationships(this, ctx)

  private def ensureNoDuplicateRelationships(pattern: Pattern, ctx: SemanticContext): SemanticCheck = {
    findDuplicateRelationships(pattern).foldLeft(SemanticCheckResult.success) {
      (acc, duplicates) =>
        val id = duplicates.head
        val dups = duplicates.tail

        acc chain SemanticError(s"Cannot use the same relationship identifier '${id.name}' for multiple patterns", id.position, dups.map(_.position):_*)
    }
  }
}

case class RelationshipsPattern(element: RelationshipChain)(val position: InputPosition) extends ASTNode {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.declareIdentifiers(ctx) chain
    element.semanticCheck(ctx)
}


sealed abstract class PatternPart extends ASTNode {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck
}

case class NamedPatternPart(identifier: Identifier, patternPart: AnonymousPatternPart)(val position: InputPosition) extends PatternPart {
  def declareIdentifiers(ctx: SemanticContext) = patternPart.declareIdentifiers(ctx) chain identifier.declare(CTPath)
  def semanticCheck(ctx: SemanticContext) = patternPart.semanticCheck(ctx)
}


sealed trait AnonymousPatternPart extends PatternPart

case class EveryPath(element: PatternElement) extends AnonymousPatternPart {
  def position = element.position

  def declareIdentifiers(ctx: SemanticContext) = (element, ctx) match {
    case (n: NodePattern, SemanticContext.Match) =>
      element.declareIdentifiers(ctx) // single node identifier is allowed to be already bound in MATCH
    case (n: NodePattern, _)                     =>
      n.identifier.fold(SemanticCheckResult.success)(_.declare(CTNode)) chain element.declareIdentifiers(ctx)
    case _                                       =>
      element.declareIdentifiers(ctx)
  }

  def semanticCheck(ctx: SemanticContext) = element.semanticCheck(ctx)
}

case class ShortestPaths(element: PatternElement, single: Boolean)(val position: InputPosition) extends AnonymousPatternPart {
  val name: String =
    if (single)
      "shortestPath"
    else
      "allShortestPaths"

  def declareIdentifiers(ctx: SemanticContext) =
    element.declareIdentifiers(ctx)

  def semanticCheck(ctx: SemanticContext) =
    checkContext(ctx) chain
    checkContainsSingle chain
    checkKnownEnds chain
    checkNoMinimalLength chain
    element.semanticCheck(ctx)

  private def checkContext(ctx: SemanticContext): SemanticCheck = ctx match {
    case SemanticContext.Merge =>
      SemanticError(s"$name(...) cannot be used to MERGE", position, element.position)
    case SemanticContext.Create | SemanticContext.CreateUnique =>
      SemanticError(s"$name(...) cannot be used to CREATE", position, element.position)
    case _ =>
      None
  }

  private def checkContainsSingle: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      None
    case _                                                    =>
      SemanticError(s"$name(...) requires a pattern containing a single relationship", position, element.position)
  }

  private def checkKnownEnds: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      if (l.identifier.isEmpty)
        SemanticError(s"$name(...) requires named nodes", position, l.position)
      else if (r.identifier.isEmpty)
        SemanticError(s"$name(...) requires named nodes", position, r.position)
      else
        None
    case _                                                    =>
      None
  }

  private def checkNoMinimalLength: SemanticCheck = element match {
    case RelationshipChain(_, rel, _) =>
      rel.length match {
        case Some(Some(Range(Some(_), _))) =>
          Some(SemanticError(s"$name(...) does not support a minimal length", position, element.position))
        case _ =>
          None
      }
    case _ =>
      None
  }
}


sealed abstract class PatternElement extends ASTNode {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck
}

case class RelationshipChain(element: PatternElement, relationship: RelationshipPattern, rightNode: NodePattern)(val position: InputPosition) extends PatternElement {
  def declareIdentifiers(ctx: SemanticContext): SemanticCheck =
    element.declareIdentifiers(ctx) chain
    relationship.declareIdentifiers(ctx) chain
    rightNode.declareIdentifiers(ctx)

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.semanticCheck(ctx) chain
    relationship.semanticCheck(ctx) chain
    rightNode.semanticCheck(ctx)
}


case class NodePattern(
  identifier: Option[Identifier],
  labels: Seq[LabelName],
  properties: Option[Expression],
  naked: Boolean)(val position: InputPosition) extends PatternElement with SemanticChecking {

  def declareIdentifiers(ctx: SemanticContext): SemanticCheck =
    identifier.fold(SemanticCheckResult.success) {
      identifier =>
        ctx match {
          case SemanticContext.Expression =>
            identifier.ensureDefined() chain
            identifier.expectType(CTNode.covariant)
          case _                          =>
            identifier.implicitDeclaration(CTNode)
        }
    }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    checkParens chain
    checkProperties(ctx)

  private def checkParens: SemanticCheck =
    when (naked && (!labels.isEmpty || properties.isDefined)) {
      SemanticError("Parentheses are required to identify nodes in patterns", position)
    }

  private def checkProperties(ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), SemanticContext.Match) =>
      SemanticError("Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case (Some(e: Parameter), SemanticContext.Merge) =>
      SemanticError("Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case _                                           =>
      properties.semanticCheck(Expression.SemanticContext.Simple) chain properties.expectType(CTMap.covariant)
  }
}


case class RelationshipPattern(
    identifier: Option[Identifier],
    optional: Boolean,
    types: Seq[RelTypeName],
    length: Option[Option[Range]],
    properties: Option[Expression],
    direction: Direction)(val position: InputPosition) extends ASTNode with SemanticChecking {

  def declareIdentifiers(ctx: SemanticContext): SemanticCheck =
    identifier.fold(SemanticCheckResult.success) {
      identifier =>
        val possibleType = if (length.isEmpty) CTRelationship else CTCollection(CTRelationship)

        ctx match {
          case SemanticContext.Match      => identifier.implicitDeclaration(possibleType)
          case SemanticContext.Expression => identifier.ensureDefined() chain identifier.expectType(possibleType.covariant)
          case _                          => identifier.declare(possibleType)
        }
    }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    checkNoOptionalRelsForAnExpression(ctx) chain
    checkNoVarLengthWhenUpdating(ctx) chain
    checkNoLegacyOptionals(ctx) chain
    checkNoParamMapsWhenMatching(ctx) chain
    checkProperties(ctx) chain
    checkNotUndirectedWhenCreating(ctx)

  private def checkNotUndirectedWhenCreating(ctx: SemanticContext): SemanticCheck = {
    ctx match {
      case SemanticContext.Create if direction == Direction.BOTH =>
        SemanticError("Only directed relationships are supported in CREATE", position)
      case _ =>
        SemanticCheckResult.success
    }
  }

  private def checkNoLegacyOptionals(ctx: SemanticContext): SemanticCheck =
    when (optional) {
      SemanticError("Question mark is no longer used for optional patterns - use OPTIONAL MATCH instead", position)
    }

  private def checkNoOptionalRelsForAnExpression(ctx: SemanticContext): SemanticCheck =
    when (ctx == SemanticContext.Expression && optional) {
      SemanticError("Optional relationships cannot be specified in this context", position)
    }

  private def checkNoVarLengthWhenUpdating(ctx: SemanticContext): SemanticCheck =
    when (!isSingleLength) {
      ctx match {
        case SemanticContext.Merge =>
          SemanticError("Variable length relationships cannot be used in MERGE", position)
        case SemanticContext.Create | SemanticContext.CreateUnique =>
          SemanticError("Variable length relationships cannot be used in CREATE", position)
        case _                          => None
      }
    }

  private def checkNoParamMapsWhenMatching(ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), SemanticContext.Match) =>
      SemanticError("Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case (Some(e: Parameter), SemanticContext.Merge) =>
      SemanticError("Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case _                                           =>
      None
  }

  private def checkProperties(ctx: SemanticContext): SemanticCheck =
    properties.semanticCheck(Expression.SemanticContext.Simple) chain properties.expectType(CTMap.covariant)

  def isSingleLength = length.fold(true)(_.fold(false)(_.isSingleLength))
}
