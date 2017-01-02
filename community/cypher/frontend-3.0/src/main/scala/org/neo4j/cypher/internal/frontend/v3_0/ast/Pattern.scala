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
package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.symbols._
import org.neo4j.cypher.internal.frontend.v3_0.notification.UnboundedShortestPathNotification

object Pattern {
  sealed trait SemanticContext

  object SemanticContext {
    case object Match extends SemanticContext
    case object Merge extends SemanticContext
    case object Create extends SemanticContext
    case object CreateUnique extends SemanticContext
    case object Expression extends SemanticContext
  }

  object findDuplicateRelationships extends (Pattern => Set[Seq[Variable]]) {

    def apply(pattern: Pattern): Set[Seq[Variable]] = {
      val (seen, duplicates) = pattern.fold((Set.empty[Variable], Seq.empty[Variable])) {
        case RelationshipChain(_, RelationshipPattern(Some(rel), _, _, None, _, _), _) =>
          (acc) =>
            val (seen, duplicates) = acc

            val newDuplicates = if (seen.contains(rel)) duplicates :+ rel else duplicates
            val newSeen = seen + rel

            (newSeen, newDuplicates)

        case _ =>
          identity
      }

      val m0: Map[String, Seq[Variable]] = duplicates.groupBy(_.name)

      val resultMap = seen.foldLeft(m0) {
        case (m, ident @ Variable(name)) if m.contains(name) => m.updated(name, Seq(ident) ++ m(name))
        case (m, _)                                            => m
      }

      resultMap.values.toSet
    }
  }
}

import org.neo4j.cypher.internal.frontend.v3_0.ast.Pattern._

case class Pattern(patternParts: Seq[PatternPart])(val position: InputPosition) extends ASTNode with ASTParticle {

  lazy val length = this.fold(0) {
    case RelationshipChain(_, _, _) => _ + 1
    case _ => identity
  }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    patternParts.foldSemanticCheck(_.declareVariables(ctx)) chain
    patternParts.foldSemanticCheck(_.semanticCheck(ctx)) chain
    ensureNoDuplicateRelationships(this, ctx)

  private def ensureNoDuplicateRelationships(pattern: Pattern, ctx: SemanticContext): SemanticCheck = {
    findDuplicateRelationships(pattern).foldLeft(SemanticCheckResult.success) {
      (acc, duplicates) =>
        val id = duplicates.head
        val dups = duplicates.tail

        acc chain SemanticError(s"Cannot use the same relationship variable '${id.name}' for multiple patterns", id.position, dups.map(_.position):_*)
    }
  }
}

case class RelationshipsPattern(element: RelationshipChain)(val position: InputPosition) extends ASTNode with ASTParticle {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.declareVariables(ctx) chain
    element.semanticCheck(ctx)
}


sealed abstract class PatternPart extends ASTNode with ASTParticle {
  def declareVariables(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def element: PatternElement
}

case class NamedPatternPart(variable: Variable, patternPart: AnonymousPatternPart)(val position: InputPosition) extends PatternPart {
  def declareVariables(ctx: SemanticContext) = patternPart.declareVariables(ctx) chain variable.declare(CTPath)
  def semanticCheck(ctx: SemanticContext) = patternPart.semanticCheck(ctx)

  def element: PatternElement = patternPart.element
}


sealed trait AnonymousPatternPart extends PatternPart

case class EveryPath(element: PatternElement) extends AnonymousPatternPart {
  def position = element.position

  def declareVariables(ctx: SemanticContext) = (element, ctx) match {
    case (n: NodePattern, SemanticContext.Match) =>
      element.declareVariables(ctx) // single node variable is allowed to be already bound in MATCH
    case (n: NodePattern, _)                     =>
      n.variable.fold(SemanticCheckResult.success)(_.declare(CTNode)) chain element.declareVariables(ctx)
    case _                                       =>
      element.declareVariables(ctx)
  }

  def semanticCheck(ctx: SemanticContext) = element.semanticCheck(ctx)
}

case class ShortestPaths(element: PatternElement, single: Boolean)(val position: InputPosition) extends AnonymousPatternPart {
  val name: String =
    if (single)
      "shortestPath"
    else
      "allShortestPaths"

  def declareVariables(ctx: SemanticContext) =
    element.declareVariables(ctx)

  def semanticCheck(ctx: SemanticContext) =
    checkContext(ctx) chain
    checkContainsSingle chain
    checkKnownEnds chain
    checkLength chain
    checkRelVariablesUnknown chain
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
    case RelationshipChain(_: NodePattern, r, _: NodePattern) =>
      r.properties.map { props =>
        SemanticError(s"$name(...) contains properties $props. This is currently not supported.", position, element.position)
      }
    case _                                                    =>
      SemanticError(s"$name(...) requires a pattern containing a single relationship", position, element.position)
  }

  private def checkKnownEnds: SemanticCheck = element match {
    case RelationshipChain(l: NodePattern, _, r: NodePattern) =>
      if (l.variable.isEmpty)
        SemanticError(s"$name(...) requires named nodes", position, l.position)
      else if (r.variable.isEmpty)
        SemanticError(s"$name(...) requires named nodes", position, r.position)
      else
        None
    case _                                                    =>
      None
  }

  private def checkLength: SemanticCheck = (state: SemanticState) => element match {
    case RelationshipChain(_, rel, _) =>
      rel.length match {
        case Some(Some(Range(Some(min), _))) if min.value < 0 || min.value > 1 =>
          SemanticCheckResult(state, Seq(SemanticError(s"$name(...) does not support a minimal length different from 0 or 1", position, element.position)))

        case Some(None) =>
          val newState = state.addNotification(UnboundedShortestPathNotification(element.position))
          SemanticCheckResult(newState, Seq.empty)
        case _ => SemanticCheckResult(state, Seq.empty)
      }
    case _ => SemanticCheckResult(state, Seq.empty)
  }

  private def checkRelVariablesUnknown: SemanticCheck = state => {
    element match {
      case RelationshipChain(_, rel, _) =>
        rel.variable.flatMap(id => state.symbol(id.name)) match {
          case Some(symbol) if symbol.positions.size > 1 => {
            SemanticCheckResult.error(state, SemanticError(s"Bound relationships not allowed in $name(...)", rel.position, symbol.positions.head))
          }
          case _ =>
            SemanticCheckResult.success(state)
        }
      case _ =>
        SemanticCheckResult.success(state)
    }
  }
}

sealed abstract class PatternElement extends ASTNode with ASTParticle {
  def variable: Option[Variable]
  def declareVariables(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def isSingleNode = false
}

case class RelationshipChain(element: PatternElement, relationship: RelationshipPattern, rightNode: NodePattern)(val position: InputPosition)
  extends PatternElement {

  def variable: Option[Variable] = relationship.variable

  def declareVariables(ctx: SemanticContext): SemanticCheck =
    element.declareVariables(ctx) chain
    relationship.declareVariables(ctx) chain
    rightNode.declareVariables(ctx)

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.semanticCheck(ctx) chain
    relationship.semanticCheck(ctx) chain
    rightNode.semanticCheck(ctx)
}

object InvalidNodePattern {
  def apply(id: Variable, labels: Seq[LabelName], properties: Option[Expression])(position: InputPosition) =
    new InvalidNodePattern(id)(position)
}

class InvalidNodePattern(val id: Variable)(position: InputPosition) extends NodePattern(Some(id), Seq.empty, None)(position) {
  override def semanticCheck(ctx: SemanticContext): SemanticCheck = super.semanticCheck(ctx) chain
    SemanticError(s"Parentheses are required to identify nodes in patterns, i.e. (${id.name})", position)

  override def canEqual(other: Any): Boolean = other.isInstanceOf[InvalidNodePattern]

  override def equals(other: Any): Boolean = other match {
    case that: InvalidNodePattern =>
      (that canEqual this) &&
        id == that.id
    case _ => false
  }

  override def hashCode(): Int = 31 * id.hashCode()
}

case class NodePattern(
  variable: Option[Variable],
  labels: Seq[LabelName],
  properties: Option[Expression])(val position: InputPosition) extends PatternElement with SemanticChecking {

  def declareVariables(ctx: SemanticContext): SemanticCheck =
    variable.fold(SemanticCheckResult.success) {
      variable =>
        ctx match {
          case SemanticContext.Expression =>
            variable.ensureDefined() chain
            variable.expectType(CTNode.covariant)
          case _                          =>
            variable.implicitDeclaration(CTNode)
        }
    }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    checkProperties(ctx)

  override def isSingleNode = true

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
    variable: Option[Variable],
    optional: Boolean,
    types: Seq[RelTypeName],
    length: Option[Option[Range]],
    properties: Option[Expression],
    direction: SemanticDirection)(val position: InputPosition) extends ASTNode with ASTParticle with SemanticChecking {

  def declareVariables(ctx: SemanticContext): SemanticCheck =
    variable.fold(SemanticCheckResult.success) {
      variable =>
        val possibleType = if (length.isEmpty) CTRelationship else CTList(CTRelationship)

        ctx match {
          case SemanticContext.Match      => variable.implicitDeclaration(possibleType)
          case SemanticContext.Expression => variable.ensureDefined() chain variable.expectType(possibleType.covariant)
          case _                          => variable.declare(possibleType)
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
      case SemanticContext.Create if direction == SemanticDirection.BOTH =>
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

  def isSingleLength = length.isEmpty

  def isDirected = direction != SemanticDirection.BOTH
}
