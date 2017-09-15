/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.symbols._
import org.neo4j.cypher.internal.frontend.v3_4.notification.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.frontend.v3_4.semantics._

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
        case RelationshipChain(_, RelationshipPattern(Some(rel), _, None, _, _, _), _) =>
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

import org.neo4j.cypher.internal.frontend.v3_4.ast.Pattern._

case class Pattern(patternParts: Seq[PatternPart])(val position: InputPosition) extends ASTNode {

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

case class RelationshipsPattern(element: RelationshipChain)(val position: InputPosition) extends ASTNode {
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    element.declareVariables(ctx) chain
      element.semanticCheck(ctx)
}


sealed abstract class PatternPart extends ASTNode {
  def declareVariables(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def element: PatternElement
}

case class NamedPatternPart(variable: Variable, patternPart: AnonymousPatternPart)(val position: InputPosition) extends PatternPart {
  def declareVariables(ctx: SemanticContext) = patternPart.declareVariables(ctx) chain variable.declareVariable(CTPath)
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
      n.variable.fold(SemanticCheckResult.success)(_.declareVariable(CTNode)) chain element.declareVariables(ctx)
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

sealed abstract class PatternElement extends ASTNode {
  def allVariables: Set[Variable]
  def variable: Option[Variable]
  def declareVariables(ctx: SemanticContext): SemanticCheck
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def isSingleNode = false
}

case class RelationshipChain(element: PatternElement, relationship: RelationshipPattern, rightNode: NodePattern)(val position: InputPosition)
  extends PatternElement {

  def variable: Option[Variable] = relationship.variable

  override def allVariables: Set[Variable] = element.allVariables ++ relationship.variable ++ rightNode.variable

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

  override def allVariables: Set[Variable] = Set.empty
}

case class NodePattern(variable: Option[Variable],
                       labels: Seq[LabelName],
                       properties: Option[Expression])(val position: InputPosition)
  extends PatternElement with SemanticChecking {

  def declareVariables(ctx: SemanticContext): SemanticCheck =
    variable.fold(SemanticCheckResult.success) {
      variable =>
        ctx match {
          case SemanticContext.Expression =>
            variable.ensureVariableDefined() chain
              SemanticAnalysis.expectType(CTNode.covariant, variable)
          case _                          =>
            variable.implicitVariable(CTNode)
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
      SemanticAnalysis.semanticCheck(Expression.SemanticContext.Simple, properties) chain
        SemanticAnalysis.expectType(CTMap.covariant, properties)
  }

  override def allVariables: Set[Variable] = variable.toSet
}


case class RelationshipPattern(
                                variable: Option[Variable],
                                types: Seq[RelTypeName],
                                length: Option[Option[Range]],
                                properties: Option[Expression],
                                direction: SemanticDirection,
                                legacyTypeSeparator: Boolean = false)(val position: InputPosition) extends ASTNode with SemanticChecking {

  def declareVariables(ctx: SemanticContext): SemanticCheck =
    variable.fold(SemanticCheckResult.success) {
      variable =>
        val possibleType = if (length.isEmpty) CTRelationship else CTList(CTRelationship)

        ctx match {
          case SemanticContext.Match      => variable.implicitVariable(possibleType)
          case SemanticContext.Expression => variable.ensureVariableDefined() chain
                                              SemanticAnalysis.expectType(possibleType.covariant, variable)
          case _                          => variable.declareVariable(possibleType)
        }
    }

  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    checkNoVarLengthWhenUpdating(ctx) chain
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

  private def checkNoVarLengthWhenUpdating(ctx: SemanticContext): SemanticCheck =
    SemanticAnalysis.when (!isSingleLength) {
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
    SemanticAnalysis.semanticCheck(Expression.SemanticContext.Simple, properties) chain
      SemanticAnalysis.expectType(CTMap.covariant, properties)

  def isSingleLength = length.isEmpty

  def isDirected = direction != SemanticDirection.BOTH
}
