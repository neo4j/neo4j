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
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

object Expression {
  sealed trait SemanticContext
  object SemanticContext {
    case object Simple extends SemanticContext
    case object Results extends SemanticContext
  }

  implicit class SemanticCheckableOption[A <: Expression](option: Option[A]) {
    def semanticCheck(ctx: SemanticContext): SemanticCheck =
      option.fold(SemanticCheckResult.success) { _.semanticCheck(ctx) }

    def expectType(possibleTypes: => TypeSpec): SemanticCheck =
      option.fold(SemanticCheckResult.success) { _.expectType(possibleTypes) }
  }

  implicit class SemanticCheckableExpressionTraversable[A <: Expression](traversable: TraversableOnce[A]) extends SemanticChecking {
    def semanticCheck(ctx: SemanticContext): SemanticCheck =
      traversable.foldSemanticCheck { _.semanticCheck(ctx) }
  }

  implicit class InferrableTypeTraversableOnce[A <: Expression](traversable: TraversableOnce[A]) {
    def mergeUpTypes: TypeGenerator =
      if (traversable.isEmpty)
        _ => CTAny.invariant
      else
        (state: SemanticState) => traversable.map { _.types(state) } reduce { _ mergeUp _ }

    def expectType(possibleTypes: => TypeSpec): SemanticCheck =
      traversable.foldSemanticCheck { _.expectType(possibleTypes) }
  }
}

import Expression._

abstract class Expression extends ASTNode with SemanticChecking {
  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def types: TypeGenerator = s => s.expressionType(this).actual

  def specifyType(typeGen: TypeGenerator): SemanticState => Either[SemanticError, SemanticState] =
    s => specifyType(typeGen(s))(s)
  def specifyType(possibleTypes: => TypeSpec): SemanticState => Either[SemanticError, SemanticState] =
    _.specifyType(this, possibleTypes)

  def expectType(typeGen: TypeGenerator): SemanticState => SemanticCheckResult =
    s => expectType(typeGen(s))(s)
  def expectType(possibleTypes: => TypeSpec): SemanticState => SemanticCheckResult = s => {
    s.expectType(this, possibleTypes) match {
      case (ss, TypeSpec.none) =>
        val existingTypesString = ss.expressionType(this).specified.mkString(", ", " or ")
        val expectedTypesString = possibleTypes.mkString(", ", " or ")
        SemanticCheckResult.error(ss, SemanticError(s"Type mismatch: expected $expectedTypesString but was $existingTypesString", position))
      case (ss, _)             =>
        SemanticCheckResult.success(ss)
    }
  }
}

trait SimpleTypedExpression { self: Expression =>
  protected def possibleTypes: TypeSpec
  def semanticCheck(ctx: SemanticContext): SemanticCheck = specifyType(possibleTypes)
}
