/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

  val DefaultTypeMismatchMessageGenerator = (expected: String, existing: String) => s"expected $expected but was $existing"

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
    def unionOfTypes: TypeGenerator = state =>
      TypeSpec.union(traversable.map(_.types(state)).toSeq: _*)

    def leastUpperBoundsOfTypes: TypeGenerator =
      if (traversable.isEmpty)
        _ => CTAny.invariant
      else
        state => traversable.map { _.types(state) } reduce { _ leastUpperBounds _ }

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
  def expectType(typeGen: TypeGenerator, messageGen: (String, String) => String): SemanticState => SemanticCheckResult =
    s => expectType(typeGen(s), messageGen)(s)

  def expectType(possibleTypes: => TypeSpec, messageGen: (String, String) => String = DefaultTypeMismatchMessageGenerator): SemanticState => SemanticCheckResult = s => {
    s.expectType(this, possibleTypes) match {
      case (ss, TypeSpec.none) =>
        val existingTypesString = ss.expressionType(this).specified.mkString(", ", " or ")
        val expectedTypesString = possibleTypes.mkString(", ", " or ")
        SemanticCheckResult.error(ss, SemanticError("Type mismatch: " + messageGen(expectedTypesString, existingTypesString), position))
      case (ss, _)             =>
        SemanticCheckResult.success(ss)
    }
  }
}

trait SimpleTyping { self: Expression =>
  protected def possibleTypes: TypeSpec
  def semanticCheck(ctx: SemanticContext): SemanticCheck = specifyType(possibleTypes)
}

trait FunctionTyping { self: Expression =>
  def arguments: IndexedSeq[Expression]

  case class Signature(argumentTypes: IndexedSeq[CypherType], outputType: CypherType)

  def signatures: Seq[Signature]

  def semanticCheck(ctx: ast.Expression.SemanticContext): SemanticCheck =
    arguments.semanticCheck(ctx) then
    checkTypes

  def checkTypes: SemanticCheck = s => {
    val initSignatures = signatures.filter(_.argumentTypes.length == arguments.length)

    val (remainingSignatures: Seq[Signature], result) = arguments.foldLeft((initSignatures, SemanticCheckResult.success(s))) {
      case (accumulator@(Seq(), _), _) =>
        accumulator
      case ((possibilities, r1), arg)  =>
        val argTypes = possibilities.foldLeft(TypeSpec.none) { _ | _.argumentTypes.head.covariant }
        val r2 = arg.expectType(argTypes)(r1.state)

        val actualTypes = arg.types(r2.state)
        val remainingPossibilities = possibilities.filter {
          sig => actualTypes containsAny sig.argumentTypes.head.covariant
        } map {
          sig => sig.copy(argumentTypes = sig.argumentTypes.tail)
        }
        (remainingPossibilities, SemanticCheckResult(r2.state, r1.errors ++ r2.errors))
    }

    val outputType = remainingSignatures match {
      case Seq() => TypeSpec.all
      case _     => remainingSignatures.foldLeft(TypeSpec.none) { _ | _.outputType.invariant }
    }
    specifyType(outputType)(result.state) match {
      case Left(err)    => SemanticCheckResult(result.state, result.errors :+ err)
      case Right(state) => SemanticCheckResult(state, result.errors)
    }
  }
}

trait PrefixFunctionTyping extends FunctionTyping { self: Expression =>
  def rhs: Expression
  val arguments = Vector(rhs)
}

trait PostfixFunctionTyping extends FunctionTyping { self: Expression =>
  def lhs: Expression
  val arguments = Vector(lhs)
}

trait InfixFunctionTyping extends FunctionTyping { self: Expression =>
  def lhs: Expression
  def rhs: Expression
  val arguments = Vector(lhs, rhs)
}
