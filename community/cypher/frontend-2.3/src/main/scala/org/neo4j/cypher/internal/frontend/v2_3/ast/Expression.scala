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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.Foldable._
import org.neo4j.cypher.internal.frontend.v2_3.{ast, _}
import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression._
import org.neo4j.cypher.internal.frontend.v2_3.symbols.{CypherType, TypeSpec, _}

import scala.collection.immutable.Stack

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

  final case class TreeAcc[A](data: A, stack: Stack[Set[Identifier]] = Stack.empty) {
    def toSet: Set[Identifier] = stack.toSet.flatten
    def map(f: A => A): TreeAcc[A] = copy(data = f(data))
    def push(newIdentifier: Identifier): TreeAcc[A] = push(Set(newIdentifier))
    def push(newIdentifiers: Set[Identifier]): TreeAcc[A] = copy(stack = stack.push(newIdentifiers))
    def pop: TreeAcc[A] = copy(stack = stack.pop)
    def contains(identifier: Identifier) = stack.exists(_.contains(identifier))
  }
}

abstract class Expression extends ASTNode with ASTExpression with SemanticChecking {

  import Expression.TreeAcc

  def semanticCheck(ctx: SemanticContext): SemanticCheck

  def types: TypeGenerator = s => s.expressionType(this).actual

  def arguments: Seq[Expression] = this.treeFold(List.empty[Expression]) {
    case e: Expression if e != this =>
      (acc, _) => acc :+ e
  }

  // All identifiers referenced from this expression or any of its childs
  // that are not introduced inside this expression
  def dependencies: Set[Identifier] =
    this.treeFold(TreeAcc[Set[Identifier]](Set.empty)) {
      case scope: ScopeExpression => {
        case (acc, children) =>
          val newAcc = acc.push(scope.identifiers)
          children(newAcc).pop
      }
      case id: Identifier => {
        case (acc, children) if acc.contains(id) =>
          children(acc)
        case (acc, children) =>
          children(acc.map(_ + id))
      }
    }.data

  // List of child expressions together with any of its dependencies introduced
  // by any of its parent expressions (where this expression is the root of the tree)
  def inputs: Seq[(Expression, Set[Identifier])] =
    this.treeFold(TreeAcc[Seq[(Expression, Set[Identifier])]](Seq.empty)) {
      case scope: ScopeExpression=> {
        case (acc, children) =>
          val newAcc = acc.push(scope.identifiers).map { case pairs => pairs :+ (scope -> acc.toSet) }
          children(newAcc).pop
      }

      case expr: Expression => {
        case (acc, children) =>
          val newAcc = acc.map { case pairs => pairs :+ (expr -> acc.toSet) }
          children(newAcc)
      }
    }.data

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

  def typeSwitch(choice: TypeSpec => SemanticCheck): SemanticCheck =
    (state: SemanticState) => choice(types(state))(state)

  def containsAggregate = this.exists {
    case IsAggregate(_) => true
  }
}

trait SimpleTyping { self: Expression =>
  protected def possibleTypes: TypeSpec
  def semanticCheck(ctx: SemanticContext): SemanticCheck = specifyType(possibleTypes)
}

trait FunctionTyping { self: Expression =>

  case class Signature(argumentTypes: IndexedSeq[CypherType], outputType: CypherType)

  def signatures: Seq[Signature]

  def semanticCheck(ctx: ast.Expression.SemanticContext): SemanticCheck =
    arguments.semanticCheck(ctx) chain
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
}

trait PostfixFunctionTyping extends FunctionTyping { self: Expression =>
  def lhs: Expression
}

trait InfixFunctionTyping extends FunctionTyping { self: Expression =>
  def lhs: Expression
  def rhs: Expression
}
