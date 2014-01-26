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
package org.neo4j.cypher.internal.compiler.v2_1

import symbols._

object Function {
  private val knownFunctions: Seq[Function] = Vector(
    functions.Abs,
    functions.Acos,
    functions.Add,
    functions.And,
    functions.Asin,
    functions.Atan,
    functions.Atan2,
    functions.Avg,
    functions.Ceil,
    functions.Coalesce,
    functions.Collect,
    functions.Ceil,
    functions.Cos,
    functions.Cot,
    functions.Count,
    functions.Degrees,
    functions.Divide,
    functions.E,
    functions.Equals,
    functions.EndNode,
    functions.Exp,
    functions.Floor,
    functions.GreaterThan,
    functions.GreaterThanOrEqual,
    functions.Has,
    functions.Haversin,
    functions.Head,
    functions.Id,
    functions.In,
    functions.InvalidNotEquals,
    functions.IsNotNull,
    functions.IsNull,
    functions.Labels,
    functions.Last,
    functions.Left,
    functions.Length,
    functions.LessThan,
    functions.LessThanOrEqual,
    functions.Log,
    functions.Log10,
    functions.Lower,
    functions.LTrim,
    functions.Max,
    functions.Min,
    functions.Modulo,
    functions.Multiply,
    functions.Nodes,
    functions.Not,
    functions.NotEquals,
    functions.Or,
    functions.Pi,
    functions.PercentileCont,
    functions.PercentileDisc,
    functions.Pow,
    functions.Radians,
    functions.Rand,
    functions.Range,
    functions.Reduce,
    functions.RegularExpression,
    functions.Relationships,
    functions.Rels,
    functions.Replace,
    functions.Right,
    functions.Round,
    functions.RTrim,
    functions.Sign,
    functions.Sin,
    functions.Size,
    functions.Sqrt,
    functions.StartNode,
    functions.StdDev,
    functions.StdDevP,
    functions.Str,
    functions.Substring,
    functions.Subtract,
    functions.Sum,
    functions.Tail,
    functions.Tan,
    functions.Timestamp,
    functions.Trim,
    functions.Type,
    functions.Upper,
    functions.Xor
  )

  val lookup: Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap
}

abstract class Function extends SemanticChecking {
  def name: String

  def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    when(invocation.distinct) {
      SemanticError(s"Invalid use of DISTINCT with function '$name'", invocation.token)
    } then invocation.arguments.semanticCheck(ctx) then semanticCheck(ctx, invocation)

  protected def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck

  protected def checkArgs(invocation: ast.FunctionInvocation, n: Int): Option[SemanticError] =
    Vector(checkMinArgs(invocation, n), checkMaxArgs(invocation, n)).flatten.headOption

  protected def checkMaxArgs(invocation: ast.FunctionInvocation, n: Int): Option[SemanticError] =
    if (invocation.arguments.length > n)
      Some(SemanticError(s"Too many parameters for function '$name'", invocation.token))
    else
      None

  protected def checkMinArgs(invocation: ast.FunctionInvocation, n: Int): Option[SemanticError] =
    if (invocation.arguments.length < n)
      Some(SemanticError(s"Insufficient parameters for function '$name'", invocation.token))
    else
      None
}


trait SimpleTypedFunction { self: Function =>
  case class Signature(argumentTypes: IndexedSeq[CypherType], outputType: CypherType)

  val signatures: Seq[Signature]

  private lazy val signatureLengths = signatures.map(_.argumentTypes.length)

  def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, signatureLengths.min) then checkMaxArgs(invocation, signatureLengths.max) then
    checkTypes(invocation)

  private def checkTypes(invocation: ast.FunctionInvocation): SemanticCheck = s => {
    val initSignatures = signatures.filter(_.argumentTypes.length == invocation.arguments.length)

    val (remainingSignatures: Seq[Signature], result) = invocation.arguments.foldLeft((initSignatures, SemanticCheckResult.success(s))) {
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
    invocation.specifyType(outputType)(result.state) match {
      case Left(err)    => SemanticCheckResult(result.state, result.errors :+ err)
      case Right(state) => SemanticCheckResult(state, result.errors)
    }
  }
}


abstract class AggregatingFunction extends Function {
  override def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    when(ctx == ast.Expression.SemanticContext.Simple) {
      SemanticError(s"Invalid use of aggregating function $name(...) in this context", invocation.token)
    } then invocation.arguments.semanticCheck(ctx) then semanticCheck(ctx, invocation)
}
