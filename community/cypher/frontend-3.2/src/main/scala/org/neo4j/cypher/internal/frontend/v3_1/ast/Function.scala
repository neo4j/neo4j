/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_1.ast

import org.neo4j.cypher.internal.frontend.v3_1._
import org.neo4j.cypher.internal.frontend.v3_1.symbols.{CypherType, TypeSpec}

object Function {
  private val knownFunctions: Seq[Function] = Vector(
    functions.Abs,
    functions.Acos,
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
    functions.Distance,
    functions.E,
    functions.EndNode,
    functions.Exists,
    functions.Exp,
    functions.Floor,
    functions.Has,
    functions.Haversin,
    functions.Head,
    functions.Id,
    functions.Labels,
    functions.Last,
    functions.Left,
    functions.Length,
    functions.Log,
    functions.Log10,
    functions.LTrim,
    functions.Max,
    functions.Min,
    functions.Nodes,
    functions.Pi,
    functions.PercentileCont,
    functions.PercentileDisc,
    functions.Point,
    functions.Keys,
    functions.Radians,
    functions.Rand,
    functions.Range,
    functions.Reduce,
    functions.Relationships,
    functions.Replace,
    functions.Reverse,
    functions.Right,
    functions.Round,
    functions.RTrim,
    functions.Sign,
    functions.Sin,
    functions.Size,
    functions.Sqrt,
    functions.Split,
    functions.StartNode,
    functions.StdDev,
    functions.StdDevP,
    functions.Substring,
    functions.Sum,
    functions.Tail,
    functions.Tan,
    functions.Timestamp,
    functions.ToBoolean,
    functions.ToFloat,
    functions.ToInteger,
    functions.ToLower,
    functions.ToString,
    functions.ToUpper,
    functions.Properties,
    functions.Trim,
    functions.Type
  )

  val lookup: Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap
}

abstract class Function extends SemanticChecking {
  def name: String

  def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    when(invocation.distinct) {
      SemanticError(s"Invalid use of DISTINCT with function '$name'", invocation.position)
    } chain invocation.arguments.semanticCheck(ctx) chain semanticCheck(ctx, invocation)

  protected def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck

  protected def checkArgs(invocation: ast.FunctionInvocation, n: Int): Option[SemanticError] =
    Vector(checkMinArgs(invocation, n), checkMaxArgs(invocation, n)).flatten.headOption

  protected def checkMaxArgs(invocation: ast.FunctionInvocation, n: Int): Option[SemanticError] =
    if (invocation.arguments.length > n)
      Some(SemanticError(s"Too many parameters for function '$name'", invocation.position))
    else
      None

  protected def checkMinArgs(invocation: ast.FunctionInvocation, n: Int): Option[SemanticError] =
    if (invocation.arguments.length < n)
      Some(SemanticError(s"Insufficient parameters for function '$name'", invocation.position))
    else
      None

  def asFunctionName(implicit position: InputPosition) = FunctionName(name)(position)

  def asInvocation(argument: ast.Expression, distinct: Boolean = false)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = distinct, IndexedSeq(argument))(position)

  def asInvocation(lhs: ast.Expression, rhs: ast.Expression)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = false, IndexedSeq(lhs, rhs))(position)
}

trait SimpleTypedFunction extends ExpressionCallTypeChecking {
  self: Function =>

  override def semanticCheck(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    checkMinArgs(invocation, signatureLengths.min) chain
    checkMaxArgs(invocation, signatureLengths.max) chain
    typeChecker.checkTypes(invocation)
}

abstract class AggregatingFunction extends Function {
  override def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    when(ctx == ast.Expression.SemanticContext.Simple) {
      SemanticError(s"Invalid use of aggregating function $name(...) in this context", invocation.position)
    } chain invocation.arguments.semanticCheck(ctx) chain semanticCheck(ctx, invocation)


  /*
   * Checks so that the expression is in the range [min, max]
   */
  protected def checkPercentileRange(expression: Expression): SemanticCheck = {
    expression match {
      case d: DoubleLiteral if d.value >= 0.0 && d.value <= 1.0 =>
        SemanticCheckResult.success
      case i: IntegerLiteral if i.value == 0L || i.value == 1L =>
        SemanticCheckResult.success
      case d: DoubleLiteral =>
        SemanticError(s"Invalid input '${d.value}' is not a valid argument, must be a number in the range 0.0 to 1.0",
          d.position)

      case l: Literal =>
        SemanticError(s"Invalid input '${
          l.asCanonicalStringVal
        }' is not a valid argument, must be a number in the range 0.0 to 1.0", l.position)

      //for other types we'll have to wait until runtime to fail
      case _ => SemanticCheckResult.success

    }
  }
}
