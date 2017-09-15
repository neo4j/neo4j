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

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticAnalysisTooling, SemanticCheckResult, SemanticError, SemanticExpressionCheck}

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

abstract class Function extends SemanticAnalysisTooling {
  def name: String

  def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    when(invocation.distinct) {
      SemanticError(s"Invalid use of DISTINCT with function '$name'", invocation.position)
    } chain SemanticExpressionCheck.check(ctx, invocation.arguments) chain semanticCheck(ctx, invocation)

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
    checkTypes(invocation, signatures)
}

abstract class AggregatingFunction extends Function {
  override def semanticCheckHook(ctx: ast.Expression.SemanticContext, invocation: ast.FunctionInvocation): SemanticCheck =
    when(ctx == ast.Expression.SemanticContext.Simple) {
      SemanticError(s"Invalid use of aggregating function $name(...) in this context", invocation.position)
    } chain
      SemanticExpressionCheck.check(ctx, invocation.arguments) chain
      semanticCheck(ctx, invocation)


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
