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

  def asFunctionName(implicit position: InputPosition): FunctionName = FunctionName(name)(position)

  def asInvocation(argument: ast.Expression, distinct: Boolean = false)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = distinct, IndexedSeq(argument))(position)

  def asInvocation(lhs: ast.Expression, rhs: ast.Expression)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = false, IndexedSeq(lhs, rhs))(position)
}

abstract class AggregatingFunction extends Function
