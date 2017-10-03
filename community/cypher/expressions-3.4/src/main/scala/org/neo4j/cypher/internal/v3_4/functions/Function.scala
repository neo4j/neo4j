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
package org.neo4j.cypher.internal.v3_4.functions

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, FunctionInvocation, FunctionName}

object Function {
  private val knownFunctions: Seq[Function] = Vector(
    Abs,
    Acos,
    Asin,
    Atan,
    Atan2,
    Avg,
    Ceil,
    Coalesce,
    Collect,
    Ceil,
    Cos,
    Cot,
    Count,
    Degrees,
    Distance,
    E,
    EndNode,
    Exists,
    Exp,
    Floor,
    Haversin,
    Head,
    Id,
    Labels,
    Last,
    Left,
    Length,
    Log,
    Log10,
    LTrim,
    Max,
    Min,
    Nodes,
    Pi,
    PercentileCont,
    PercentileDisc,
    Point,
    Keys,
    Radians,
    Rand,
    Range,
    Reduce,
    Relationships,
    Replace,
    Reverse,
    Right,
    Round,
    RTrim,
    Sign,
    Sin,
    Size,
    Sqrt,
    Split,
    StartNode,
    StdDev,
    StdDevP,
    Substring,
    Sum,
    Tail,
    Tan,
    Timestamp,
    ToBoolean,
    ToFloat,
    ToInteger,
    ToLower,
    ToString,
    ToUpper,
    Properties,
    Trim,
    Type
  )

  val lookup: Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap
}

abstract class Function {
  def name: String

  def asFunctionName(implicit position: InputPosition): FunctionName = FunctionName(name)(position)

  def asInvocation(argument: Expression, distinct: Boolean = false)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = distinct, IndexedSeq(argument))(position)

  def asInvocation(lhs: Expression, rhs: Expression)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = false, IndexedSeq(lhs, rhs))(position)
}

abstract class AggregatingFunction extends Function
