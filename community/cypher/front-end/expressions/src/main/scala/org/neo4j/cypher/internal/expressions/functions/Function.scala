/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.expressions.functions

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.TypeSignatures
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.util.InputPosition

import java.util.Locale

object Category extends Enumeration {
  val NUMERIC = "Numeric"
  val TRIGONOMETRIC = "Trigonometric"
  val PREDICATE = "Predicate"
  val AGGREGATING = "Aggregating"
  val SCALAR = "Scalar"
  val TEMPORAL = "Temporal"
  val LOGARITHMIC = "Logarithmic"
  val LIST = "List"
  val STRING = "String"
  val SPATIAL = "Spatial"
}

object Function {

  private val knownFunctions: Seq[Function] = Vector(
    Abs,
    Acos,
    Asin,
    Atan,
    Atan2,
    Avg,
    Ceil,
    CharLength,
    CharacterLength,
    Coalesce,
    Collect,
    Ceil,
    Cos,
    Cot,
    Count,
    Degrees,
    Distance,
    E,
    ElementId,
    EndNode,
    Exists,
    Exp,
    File,
    Floor,
    Haversin,
    Head,
    Id,
    IsEmpty,
    IsNaN,
    Labels,
    Last,
    Left,
    Length,
    Linenumber,
    Log,
    Log10,
    LTrim,
    Max,
    Min,
    Percentiles,
    Nodes,
    NullIf,
    Pi,
    PercentileCont,
    PercentileDisc,
    Point,
    Keys,
    Radians,
    Rand,
    RandomUUID,
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
    ToBoolean,
    ToBooleanList,
    ToBooleanOrNull,
    ToFloat,
    ToFloatList,
    ToFloatOrNull,
    ToInteger,
    ToIntegerList,
    ToIntegerOrNull,
    ToLower,
    ToString,
    ToStringList,
    ToStringOrNull,
    ToUpper,
    Timestamp,
    Properties,
    Trim,
    Type,
    ValueType,
    WithinBBox
  )

  lazy val lookup: Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase(Locale.ROOT), f) }.toMap

  lazy val functionInfo: List[FunctionTypeSignature] = {
    lookup.values.flatMap {
      (f: Function) =>
        f.signatures.flatMap {
          case signature: FunctionTypeSignature if !signature.deprecated => Some(signature)
          case signature: FunctionTypeSignature if signature.deprecated  => None
          case problem =>
            throw new IllegalStateException("Did not expect the following at this point: " + problem)
        }
    }.toList
  }

  def isIdFunction(func: FunctionInvocation) = func.function == functions.Id || func.function == functions.ElementId
}

abstract case class FunctionInfo(f: FunctionWithName) {
  def getFunctionName: String = f.name

  def isAggregationFunction: Boolean = f match {
    case _: AggregatingFunction => true
    case _                      => false
  }

  def getDescription: String

  def getCategory: String

  def getSignature: String

  override def toString: String =
    getFunctionName + " || " + getSignature + " || " + getDescription + " || " + isAggregationFunction
}

/**
 * Deterministic function, always produces the same output given the same input
 * arguments and graph state.
 *
 * Allows us to find functions that might be duplicated and can be reduced to
 * a single call.
 */
object DeterministicFunction {
  def unapply(f: Function): Option[Function] = Some(f).filter(isFunctionDeterministic)

  def isFunctionDeterministic(f: Function): Boolean = f != Rand && f != RandomUUID && f != UnresolvedFunction
}

abstract class Function extends FunctionWithName with TypeSignatures {

  def asFunctionName(implicit position: InputPosition): (Namespace, FunctionName) = {
    val names = name.split("\\.")
    if (names.length == 1) {
      (Namespace()(position), FunctionName(name)(position))
    } else {
      (Namespace(names.dropRight(1).toList)(position), FunctionName(names.last)(position))
    }
  }

  def asInvocation(argument: Expression, distinct: Boolean = false)(implicit
  position: InputPosition): FunctionInvocation = {
    val (namespace, functionName) = asFunctionName
    FunctionInvocation(namespace, functionName, distinct = distinct, IndexedSeq(argument))(position)
  }

  def asInvocation(lhs: Expression, rhs: Expression)(implicit position: InputPosition): FunctionInvocation = {
    val (namespace, functionName) = asFunctionName(position)
    FunctionInvocation(namespace, functionName, distinct = false, IndexedSeq(lhs, rhs))(position)
  }

  // Default apply and unapply methods which are valid for functions taking exactly one argument
  def apply(arg: Expression)(pos: InputPosition): FunctionInvocation = {
    val (namespace, functionName) = asFunctionName(pos)
    FunctionInvocation(namespace, functionName, arg)(pos)
  }

  def unapply(arg: Expression): Option[Expression] = {
    val (namespace, functionName) = asFunctionName(InputPosition.NONE)
    arg match {
      case FunctionInvocation(ns, `functionName`, _, args) if ns == namespace => Some(args.head)
      case _                                                                  => None
    }
  }
}

trait FunctionWithName {
  def name: String
}

abstract class AggregatingFunction extends Function
