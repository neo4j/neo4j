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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionTypeSignature
import org.neo4j.cypher.internal.expressions.FunctionTypeSignatures
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.helpers.LazyVal

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
  val GRAPH = "Graph"
  val UUID = "UUID"
  val VECTOR = "Vector"
  val SECURITY = "Security"
  val LOCAL = "Local"
}

object Function {

  private val knownFunctions: Seq[Function] = Vector(
    Abs,
    Acos,
    AllReduce,
    Asin,
    Atan,
    Atan2,
    Avg,
    BTrim,
    Cardinality,
    Ceil,
    Ceiling,
    CharacterLength,
    CharLength,
    Coalesce,
    CollDistinct,
    Collect,
    CollectList,
    CollFlatten,
    CollIndexOf,
    CollInsert,
    CollMax,
    CollMin,
    CollRemove,
    CollSort,
    Cos,
    Cosh,
    Cot,
    Coth,
    Count,
    Degrees,
    Distance,
    DurationBetween,
    E,
    ElementId,
    EndNode,
    Exists,
    Exp,
    File,
    Floor,
    Format,
    GraphByName,
    GraphByElementId,
    Haversin,
    Head,
    Id,
    IsEmpty,
    IsNaN,
    Labels,
    Last,
    Left,
    Length,
    PathLength,
    Linenumber,
    Ln,
    LocalDateTime,
    LocalTime,
    Log,
    Log10,
    Lower,
    LTrim,
    Max,
    Min,
    Nodes,
    Normalize,
    NullIf,
    Pi,
    PercentileCont,
    PercentileContAlias,
    PercentileDisc,
    PercentileDiscAlias,
    Percentiles,
    Point,
    Properties,
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
    Sinh,
    Size,
    Sqrt,
    Split,
    StartNode,
    StdDev,
    StdDevP,
    StdDevPop,
    StdDevSamp,
    StringIndexOf,
    StringJoin,
    StringRegexReplace,
    Substring,
    Sum,
    Tail,
    Tan,
    Tanh,
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
    Trim,
    Type,
    Upper,
    UUIDConstructor,
    UUIDLeastSignificantBits,
    UUIDMostSignificantBits,
    ValueType,
    VectorDimensionCount,
    VectorDistance,
    VectorNorm,
    VectorValueConstructor,
    VectorSimilarityEuclidean,
    VectorSimilarityCosine,
    WithinBBox,
    ZonedDateTime,
    ZonedTime
  )

  def lookup: Map[String, Function] = lazyLookup.value

  private val lazyLookup: LazyVal[Map[String, Function]] = LazyVal {
    knownFunctions.map { f => (f.name.toLowerCase(Locale.ROOT), f) }.toMap
  }

  def scopedLookup(scope: CypherVersion): Map[String, Function] =
    knownFunctions.filter(_.signatures.exists(_.scopes.contains(scope))).map { f =>
      (f.name.toLowerCase(Locale.ROOT), f)
    }.toMap

  def functionInfo: List[FunctionTypeSignature] = lazyFunctionInfo.value

  private val lazyFunctionInfo: LazyVal[List[FunctionTypeSignature]] = LazyVal {
    lookup.values.flatMap {
      (f: Function) =>
        f.signatures.flatMap {
          case signature: FunctionTypeSignature if !signature.internal && signature.semanticFeature.isEmpty =>
            Some(signature)
          case _ => None
        }
    }.toList
  }

  // Only use this if there are known functions under flags, otherwise use the lazy one above
  def functionInfoWithFeatureFlags(flags: Set[String]): List[FunctionTypeSignature] = {
    lookup.values.flatMap {
      (f: Function) =>
        f.signatures.flatMap {
          case signature @ FunctionTypeSignature(_, _, _, _, _, _, _, _, _, false, semanticFeature, _, _, _, _)
            if semanticFeature.isEmpty || semanticFeature.exists(flags.contains) => Some(signature)
          case _ => None
        }
    }.toList
  }

  def scopedFunctionInfo(scope: CypherVersion): List[FunctionTypeSignature] = {
    scopedLookup(scope).values.flatMap {
      (f: Function) =>
        f.signatures.flatMap {
          case signature: FunctionTypeSignature if !signature.internal => Some(signature)
          case _                                                       => None
        }
    }.toList
  }

  def isIdFunction(func: FunctionInvocation) = func.function == functions.Id || func.function == functions.ElementId
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

abstract class Function extends FunctionWithName with FunctionTypeSignatures {

  def asFunctionName(implicit position: InputPosition): FunctionName = {
    val names = name.split("\\.")
    if (names.length == 1) {
      FunctionName(Namespace()(position), name)(position)
    } else {
      FunctionName(Namespace(names.dropRight(1).toList)(position), names.last)(position)
    }
  }

  def asInvocation(argument: Expression, distinct: Boolean = false)(implicit
    position: InputPosition): FunctionInvocation = {
    FunctionInvocation(asFunctionName, distinct = distinct, IndexedSeq(argument))(position)
  }

  def asInvocation(lhs: Expression, rhs: Expression)(implicit position: InputPosition): FunctionInvocation = {
    FunctionInvocation(asFunctionName, distinct = false, IndexedSeq(lhs, rhs))(position)
  }

  // Default apply and unapply methods which are valid for functions taking exactly one argument
  def apply(arg: Expression)(pos: InputPosition): FunctionInvocation = {
    FunctionInvocation(asFunctionName(pos), arg)(pos)
  }

  def unapply(arg: Expression): Option[Expression] = {
    val function = asFunctionName(InputPosition.NONE)
    val namespace = function.namespace
    val functionName = function.name
    arg match {
      case FunctionInvocation(FunctionName(ns, fn), _, args, _, _, _, _)
        if functionName.equalsIgnoreCase(fn) && ns == namespace =>
        Some(args.head)
      case _ =>
        None
    }
  }
}

trait FunctionWithName {
  def name: String
}

abstract class AggregatingFunction extends Function
