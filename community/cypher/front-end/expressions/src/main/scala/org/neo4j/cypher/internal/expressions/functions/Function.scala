/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.expressions.TypeSignatures
import org.neo4j.cypher.internal.util.InputPosition

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
    File,
    Floor,
    Haversin,
    Head,
    Id,
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
    Nodes,
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
    ToFloat,
    ToInteger,
    ToLower,
    ToString,
    ToUpper,
    Properties,
    Trim,
    Type
  )

  lazy val lookup: Map[String, Function] = knownFunctions.map { f => (f.name.toLowerCase, f) }.toMap

  lazy val functionInfo: List[FunctionInfo] = {
    lookup.values.flatMap {
      case f: TypeSignatures =>
        f.signatures.flatMap {
          case signature: FunctionTypeSignature if !signature.deprecated =>
            val info: FunctionInfo = new FunctionInfo(f) {
              def getDescription: String = signature.description

              def getCategory: String = signature.category

              def getSignature: String = signature.getSignatureAsString
            }
            Seq(info)
          case signature: FunctionTypeSignature if signature.deprecated =>
            Seq()
          case problem =>
            throw new IllegalStateException("Did not expect the following at this point: "+ problem)
        }
      case func: FunctionWithInfo =>
        Seq(new FunctionInfo(func) {
          def getDescription: String = func.getDescription

          def getCategory: String = func.getCategory

          def getSignature: String = func.getSignatureAsString
        })
    }.toList
  }
}

abstract case class FunctionInfo(f: FunctionWithName) {
  def getFunctionName: String = f.name

  def isAggregationFunction: Boolean = f match {
    case _: AggregatingFunction => true
    case _ => false
  }

  def getDescription: String

  def getCategory: String

  def getSignature: String

  override def toString: String = getFunctionName + " || " + getSignature + " || " + getDescription + " || " + isAggregationFunction
}

abstract class Function extends FunctionWithName {
  private val functionName = asFunctionName(InputPosition.NONE)

  def asFunctionName(implicit position: InputPosition): FunctionName = FunctionName(name)(position)

  def asInvocation(argument: Expression, distinct: Boolean = false)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = distinct, IndexedSeq(argument))(position)

  def asInvocation(lhs: Expression, rhs: Expression)(implicit position: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName, distinct = false, IndexedSeq(lhs, rhs))(position)

  // Default apply and unapply methods which are valid for functions taking exactly one argument
  def apply(arg: Expression)(pos: InputPosition): FunctionInvocation =
    FunctionInvocation(asFunctionName(pos), arg)(pos)

  def unapply(arg: Expression): Option[Expression] =
    arg match {
      case FunctionInvocation(_, `functionName`, _, args) => Some(args.head)
      case _ => None
    }
}

trait FunctionWithName {
  def name: String
}

trait FunctionWithInfo {
  def getSignatureAsString: String

  def getDescription: String

  def getCategory: String
}

abstract class AggregatingFunction extends Function
