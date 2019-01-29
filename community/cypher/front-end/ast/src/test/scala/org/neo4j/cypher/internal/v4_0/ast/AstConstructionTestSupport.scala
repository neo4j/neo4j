/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.expressions
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.{Avg, Collect, Count, Max, Min, Sum}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.v4_0.util.{DummyPosition, InputPosition, symbols}

import scala.language.implicitConversions

trait AstConstructionTestSupport extends CypherTestSupport {
  protected val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  def varFor(name: String): Variable = Variable(name)(pos)

  def lblName(s: String): LabelName = LabelName(s)(pos)

  def hasLabels(v: String, label: String): HasLabels =
    hasLabels(varFor(v), label)

  def hasLabels(v: LogicalVariable, labels: String*): HasLabels =
    HasLabels(v, labels.map(lblName))(pos)

  def prop(variable: String, propKey: String): Property = Property(varFor(variable), PropertyKeyName(propKey)(pos))(pos)

  def property(map: Expression, key: String): Expression = Property(map, PropertyKeyName(key)(pos))(pos)

  def propEquality(variable: String, propKey: String, intValue: Int): Equals =
    Equals(prop(variable, propKey), literalInt(intValue))(pos)

  def propLessThan(variable: String, propKey: String, intValue: Int): LessThan =
    LessThan(prop(variable, propKey), literalInt(intValue))(pos)

  def literalString(stringValue: String): StringLiteral =
    StringLiteral(stringValue)(pos)

  def literalInt(intValue: Int): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(intValue.toString)(pos)

  def literalFloat(floatValue: Double): DecimalDoubleLiteral =
    DecimalDoubleLiteral(floatValue.toString)(pos)

  def literalList(expressions: Expression*): ListLiteral =
    ListLiteral(expressions.toSeq)(pos)

  def literalIntList(intValues: Int*): ListLiteral =
    ListLiteral(intValues.toSeq.map(literalInt))(pos)

  def literalFloatList(floatValues: Double*): ListLiteral =
    ListLiteral(floatValues.toSeq.map(literalFloat))(pos)

  def literalIntMap(keyValues: (String, Int)*): MapExpression =
    MapExpression(keyValues.map {
      case (k, v) => (PropertyKeyName(k)(pos), SignedDecimalIntegerLiteral(v.toString)(pos))
    })(pos)

  def listOf(expressions: Expression*): ListLiteral = ListLiteral(expressions)(pos)

  def mapOf(keysAndValues: (String, Expression)*): MapExpression = MapExpression(keysAndValues.map {
    case (k, v) => PropertyKeyName(k)(pos) -> v
  })(pos)

  def TRUE: Expression = True()(pos)

  def nullLiteral: Null = Null()(pos)

  def trueLiteral: True = True()(pos)

  def falseLiteral: False = False()(pos)

  def literal(a: Any): Expression = a match {
    case null => nullLiteral
    case s: String => literalString(s)
    case d: Double => literalFloat(d)
    case d: java.lang.Float => literalFloat(d.doubleValue())
    case i: Byte => literalInt(i)
    case i: Short => literalInt(i)
    case i: Int => literalInt(i)
    case l: Long => SignedDecimalIntegerLiteral(l.toString)(pos)
  }

  def function(name: String, args: Expression*): FunctionInvocation =  FunctionInvocation(FunctionName(name)(pos),
                                                                                          distinct = false, args.toIndexedSeq)(pos)

  def count(expression: Expression): FunctionInvocation = FunctionInvocation(expression, FunctionName(Count.name)(pos))

  def avg(expression: Expression): FunctionInvocation = FunctionInvocation(expression, FunctionName(Avg.name)(pos))

  def collect(expression: Expression): FunctionInvocation = FunctionInvocation(expression, FunctionName(Collect.name)(pos))

  def max(expression: Expression): FunctionInvocation = FunctionInvocation(expression, FunctionName(Max.name)(pos))

  def min(expression: Expression): FunctionInvocation = FunctionInvocation(expression, FunctionName(Min.name)(pos))

  def sum(expression: Expression): FunctionInvocation = FunctionInvocation(expression, FunctionName(Sum.name)(pos))

  def literalMap(keyValues: (String,Expression)*): MapExpression =
    MapExpression(keyValues.map(kv => (PropertyKeyName(kv._1)(pos), kv._2)))(pos)

  def not(e: Expression): Expression = expressions.Not(e)(pos)

  def equals(lhs: Expression, rhs: Expression): Equals = Equals(lhs, rhs)(pos)

  def notEquals(lhs: Expression, rhs: Expression): Expression = NotEquals(lhs, rhs)(pos)

  def lessThan(lhs: Expression, rhs: Expression): LessThan = LessThan(lhs, rhs)(pos)

  def lessThanOrEqual(lhs: Expression, rhs: Expression): LessThanOrEqual = LessThanOrEqual(lhs, rhs)(pos)

  def greaterThan(lhs: Expression, rhs: Expression): GreaterThan = GreaterThan(lhs, rhs)(pos)

  def greaterThanOrEqual(lhs: Expression, rhs: Expression): GreaterThanOrEqual = GreaterThanOrEqual(lhs, rhs)(pos)

  def regex(lhs: Expression, rhs: Expression): RegexMatch = RegexMatch(lhs, rhs)(pos)

  def startsWith(lhs: Expression, rhs: Expression): StartsWith = StartsWith(lhs, rhs)(pos)

  def endsWith(lhs: Expression, rhs: Expression): EndsWith = EndsWith(lhs, rhs)(pos)

  def contains(lhs: Expression, rhs: Expression): Contains = Contains(lhs, rhs)(pos)

  def in(lhs: Expression, rhs: Expression): In = In(lhs, rhs)(pos)

  def coerceTo(expression: Expression, typ: symbols.CypherType): CoerceTo = CoerceTo(expression, typ)

  def isNull(expression: Expression): IsNull = expressions.IsNull(expression)(pos)

  def isNotNull(expression: Expression): IsNotNull = expressions.IsNotNull(expression)(pos)

  def sliceFrom(list: Expression, from: Expression): ListSlice = ListSlice(list, Some(from), None)(pos)

  def sliceTo(list: Expression, to: Expression): ListSlice = ListSlice(list, None, Some(to))(pos)

  def sliceFull(list: Expression, from: Expression, to: Expression): ListSlice = ListSlice(list, Some(from), Some(to))(pos)

  def singleInList(variable: String, collection: Expression, predicate: Expression): SingleIterablePredicate =
    SingleIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  def noneInList(variable: String, collection: Expression, predicate: Expression): NoneIterablePredicate =
    NoneIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  def anyInList(variable: String, collection: Expression, predicate: Expression): AnyIterablePredicate =
    AnyIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  def allInList(variable: String, collection: Expression, predicate: Expression): AllIterablePredicate =
    AllIterablePredicate(varFor(variable), collection, Some(predicate) )(pos)

  def filter(variable: String, collection: Expression, predicate: Expression): FilterExpression =
    FilterExpression(varFor(variable), collection, Some(predicate) )(pos)

  def extract(variable: String, collection: Expression, extract: Expression): ExtractExpression =
    ExtractExpression(varFor(variable), collection, None, Some(extract) )(pos)

  def reduce(accumulator: LogicalVariable, init: Expression, variable: LogicalVariable, collection: Expression, expression: Expression): ReduceExpression =
    ReduceExpression(ReduceScope(accumulator, variable, expression)(pos), init,
                     collection)(pos)

  def listComprehension(variable: String,
                                collection: Expression,
                                predicate: Option[Expression],
                                extractExpression: Option[Expression]): ListComprehension =
    ListComprehension(varFor(variable), collection, predicate, extractExpression)(pos)

  def add(l: Expression, r: Expression): Expression = expressions.Add(l, r)(pos)

  def unaryAdd(source: Expression): Expression = UnaryAdd(source)(pos)

  def subtract(l: Expression, r: Expression): Expression = expressions.Subtract(l, r)(pos)

  def unarySubtract(source: Expression): Expression = UnarySubtract(source)(pos)

  def multiply(l: Expression, r: Expression): Expression = Multiply(l, r)(pos)

  def divide(l: Expression, r: Expression): Expression = Divide(l, r)(pos)

  def modulo(l: Expression, r: Expression): Expression = Modulo(l, r)(pos)

  def pow(l: Expression, r: Expression): Expression = Pow(l, r)(pos)

  def parameter(key: String): Expression = Parameter(key, symbols.CTAny)(pos)

  def or(l: Expression, r: Expression): Expression = Or(l, r)(pos)

  def xor(l: Expression, r: Expression): Expression = Xor(l, r)(pos)

  def ors(es: Expression*): Expression = Ors(es.toSet)(pos)

  def and(l: Expression, r: Expression): Expression = And(l, r)(pos)

  def ands(es: Expression*): Expression = Ands(es.toSet)(pos)

  def containerIndex(container: Expression, index: Expression): Expression = ContainerIndex(container, index)(pos)
}
