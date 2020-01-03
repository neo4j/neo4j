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
package org.neo4j.cypher.internal.v4_0.ast

import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions._
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherTestSupport
import org.neo4j.cypher.internal.v4_0.util.{DummyPosition, InputPosition}

import scala.language.implicitConversions

trait AstConstructionTestSupport extends CypherTestSupport {
  protected val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  def varFor(name: String): Variable = Variable(name)(pos)

  def labelName(s: String): LabelName = LabelName(s)(pos)

  def hasLabels(v: String, label: String): HasLabels =
    hasLabels(varFor(v), label)

  def hasLabels(v: LogicalVariable, labels: String*): HasLabels =
    HasLabels(v, labels.map(labelName))(pos)

  def exists(e: Expression): FunctionInvocation =
    FunctionInvocation(FunctionName(Exists.name)(e.position), e)(e.position)

  def prop(variable: String, propKey: String): Property =
    Property(varFor(variable), PropertyKeyName(propKey)(pos))(pos)

  def cachedNodeProp(variable: String, propKey: String): CachedProperty =
    CachedProperty(variable, varFor(variable), PropertyKeyName(propKey)(pos), NODE_TYPE)(pos)

  def cachedRelProp(variable: String, propKey: String): CachedProperty =
    CachedProperty(variable, varFor(variable), PropertyKeyName(propKey)(pos), RELATIONSHIP_TYPE)(pos)

  def prop(map: Expression, key: String): Property =
    Property(map, PropertyKeyName(key)(pos))(pos)

  def propEquality(variable: String, propKey: String, intValue: Int): Equals =
    Equals(prop(variable, propKey), literalInt(intValue))(pos)

  def propLessThan(variable: String, propKey: String, intValue: Int): LessThan =
    LessThan(prop(variable, propKey), literalInt(intValue))(pos)

  def propGreaterThan(variable: String, propKey: String, intValue: Int): GreaterThan =
    greaterThan(prop(variable, propKey), literalInt(intValue))

  def literalString(stringValue: String): StringLiteral =
    StringLiteral(stringValue)(pos)

  def literalInt(value: Long): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(value.toString)(pos)

  def literalUnsignedInt(intValue: Int): UnsignedDecimalIntegerLiteral =
    UnsignedDecimalIntegerLiteral(intValue.toString)(pos)

  def literalFloat(floatValue: Double): DecimalDoubleLiteral =
    DecimalDoubleLiteral(floatValue.toString)(pos)

  def listOf(expressions: Expression*): ListLiteral =
    ListLiteral(expressions)(pos)

  def listOfInt(values: Long*): ListLiteral =
    ListLiteral(values.toSeq.map(literalInt))(pos)

  def listOfString(stringValues: String*): ListLiteral =
    ListLiteral(stringValues.toSeq.map(literalString))(pos)

  def index(expression: Expression, idx: Int): ContainerIndex =
    ContainerIndex(expression, literal(idx))(pos)

  def mapOf(keysAndValues: (String, Expression)*): MapExpression =
    MapExpression(keysAndValues.map {
      case (k, v) => PropertyKeyName(k)(pos) -> v
    })(pos)

  def mapOfInt(keyValues: (String, Int)*): MapExpression =
    MapExpression(keyValues.map {
      case (k, v) => (PropertyKeyName(k)(pos), literalInt(v))
    })(pos)

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

  def function(name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, args.toIndexedSeq)(pos)

  def function(ns: Seq[String], name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(Namespace(ns.toList)(pos), FunctionName(name)(pos), distinct = false, args.toIndexedSeq) (pos)

  def distinctFunction(name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = true, args.toIndexedSeq)(pos)

  def count(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Count.name)(pos))

  def countStar(): CountStar =
    CountStar()(pos)

  def avg(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Avg.name)(pos))

  def collect(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Collect.name)(pos))

  def max(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Max.name)(pos))

  def min(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Min.name)(pos))

  def sum(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Sum.name)(pos))

  def id(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Id.name)(pos))

  def not(expression: Expression): Not = Not(expression)(pos)

  def equals(lhs: Expression, rhs: Expression): Equals = Equals(lhs, rhs)(pos)

  def notEquals(lhs: Expression, rhs: Expression): NotEquals = NotEquals(lhs, rhs)(pos)

  def lessThan(lhs: Expression, rhs: Expression): LessThan = LessThan(lhs, rhs)(pos)

  def lessThanOrEqual(lhs: Expression, rhs: Expression): LessThanOrEqual = LessThanOrEqual(lhs, rhs)(pos)

  def greaterThan(lhs: Expression, rhs: Expression): GreaterThan = GreaterThan(lhs, rhs)(pos)

  def greaterThanOrEqual(lhs: Expression, rhs: Expression): GreaterThanOrEqual = GreaterThanOrEqual(lhs, rhs)(pos)

  def getDegree(node: Expression, direction: SemanticDirection): GetDegree = GetDegree(node, None, direction)(pos)

  def regex(lhs: Expression, rhs: Expression): RegexMatch = RegexMatch(lhs, rhs)(pos)

  def startsWith(lhs: Expression, rhs: Expression): StartsWith = StartsWith(lhs, rhs)(pos)

  def endsWith(lhs: Expression, rhs: Expression): EndsWith = EndsWith(lhs, rhs)(pos)

  def contains(lhs: Expression, rhs: Expression): Contains = Contains(lhs, rhs)(pos)

  def in(lhs: Expression, rhs: Expression): In = In(lhs, rhs)(pos)

  def coerceTo(expression: Expression, typ: CypherType): CoerceTo = CoerceTo(expression, typ)

  def isNull(expression: Expression): IsNull = IsNull(expression)(pos)

  def isNotNull(expression: Expression): IsNotNull = IsNotNull(expression)(pos)

  def sliceFrom(list: Expression, from: Expression): ListSlice = ListSlice(list, Some(from), None)(pos)

  def sliceTo(list: Expression, to: Expression): ListSlice = ListSlice(list, None, Some(to))(pos)

  def sliceFull(list: Expression, from: Expression, to: Expression): ListSlice =
    ListSlice(list, Some(from), Some(to))(pos)

  def singleInList(variable: LogicalVariable, collection: Expression, predicate: Expression): SingleIterablePredicate =
    SingleIterablePredicate(variable, collection, Some(predicate) )(pos)

  def noneInList(variable: LogicalVariable, collection: Expression, predicate: Expression): NoneIterablePredicate =
    NoneIterablePredicate(variable, collection, Some(predicate) )(pos)

  def anyInList(variable: LogicalVariable, collection: Expression, predicate: Expression): AnyIterablePredicate =
    AnyIterablePredicate(variable, collection, Some(predicate) )(pos)

  def allInList(variable: LogicalVariable, collection: Expression, predicate: Expression): AllIterablePredicate =
    AllIterablePredicate(variable, collection, Some(predicate) )(pos)

  def reduce(accumulator: LogicalVariable,
             init: Expression,
             variable: LogicalVariable,
             collection: Expression,
             expression: Expression): ReduceExpression =
    ReduceExpression(ReduceScope(accumulator, variable, expression)(pos), init, collection)(pos)

  def listComprehension(variable: LogicalVariable,
                        collection: Expression,
                        predicate: Option[Expression],
                        extractExpression: Option[Expression]): ListComprehension =
    ListComprehension(variable, collection, predicate, extractExpression)(pos)

  def add(lhs: Expression, rhs: Expression): Add = Add(lhs, rhs)(pos)

  def unaryAdd(source: Expression): UnaryAdd = UnaryAdd(source)(pos)

  def subtract(lhs: Expression, rhs: Expression): Subtract = Subtract(lhs, rhs)(pos)

  def unarySubtract(source: Expression): UnarySubtract = UnarySubtract(source)(pos)

  def multiply(lhs: Expression, rhs: Expression): Multiply = Multiply(lhs, rhs)(pos)

  def divide(lhs: Expression, rhs: Expression): Divide = Divide(lhs, rhs)(pos)

  def modulo(lhs: Expression, rhs: Expression): Modulo = Modulo(lhs, rhs)(pos)

  def pow(lhs: Expression, rhs: Expression): Pow = Pow(lhs, rhs)(pos)

  def parameter(key: String, typ: CypherType): Parameter = Parameter(key, typ)(pos)

  def or(lhs: Expression, rhs: Expression): Or = Or(lhs, rhs)(pos)

  def xor(lhs: Expression, rhs: Expression): Xor = Xor(lhs, rhs)(pos)

  def ors(expressions: Expression*): Ors = Ors(expressions.toSet)(pos)

  def and(lhs: Expression, rhs: Expression): And = And(lhs, rhs)(pos)

  def ands(expressions: Expression*): Ands = Ands(expressions.toSet)(pos)

  def containerIndex(container: Expression, index: Expression): ContainerIndex = ContainerIndex(container, index)(pos)

  def nodePat(name: String): NodePattern =
    NodePattern(Some(Variable(name)(pos)), Seq(), None)(pos)

  def nodePat(name: String, labels: String*): NodePattern =
    NodePattern(Some(Variable(name)(pos)), labels.map(LabelName(_)(pos)), None)(pos)

  def patternExpression(nodeVar1: Variable, nodeVar2: Variable) =
    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(nodeVar1), Seq.empty, None)(pos),
      RelationshipPattern(None, Seq.empty, None, None, BOTH)(pos),
      NodePattern(Some(nodeVar2), Seq.empty, None)(pos)
    )(pos))(pos))

  def query(part: QueryPart): Query =
    Query(None, part)(pos)

  def query(cs: Clause*): Query =
    Query(None, SingleQuery(cs)(pos))(pos)

  def singleQuery(cs: Clause*): SingleQuery =
    SingleQuery(cs)(pos)

  def unionDistinct(qs: SingleQuery*): QueryPart =
    qs.reduceLeft[QueryPart](UnionDistinct(_, _)(pos))

  def subQuery(cs: Clause*): SubQuery =
    SubQuery(SingleQuery(cs)(pos))(pos)

  def subQuery(part: QueryPart): SubQuery =
    SubQuery(part)(pos)

  def create(pattern: PatternElement): Create =
    Create(Pattern(Seq(EveryPath(pattern)))(pos))(pos)

  def merge(pattern: PatternElement): Merge =
    Merge(Pattern(Seq(EveryPath(pattern)))(pos), Seq.empty)(pos)

  def match_(pattern: PatternElement, where: Option[Where] = None): Match =
    Match(false, Pattern(Seq(EveryPath(pattern)))(pos), Seq(), where)(pos)

  def with_(items: ReturnItem*): With =
    With(ReturnItems(false, items)(pos))(pos)

  def return_(items: ReturnItem*): Return =
    Return(ReturnItems(false, items)(pos))(pos)

  def return_(ob: OrderBy, items: ReturnItem*): Return =
    Return(false, ReturnItems(false, items)(pos), Some(ob), None, None)(pos)

  def returnAll: Return =
    Return(ReturnItems(true, Seq.empty)(pos))(pos)

  def orderBy(items: SortItem*): OrderBy =
    OrderBy(items)(pos)

  def sortItem(e: Expression): AscSortItem =
    AscSortItem(e)(pos)

  def input(variables: Variable*): InputDataStream =
    InputDataStream(variables)(pos)

  def unwind(e: Expression, v: Variable): Unwind =
    Unwind(e, v)(pos)

  def call(ns: Seq[String], name: String,
           args: Option[Seq[Expression]] = Some(Vector()),
           yields: Option[Seq[Variable]] = None
          ): UnresolvedCall =
    UnresolvedCall(
      Namespace(ns.toList)(pos),
      ProcedureName(name)(pos),
      args,
      yields.map(vs => ProcedureResult(vs.toIndexedSeq.map(ProcedureResultItem(_)(pos)))(pos))
    )(pos)

  def from(e: Expression): FromGraph =
    FromGraph(e)(pos)

  def use(e: Expression): UseGraph =
    UseGraph(e)(pos)

  def union(part: QueryPart, query: SingleQuery): UnionDistinct = UnionDistinct(part, query)(pos)

  implicit class ExpressionOps(expr: Expression) {
    def as(name: String): ReturnItem = AliasedReturnItem(expr, varFor(name))(pos)

    def asc: AscSortItem = AscSortItem(expr)(pos)
    def desc: DescSortItem = DescSortItem(expr)(pos)
  }

  implicit class VariableOps(v: Variable) {
    def aliased: AliasedReturnItem = AliasedReturnItem(v, v)(pos)
  }

  implicit class NumberLiteralOps(nl: NumberLiteral) {
    def unaliased: UnaliasedReturnItem = UnaliasedReturnItem(nl, nl.stringVal)(pos)
  }

  implicit class UnionLiteralOps(u: UnionDistinct) {
    def all: UnionAll = UnionAll(u.part, u.query)(pos)
  }


}
