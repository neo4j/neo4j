/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VarLengthLowerBound
import org.neo4j.cypher.internal.expressions.VarLengthUpperBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Id
import org.neo4j.cypher.internal.expressions.functions.Length3_5
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.Nodes
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.test_helpers.CypherTestSupport

import java.nio.charset.StandardCharsets
import scala.language.implicitConversions

trait AstConstructionTestSupport extends CypherTestSupport {
  protected val pos: InputPosition = DummyPosition(0)

  //noinspection LanguageFeature
  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  def varFor(name: String): Variable = Variable(name)(pos)

  def labelName(s: String): LabelName = LabelName(s)(pos)

  def relTypeName(s: String): RelTypeName = RelTypeName(s)(pos)

  def labelOrRelTypeName(s: String): LabelOrRelTypeName = LabelOrRelTypeName(s)(pos)

  def propName(s: String): PropertyKeyName = PropertyKeyName(s)(pos)

  def hasLabels(v: String, label: String): HasLabels =
    hasLabels(varFor(v), label)

  def hasTypes(v: String, types: String*): HasTypes =
    HasTypes(varFor(v), types.map(relTypeName))(pos)

  def hasLabels(v: LogicalVariable, labels: String*): HasLabels =
    HasLabels(v, labels.map(labelName))(pos)

  def hasAnyLabel(v: LogicalVariable, labels: String*): HasAnyLabel =
    HasAnyLabel(v, labels.map(labelName))(pos)

  def hasAnyLabel(v: String, labels: String*): HasAnyLabel =
    HasAnyLabel(varFor(v), labels.map(labelName))(pos)

  def hasLabelsOrTypes(v: String, labelsOrTypes: String*): HasLabelsOrTypes =
    HasLabelsOrTypes(varFor(v), labelsOrTypes.map(n => LabelOrRelTypeName(n)(pos)))(pos)

  def exists(e: Expression): FunctionInvocation =
    FunctionInvocation(FunctionName(Exists.name)(e.position), e)(e.position)

  def prop(variable: String, propKey: String): Property =
    Property(varFor(variable), propName(propKey))(pos)

  def cachedNodeProp(variable: String, propKey: String): CachedProperty =
    cachedNodeProp(variable, propKey, variable)

  def cachedNodePropFromStore(variable: String, propKey: String): CachedProperty =
    cachedNodeProp(variable, propKey, variable, knownToAccessStore = true)

  def cachedNodeProp(variable: String, propKey: String, currentVarName: String, knownToAccessStore: Boolean = false): CachedProperty =
    CachedProperty(variable, varFor(currentVarName), propName(propKey), NODE_TYPE, knownToAccessStore)(pos)

  def cachedRelProp(variable: String, propKey: String): CachedProperty =
    cachedRelProp(variable, propKey, variable)

  def cachedRelPropFromStore(variable: String, propKey: String): CachedProperty =
    cachedRelProp(variable, propKey, variable, knownToAccessStore = true)

  def cachedRelProp(variable: String, propKey: String, currentVarName: String, knownToAccessStore: Boolean = false): CachedProperty =
    CachedProperty(variable, varFor(currentVarName), propName(propKey), RELATIONSHIP_TYPE, knownToAccessStore)(pos)

  def prop(map: Expression, key: String): Property =
    Property(map, propName(key))(pos)

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

  def sensitiveLiteral(stringVal: String): SensitiveStringLiteral =
    SensitiveStringLiteral(stringVal.getBytes(StandardCharsets.UTF_8))(pos)

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
      case (k, v) => propName(k) -> v
    })(pos)

  def mapOfInt(keyValues: (String, Int)*): MapExpression =
    MapExpression(keyValues.map {
      case (k, v) => (propName(k), literalInt(v))
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
    case true => trueLiteral
    case false => falseLiteral
  }

  def returnLit(items: (Any, String)*): Return =
    return_(items.map(i => literal(i._1).as(i._2)): _*)

  def returnVars(vars: String*): Return =
    return_(vars.map(v => varFor(v).aliased): _*)

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

  def andedPropertyInequalities(firstInequality: InequalityExpression, otherInequalities: InequalityExpression*): AndedPropertyInequalities = {
    val property = firstInequality.lhs match {
      case p: Property => p
      case _ => throw new IllegalStateException("Must specify property as LHS of InequalityExpression")
    }
    val variable = property.map match {
      case v: Variable => v
      case _ => throw new IllegalStateException("Must specify variable as map of property")
    }
    AndedPropertyInequalities(variable, property, NonEmptyList(firstInequality, otherInequalities: _*))
  }

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

  def ors(expressions: Expression*): Ors = Ors(expressions)(pos)

  def and(lhs: Expression, rhs: Expression): And = And(lhs, rhs)(pos)

  def ands(expressions: Expression*): Ands = Ands(expressions)(pos)

  def containerIndex(container: Expression, index: Expression): ContainerIndex = ContainerIndex(container, index)(pos)

  def nodePat(): NodePattern =
    NodePattern(None, Seq(), None, None)(pos)

  def nodePat(name: String): NodePattern =
    NodePattern(Some(Variable(name)(pos)), Seq(), None, None)(pos)

  def nodePat(name: String, labels: String*): NodePattern =
    NodePattern(Some(Variable(name)(pos)), labels.map(LabelName(_)(pos)), None, None)(pos)

  def patternExpression(nodeVar1: Variable, nodeVar2: Variable): PatternExpression =
    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(nodeVar1), Seq.empty, None, None)(pos),
      RelationshipPattern(None, Seq.empty, None, None, BOTH)(pos),
      NodePattern(Some(nodeVar2), Seq.empty, None, None)(pos)
    )(pos))(pos))(Set.empty, "", "")

  def nodes(p: PathExpression): FunctionInvocation = {
    FunctionInvocation(FunctionName(Nodes.name)(p.position), p)(p.position)
  }

  def query(part: QueryPart): Query =
    Query(None, part)(pos)

  def query(cs: Clause*): Query =
    Query(None, SingleQuery(cs)(pos))(pos)

  def singleQuery(cs: Clause*): SingleQuery =
    SingleQuery(cs)(pos)

  def unionDistinct(qs: SingleQuery*): QueryPart =
    qs.reduceLeft[QueryPart](UnionDistinct(_, _)(pos))

  def subqueryCall(cs: Clause*): SubqueryCall =
    SubqueryCall(SingleQuery(cs)(pos), None)(pos)

  def subqueryCall(part: QueryPart): SubqueryCall =
    SubqueryCall(part, None)(pos)

  def subqueryCallInTransactions(cs: Clause*): SubqueryCall = {
    val call = subqueryCall(cs:_*)
    call.copy(inTransactionsParameters = Some(inTransactionsParameters(None)))(pos)
  }

  def subqueryCallInTransactions(inTransactionParameters: SubqueryCall.InTransactionsParameters, cs: Clause*): SubqueryCall = {
    val call = subqueryCall(cs:_*)
    call.copy(inTransactionsParameters = Some(inTransactionParameters))(pos)
  }

  def inTransactionsParameters(batchSize: Option[Expression]): SubqueryCall.InTransactionsParameters =
    SubqueryCall.InTransactionsParameters(batchSize)(pos)

  def create(pattern: PatternElement): Create =
    Create(Pattern(Seq(EveryPath(pattern)))(pos))(pos)

  def merge(pattern: PatternElement): Merge =
    Merge(Pattern(Seq(EveryPath(pattern)))(pos), Seq.empty)(pos)

  def match_(pattern: PatternElement, where: Option[Where] = None): Match =
    Match(optional = false, Pattern(Seq(EveryPath(pattern)))(pos), Seq(), where)(pos)

  def with_(items: ReturnItem*): With =
    With(ReturnItems(includeExisting = false, items)(pos))(pos)

  def return_(items: ReturnItem*): Return =
    Return(ReturnItems(includeExisting = false, items)(pos))(pos)

  def return_(ob: OrderBy, items: ReturnItem*): Return =
    Return(distinct = false, ReturnItems(includeExisting = false, items)(pos), Some(ob), None, None)(pos)

  def returnAll: Return = Return(returnAllItems)(pos)

  def returnAllItems: ReturnItems = ReturnItems(includeExisting = true, Seq.empty)(pos)

  def returnItems(items: ReturnItem*): ReturnItems = ReturnItems(includeExisting = false, items)(pos)

  def returnItem(expr: Expression, text: String): UnaliasedReturnItem = UnaliasedReturnItem(expr, text)(pos)

  def variableReturnItem(text: String): UnaliasedReturnItem = returnItem(varFor(text), text)

  def aliasedReturnItem(variable: Variable): AliasedReturnItem = AliasedReturnItem(variable)

  def aliasedReturnItem(originalName: String, newName: String): AliasedReturnItem = AliasedReturnItem(varFor(originalName), varFor(newName))(pos, isAutoAliased = false)

  def orderBy(items: SortItem*): OrderBy =
    OrderBy(items)(pos)

  def skip(value: Long): Skip = Skip(literalInt(value))(pos)

  def limit(value: Long): Limit = Limit(literalInt(value))(pos)

  def sortItem(e: Expression): AscSortItem =
    AscSortItem(e)(pos)

  def where(expr: Expression): Where = Where(expr)(pos)

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

  def use(e: Expression): UseGraph =
    UseGraph(e)(pos)

  def union(part: QueryPart, query: SingleQuery): UnionDistinct = UnionDistinct(part, query)(pos)

  def yieldClause(returnItems: ReturnItems,
                  orderBy: Option[OrderBy] = None,
                  skip: Option[Skip] = None,
                  limit: Option[Limit] = None,
                  where: Option[Where] = None): Yield =
    Yield(returnItems, orderBy, skip, limit, where)(pos)

  def length3_5(argument: Expression): Length3_5 =
    Length3_5(argument)(pos)

  def assertIsNode(v: String): AssertIsNode = AssertIsNode(varFor(v))(pos)

  def varLengthLowerLimitPredicate(relVar: String, limit: Long): VarLengthLowerBound =
    VarLengthLowerBound(varFor(relVar), limit)(pos)

  def varLengthUpperLimitPredicate(relVar: String, limit: Long): VarLengthUpperBound =
    VarLengthUpperBound(varFor(relVar), limit)(pos)

  def foreach(variable: String, listExpr: Expression, updates: Clause*): Foreach =
    Foreach(varFor(variable), listExpr, updates)(pos)

  implicit class ExpressionOps(expr: Expression) {
    def as(name: String): ReturnItem = AliasedReturnItem(expr, varFor(name))(pos, isAutoAliased = false)

    def asc: AscSortItem = AscSortItem(expr)(pos)
    def desc: DescSortItem = DescSortItem(expr)(pos)
  }

  implicit class VariableOps(v: Variable) {
    def aliased: AliasedReturnItem = AliasedReturnItem(v, v)(pos, isAutoAliased = true)
  }

  implicit class NumberLiteralOps(nl: NumberLiteral) {
    def unaliased: UnaliasedReturnItem = UnaliasedReturnItem(nl, nl.stringVal)(pos)
  }

  implicit class UnionLiteralOps(u: UnionDistinct) {
    def all: UnionAll = UnionAll(u.part, u.query)(pos)
  }

}

object AstConstructionTestSupport extends AstConstructionTestSupport {

  implicit class VariableStringInterpolator(val sc: StringContext) extends AnyVal {

    def v(args: Any*): Variable = {
      val connectors = sc.parts.iterator
      val expressions = args.iterator
      val buf = new StringBuffer(connectors.next())
      while (connectors.hasNext) {
        val nextExp = expressions.next() match {
          case s: String           => s
          case lv: LogicalVariable => lv.name
          case x                   => x.toString
        }
        buf.append(nextExp)
        buf.append(connectors.next())
      }
      varFor(buf.toString)
    }
  }
}
