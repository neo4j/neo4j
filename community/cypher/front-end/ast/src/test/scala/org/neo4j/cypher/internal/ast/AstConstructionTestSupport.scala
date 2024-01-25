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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsBatchParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsErrorParameters
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsReportParameters
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.BooleanLiteral
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.CachedProperty
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.CoerceTo
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DifferentRelationships
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.ElementIdToLongId
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentOrder
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GetDegree
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasALabel
import org.neo4j.cypher.internal.expressions.HasALabelOrType
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.Infinity
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.MatchMode
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NaN
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NodeRelPair
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.NormalForm
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.SensitiveStringLiteral
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VarLengthLowerBound
import org.neo4j.cypher.internal.expressions.VarLengthUpperBound
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.CharacterLength
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.ElementId
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Id
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.Nodes
import org.neo4j.cypher.internal.expressions.functions.Percentiles
import org.neo4j.cypher.internal.expressions.functions.Relationships
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.SizeBucket
import org.neo4j.cypher.internal.util.UnknownSize
import org.neo4j.cypher.internal.util.symbols.CypherType

import java.nio.charset.StandardCharsets

import scala.annotation.tailrec
import scala.collection.immutable.ListSet
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

trait AstConstructionTestSupport {
  protected val pos: InputPosition = DummyPosition(0)
  protected val defaultPos: InputPosition = InputPosition(0, 1, 1)

  // noinspection LanguageFeature
  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  def varFor(name: String): Variable = varFor(name, pos)
  def varFor(name: String, position: InputPosition): Variable = Variable(name)(position)

  def labelName(s: String, position: InputPosition = pos): LabelName = LabelName(s)(position)

  def relTypeName(s: String, position: InputPosition = pos): RelTypeName = RelTypeName(s)(position)

  def labelOrRelTypeName(s: String, position: InputPosition = pos): LabelOrRelTypeName = LabelOrRelTypeName(s)(position)

  def propName(s: String, position: InputPosition = pos): PropertyKeyName = PropertyKeyName(s)(position)

  def hasLabels(v: String, label: String): HasLabels =
    hasLabels(varFor(v), label)

  def andsReorderableAst(exprs: Expression*): AndsReorderable = {
    AndsReorderable(ListSet.from(exprs))(pos)
  }

  def hasTypes(v: String, types: String*): HasTypes =
    HasTypes(varFor(v), types.map(relTypeName(_)))(pos)

  def hasLabels(v: LogicalVariable, labels: String*): HasLabels =
    HasLabels(v, labels.map(labelName(_)))(pos)

  def hasAnyLabel(v: LogicalVariable, labels: String*): HasAnyLabel =
    HasAnyLabel(v, labels.map(labelName(_)))(pos)

  def hasAnyLabel(v: String, labels: String*): HasAnyLabel =
    HasAnyLabel(varFor(v), labels.map(labelName(_)))(pos)

  def hasLabelsOrTypes(v: String, labelsOrTypes: String*): HasLabelsOrTypes =
    HasLabelsOrTypes(varFor(v), labelsOrTypes.map(n => LabelOrRelTypeName(n)(pos)))(pos)

  def hasALabelOrType(v: String): HasALabelOrType =
    HasALabelOrType(varFor(v))(pos)

  def hasALabel(v: String): HasALabel =
    HasALabel(varFor(v))(pos)

  def exists(e: Expression): FunctionInvocation =
    FunctionInvocation(FunctionName(Exists.name)(e.position), e)(e.position)

  def prop(variable: String, propKey: String, position: InputPosition = pos): Property =
    Property(varFor(variable, position), propName(propKey, increasePos(position, variable.length + 1)))(position)

  def cachedNodeProp(variable: String, propKey: String): CachedProperty =
    cachedNodeProp(variable, propKey, variable)

  def cachedNodePropFromStore(variable: String, propKey: String): CachedProperty =
    cachedNodeProp(variable, propKey, variable, knownToAccessStore = true)

  def cachedNodeProp(
    variable: String,
    propKey: String,
    currentVarName: String,
    knownToAccessStore: Boolean = false
  ): CachedProperty =
    CachedProperty(varFor(variable), varFor(currentVarName), propName(propKey), NODE_TYPE, knownToAccessStore)(pos)

  def cachedNodeHasProp(variable: String, propKey: String): CachedHasProperty =
    cachedNodeHasProp(variable, propKey, variable)

  def cachedNodeHasProp(
    variable: String,
    propKey: String,
    currentVarName: String,
    knownToAccessStore: Boolean = false
  ): CachedHasProperty =
    CachedHasProperty(varFor(variable), varFor(currentVarName), propName(propKey), NODE_TYPE, knownToAccessStore)(pos)

  def cachedRelProp(variable: String, propKey: String): CachedProperty =
    cachedRelProp(variable, propKey, variable)

  def cachedRelPropFromStore(variable: String, propKey: String): CachedProperty =
    cachedRelProp(variable, propKey, variable, knownToAccessStore = true)

  def cachedRelProp(
    variable: String,
    propKey: String,
    currentVarName: String,
    knownToAccessStore: Boolean = false
  ): CachedProperty =
    CachedProperty(varFor(variable), varFor(currentVarName), propName(propKey), RELATIONSHIP_TYPE, knownToAccessStore)(
      pos
    )

  def prop(map: Expression, key: String): Property =
    Property(map, propName(key))(pos)

  def propEquality(variable: String, propKey: String, intValue: Int): Equals =
    propEquality(variable, propKey, literalInt(intValue))

  def propEquality(variable: String, propKey: String, intExpression: Expression): Equals =
    Equals(prop(variable, propKey), intExpression)(pos)

  def propLessThan(variable: String, propKey: String, intValue: Int): LessThan =
    LessThan(prop(variable, propKey), literalInt(intValue))(pos)

  def propGreaterThan(variable: String, propKey: String, intValue: Int): GreaterThan =
    greaterThan(prop(variable, propKey), literalInt(intValue))

  def literalString(stringValue: String): StringLiteral =
    StringLiteral(stringValue)(pos)

  def literalBoolean(booleanValue: Boolean): BooleanLiteral = if (booleanValue) {
    True()(pos)
  } else {
    False()(pos)
  }

  def literalInt(value: Long, position: InputPosition = pos): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(value.toString)(position)

  def literalUnsignedInt(intValue: Int): UnsignedDecimalIntegerLiteral =
    UnsignedDecimalIntegerLiteral(intValue.toString)(pos)

  def literalFloat(floatValue: Double): DecimalDoubleLiteral =
    DecimalDoubleLiteral(floatValue.toString)(pos)

  def sensitiveLiteral(stringVal: String): SensitiveStringLiteral =
    SensitiveStringLiteral(stringVal.getBytes(StandardCharsets.UTF_8))(pos)

  def listOf(expressions: Expression*): ListLiteral =
    ListLiteral(expressions)(pos)

  def listOfInt(values: Long*): ListLiteral =
    ListLiteral(values.toSeq.map(literalInt(_)))(pos)

  def listOfFloat(values: Double*): ListLiteral = {
    ListLiteral(values.toSeq.map(literalFloat(_)))(pos)
  }

  def listOfString(stringValues: String*): ListLiteral =
    ListLiteral(stringValues.toSeq.map(literalString))(pos)

  def listOfBoolean(booleanValues: Boolean*): ListLiteral =
    ListLiteral(booleanValues.toSeq.map(literalBoolean))(pos)

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

  def InfinityLiteral: Infinity = Infinity()(pos)

  def NaNLiteral: NaN = NaN()(pos)

  def literal(a: Any): Expression = a match {
    case null               => nullLiteral
    case s: String          => literalString(s)
    case d: Double          => literalFloat(d)
    case d: java.lang.Float => literalFloat(d.doubleValue())
    case i: Byte            => literalInt(i)
    case i: Short           => literalInt(i)
    case i: Int             => literalInt(i)
    case l: Long            => SignedDecimalIntegerLiteral(l.toString)(pos)
    case true               => trueLiteral
    case false              => falseLiteral
    case seq: Seq[_]        => ListLiteral(seq.map(literal))(pos)
    case other =>
      throw new RuntimeException(s"Unexpected type ${other.getClass.getName} ($other)")
  }

  def returnLit(items: (Any, String)*): Return =
    return_(items.map(i => literal(i._1).as(i._2)): _*)

  def returnVars(vars: String*): Return =
    return_(vars.map(v => varFor(v).aliased): _*)

  def function(name: String, args: Expression*): FunctionInvocation =
    function(name, ArgumentUnordered, args: _*)

  def function(name: String, order: ArgumentOrder, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, args.toIndexedSeq, order)(pos)

  def function(ns: Seq[String], name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(Namespace(ns.toList)(pos), FunctionName(name)(pos), distinct = false, args.toIndexedSeq)(pos)

  def distinctFunction(name: String, args: Expression*): FunctionInvocation =
    distinctFunction(name, ArgumentUnordered, args: _*)

  def distinctFunction(name: String, order: ArgumentOrder, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = true, args.toIndexedSeq, order)(pos)

  def count(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Count.name)(pos))

  def count(expression: Expression, isDistinct: Boolean, order: ArgumentOrder): FunctionInvocation =
    FunctionInvocation(FunctionName(Count.name)(pos), isDistinct, IndexedSeq(expression), order)(pos)

  def countStar(): CountStar =
    CountStar()(pos)

  def avg(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Avg.name)(pos))

  def collect(expression: Expression, distinct: Boolean = false): FunctionInvocation =
    FunctionInvocation(FunctionName(Collect.name)(pos), distinct, IndexedSeq(expression))(pos)

  def max(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Max.name)(pos))

  def min(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Min.name)(pos))

  def characterLength(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(CharacterLength.name)(pos))

  def size(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Size.name)(pos))

  def length(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Length.name)(pos))

  def percentiles(
    input: Expression,
    percentiles: Seq[Double],
    propertyKeys: Seq[String],
    isDiscretes: Seq[Boolean],
    distinct: Boolean = false,
    order: ArgumentOrder = ArgumentUnordered
  ): FunctionInvocation = {
    FunctionInvocation(
      FunctionName(Percentiles.name)(pos),
      distinct,
      IndexedSeq(input, listOfFloat(percentiles: _*), listOfString(propertyKeys: _*), listOfBoolean(isDiscretes: _*)),
      order
    )(pos)
  }

  def varLengthPathExpression(
    start: LogicalVariable,
    relationships: LogicalVariable,
    end: LogicalVariable,
    direction: SemanticDirection = SemanticDirection.BOTH
  ): PathExpression =
    PathExpression(
      NodePathStep(start, MultiRelationshipPathStep(relationships, direction, Some(end), NilPathStep()(pos))(pos))(pos)
    )(pos)

  def qppPath(
    start: LogicalVariable,
    variables: Seq[LogicalVariable],
    end: LogicalVariable
  ): PathExpression = {
    if (variables.size % 2 == 1) {
      throw new IllegalArgumentException("Tried to construct node rel pairs but found uneven number of elements")
    }
    val pairs = new ArrayBuffer[NodeRelPair]
    var i = 0
    while (i < variables.size) {
      pairs += NodeRelPair(variables(i), variables(i + 1))
      i += 2
    }
    PathExpression(
      NodePathStep(start, RepeatPathStep(pairs.toSeq, end, NilPathStep()(pos))(pos))(pos)
    )(pos)
  }

  def sum(expression: Expression, distinct: Boolean = false): FunctionInvocation =
    FunctionInvocation(FunctionName(Sum.name)(pos), distinct, IndexedSeq(expression))(pos)

  def id(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(Id.name)(pos))

  def elementId(expression: Expression): FunctionInvocation =
    FunctionInvocation(expression, FunctionName(ElementId.name)(pos))

  def elementIdToNodeId(expression: Expression): ElementIdToLongId =
    ElementIdToLongId(NODE_TYPE, ElementIdToLongId.Mode.Single, expression)(pos)

  def elementIdListToNodeIdList(expression: Expression): ElementIdToLongId =
    ElementIdToLongId(NODE_TYPE, ElementIdToLongId.Mode.Many, expression)(pos)

  def elementIdToRelationshipId(expression: Expression): ElementIdToLongId =
    ElementIdToLongId(RELATIONSHIP_TYPE, ElementIdToLongId.Mode.Single, expression)(pos)

  def elementIdListToRelationshipIdList(expression: Expression): ElementIdToLongId =
    ElementIdToLongId(RELATIONSHIP_TYPE, ElementIdToLongId.Mode.Many, expression)(pos)

  def not(expression: Expression): Not = Not(expression)(pos)

  def equals(lhs: Expression, rhs: Expression): Equals = Equals(lhs, rhs)(pos)

  def notEquals(lhs: Expression, rhs: Expression): NotEquals = NotEquals(lhs, rhs)(pos)

  def lessThan(lhs: Expression, rhs: Expression): LessThan = LessThan(lhs, rhs)(pos)

  def lessThanOrEqual(lhs: Expression, rhs: Expression): LessThanOrEqual = LessThanOrEqual(lhs, rhs)(pos)

  def greaterThan(lhs: Expression, rhs: Expression): GreaterThan = GreaterThan(lhs, rhs)(pos)

  def greaterThanOrEqual(lhs: Expression, rhs: Expression): GreaterThanOrEqual = GreaterThanOrEqual(lhs, rhs)(pos)

  def andedPropertyInequalities(
    firstInequality: InequalityExpression,
    otherInequalities: InequalityExpression*
  ): AndedPropertyInequalities = {
    val property = firstInequality.lhs match {
      case p: Property => p
      case _           => throw new IllegalStateException("Must specify property as LHS of InequalityExpression")
    }
    val variable = property.map match {
      case v: Variable => v
      case _           => throw new IllegalStateException("Must specify variable as map of property")
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

  def isTyped(expression: Expression, typeName: CypherType): IsTyped =
    IsTyped(expression, typeName)(pos)

  def isNotTyped(expression: Expression, typeName: CypherType): IsNotTyped =
    IsNotTyped(expression, typeName)(pos)

  def isNormalized(expression: Expression, normalForm: NormalForm): IsNormalized =
    IsNormalized(expression, normalForm)(pos)

  def isNotNormalized(expression: Expression, normalForm: NormalForm): IsNotNormalized =
    IsNotNormalized(expression, normalForm)(pos)

  def sliceFrom(list: Expression, from: Expression): ListSlice = ListSlice(list, Some(from), None)(pos)

  def sliceTo(list: Expression, to: Expression): ListSlice = ListSlice(list, None, Some(to))(pos)

  def sliceFull(list: Expression, from: Expression, to: Expression): ListSlice =
    ListSlice(list, Some(from), Some(to))(pos)

  def singleInList(variable: LogicalVariable, collection: Expression, predicate: Expression): SingleIterablePredicate =
    SingleIterablePredicate(variable, collection, Some(predicate))(pos)

  def noneInList(variable: LogicalVariable, collection: Expression, predicate: Expression): NoneIterablePredicate =
    NoneIterablePredicate(variable, collection, Some(predicate))(pos)

  def anyInList(variable: LogicalVariable, collection: Expression, predicate: Expression): AnyIterablePredicate =
    AnyIterablePredicate(variable, collection, Some(predicate))(pos)

  def allInList(variable: LogicalVariable, collection: Expression, predicate: Expression): AllIterablePredicate =
    AllIterablePredicate(variable, collection, Some(predicate))(pos)

  def reduce(
    accumulator: LogicalVariable,
    init: Expression,
    variable: LogicalVariable,
    collection: Expression,
    expression: Expression
  ): ReduceExpression =
    ReduceExpression(ReduceScope(accumulator, variable, expression)(pos), init, collection)(pos)

  def listComprehension(
    variable: LogicalVariable,
    collection: Expression,
    predicate: Option[Expression],
    extractExpression: Option[Expression]
  ): ListComprehension =
    ListComprehension(variable, collection, predicate, extractExpression)(pos)

  def add(lhs: Expression, rhs: Expression, position: InputPosition = pos): Add = Add(lhs, rhs)(position)

  def unaryAdd(source: Expression): UnaryAdd = UnaryAdd(source)(pos)

  def subtract(lhs: Expression, rhs: Expression): Subtract = Subtract(lhs, rhs)(pos)

  def unarySubtract(source: Expression): UnarySubtract = UnarySubtract(source)(pos)

  def multiply(lhs: Expression, rhs: Expression): Multiply = Multiply(lhs, rhs)(pos)

  def divide(lhs: Expression, rhs: Expression): Divide = Divide(lhs, rhs)(pos)

  def modulo(lhs: Expression, rhs: Expression): Modulo = Modulo(lhs, rhs)(pos)

  def pow(lhs: Expression, rhs: Expression): Pow = Pow(lhs, rhs)(pos)

  def parameter(key: String, typ: CypherType, sizeHint: Option[Int] = None, position: InputPosition = pos): Parameter =
    ExplicitParameter(key, typ, sizeHint.map(i => SizeBucket.computeBucket(i)).getOrElse(UnknownSize))(position)

  def autoParameter(
    key: String,
    typ: CypherType,
    sizeHint: Option[Int] = None,
    position: InputPosition = pos
  ): AutoExtractedParameter =
    AutoExtractedParameter(
      key,
      typ,
      sizeHint.map(i => SizeBucket.computeBucket(i)).getOrElse(UnknownSize)
    )(position)

  def or(lhs: Expression, rhs: Expression): Or = Or(lhs, rhs)(pos)

  def xor(lhs: Expression, rhs: Expression): Xor = Xor(lhs, rhs)(pos)

  def ors(expressions: Expression*): Ors = Ors(expressions)(pos)

  def and(lhs: Expression, rhs: Expression): And = And(lhs, rhs)(pos)

  def labelConjunction(
    lhs: LabelExpression,
    rhs: LabelExpression,
    position: InputPosition = pos,
    containsIs: Boolean = false
  ): LabelExpression =
    labelConjunctions(Seq(lhs, rhs), position, containsIs)

  def labelConjunctions(
    children: Seq[LabelExpression],
    position: InputPosition = pos,
    containsIs: Boolean = false
  ): LabelExpression =
    LabelExpression.Conjunctions(children, containsIs)(position)

  def labelColonConjunction(
    lhs: LabelExpression,
    rhs: LabelExpression,
    position: InputPosition = pos,
    containsIs: Boolean = false
  ): LabelExpression = LabelExpression.ColonConjunction(lhs, rhs, containsIs)(position)

  def labelDisjunction(
    lhs: LabelExpression,
    rhs: LabelExpression,
    position: InputPosition = pos,
    containsIs: Boolean = false
  ): LabelExpression =
    labelDisjunctions(Seq(lhs, rhs), position, containsIs)

  def labelDisjunctions(
    children: Seq[LabelExpression],
    position: InputPosition = pos,
    containsIs: Boolean = false
  ): LabelExpression =
    LabelExpression.Disjunctions(children, containsIs)(position)

  def labelColonDisjunction(
    lhs: LabelExpression,
    rhs: LabelExpression,
    position: InputPosition = pos,
    containsIs: Boolean = false
  ): LabelExpression = LabelExpression.ColonDisjunction(lhs, rhs, containsIs)(position)

  def labelNegation(e: LabelExpression, position: InputPosition = pos, containsIs: Boolean = false): LabelExpression =
    LabelExpression.Negation(e, containsIs)(position)

  def labelWildcard(position: InputPosition = pos, containsIs: Boolean = false): LabelExpression =
    LabelExpression.Wildcard(containsIs)(position)

  def labelLeaf(name: String, position: InputPosition = pos, containsIs: Boolean = false): LabelExpression =
    Leaf(LabelName(name)(position), containsIs)

  def labelRelTypeLeaf(name: String, position: InputPosition = pos, containsIs: Boolean = false): LabelExpression =
    Leaf(RelTypeName(name)(position), containsIs)

  def labelOrRelTypeLeaf(name: String, position: InputPosition = pos, containsIs: Boolean = false): LabelExpression =
    Leaf(LabelOrRelTypeName(name)(position), containsIs)

  def labelExpressionPredicate(v: String, labelExpression: LabelExpression): LabelExpressionPredicate =
    labelExpressionPredicate(varFor(v), labelExpression)

  def labelExpressionPredicate(subject: Expression, labelExpression: LabelExpression): LabelExpressionPredicate = {
    LabelExpressionPredicate(subject, labelExpression)(pos)
  }

  def ands(expressions: Expression*): Ands = Ands(expressions)(pos)

  def containerIndex(container: Expression, index: Int): ContainerIndex = containerIndex(container, literalInt(index))

  def containerIndex(container: Expression, index: Expression): ContainerIndex = ContainerIndex(container, index)(pos)

  def nodePat(
    name: Option[String] = None,
    labelExpression: Option[LabelExpression] = None,
    properties: Option[Expression] = None,
    predicates: Option[Expression] = None,
    namePos: InputPosition = pos,
    position: InputPosition = pos
  ): NodePattern =
    NodePattern(name.map(Variable(_)(namePos)), labelExpression, properties, predicates)(position)

  def relPat(
    name: Option[String] = None,
    labelExpression: Option[LabelExpression] = None,
    length: Option[Option[Range]] = None,
    properties: Option[Expression] = None,
    predicates: Option[Expression] = None,
    direction: SemanticDirection = OUTGOING,
    namePos: InputPosition = pos,
    position: InputPosition = pos
  ): RelationshipPattern =
    RelationshipPattern(name.map(Variable(_)(namePos)), labelExpression, length, properties, predicates, direction)(
      position
    )

  def pathConcatenation(factors: PathFactor*): PathConcatenation = PathConcatenation(factors)(pos)

  def quantifiedPath(
    relChain: RelationshipChain,
    quantifier: GraphPatternQuantifier,
    optionalWhereExpression: Option[Expression] = None
  ): QuantifiedPath =
    QuantifiedPath(PatternPart(relChain), quantifier, optionalWhereExpression)(pos)

  def quantifiedPath(
    relChain: RelationshipChain,
    quantifier: GraphPatternQuantifier,
    optionalWhereExpression: Option[Expression],
    variableGroupings: Set[VariableGrouping]
  ): QuantifiedPath =
    QuantifiedPath(PatternPart(relChain), quantifier, optionalWhereExpression, variableGroupings)(pos)

  def parenthesizedPath(
    relChain: RelationshipChain,
    optionalWhereExpression: Option[Expression] = None
  ): ParenthesizedPath =
    ParenthesizedPath(PatternPart(relChain), optionalWhereExpression)(pos)

  def allPathsSelector(): PatternPart.AllPaths =
    PatternPart.AllPaths()(pos)

  def anyPathSelector(count: String): PatternPart.AnyPath =
    PatternPart.AnyPath(UnsignedDecimalIntegerLiteral(count)(pos))(pos)

  def anyShortestPathSelector(count: Int): PatternPart.AnyShortestPath =
    PatternPart.AnyShortestPath(UnsignedDecimalIntegerLiteral(count.toString)(pos))(pos)

  def allShortestPathsSelector(): PatternPart.AllShortestPaths =
    PatternPart.AllShortestPaths()(pos)

  def shortestGroups(count: String): PatternPart.ShortestGroups =
    PatternPart.ShortestGroups(UnsignedDecimalIntegerLiteral(count)(pos))(pos)

  def relationshipChain(patternAtoms: PatternAtom*): RelationshipChain =
    patternAtoms.length match {
      case 0 | 1 | 2 => throw new IllegalArgumentException()
      case 3 => RelationshipChain(
          patternAtoms(0).asInstanceOf[NodePattern],
          patternAtoms(1).asInstanceOf[RelationshipPattern],
          patternAtoms(2).asInstanceOf[NodePattern]
        )(pos)
      case _ =>
        RelationshipChain(
          relationshipChain(patternAtoms.dropRight(2): _*),
          patternAtoms.dropRight(1).last.asInstanceOf[RelationshipPattern],
          patternAtoms.last.asInstanceOf[NodePattern]
        )(pos)
    }

  def plusQuantifier: PlusQuantifier = PlusQuantifier()(pos)

  def starQuantifier: StarQuantifier = StarQuantifier()(pos)

  def variableGrouping(singleton: LogicalVariable, group: LogicalVariable): VariableGrouping =
    VariableGrouping(singleton, group)(pos)

  def variableGrouping(singleton: String, group: String): VariableGrouping =
    VariableGrouping(varFor(singleton), varFor(group))(pos)

  def patternExpression(nodeVar1: Variable, nodeVar2: Variable): PatternExpression =
    PatternExpression(RelationshipsPattern(RelationshipChain(
      NodePattern(Some(nodeVar1), None, None, None)(pos),
      RelationshipPattern(None, None, None, None, None, BOTH)(pos),
      NodePattern(Some(nodeVar2), None, None, None)(pos)
    )(pos))(pos))(None, None)

  def patternComprehension(relChain: RelationshipChain, projection: Expression): PatternComprehension =
    PatternComprehension(None, RelationshipsPattern(relChain)(pos), None, projection)(pos, None, None)

  def nodes(p: PathExpression): FunctionInvocation = {
    FunctionInvocation(FunctionName(Nodes.name)(p.position), p)(p.position)
  }

  def relationships(p: PathExpression): FunctionInvocation = {
    FunctionInvocation(FunctionName(Relationships.name)(p.position), p)(p.position)
  }

  def singleQuery(cs: Clause, position: InputPosition): Query =
    SingleQuery(List(cs))(position)

  def singleQuery(cs: Clause*): SingleQuery =
    SingleQuery(cs)(pos)

  def unionDistinct(qs: SingleQuery*): Query =
    qs.reduceLeft[Query](UnionDistinct(_, _)(pos))

  def subqueryCall(cs: Clause*): SubqueryCall =
    SubqueryCall(SingleQuery(cs)(pos), None)(pos)

  def subqueryCall(innerQuery: Query): SubqueryCall =
    SubqueryCall(innerQuery, None)(pos)

  def subqueryCallInTransactions(cs: Clause*): SubqueryCall = {
    val call = subqueryCall(cs: _*)
    call.copy(inTransactionsParameters = Some(inTransactionsParameters(None, None, None)))(pos)
  }

  def subqueryCallInTransactions(
    inTransactionParameters: SubqueryCall.InTransactionsParameters,
    cs: Clause*
  ): SubqueryCall = {
    val call = subqueryCall(cs: _*)
    call.copy(inTransactionsParameters = Some(inTransactionParameters))(pos)
  }

  def inTransactionsParameters(
    batchParams: Option[InTransactionsBatchParameters],
    errorParams: Option[InTransactionsErrorParameters],
    reportParams: Option[InTransactionsReportParameters]
  ): SubqueryCall.InTransactionsParameters =
    SubqueryCall.InTransactionsParameters(batchParams, errorParams, reportParams)(pos)

  def create(pattern: PatternElement, position: InputPosition = pos): Create =
    Create(Pattern.ForUpdate(Seq(PatternPart(pattern)))(pattern.position))(position)

  def merge(pattern: PatternElement): Merge =
    Merge(PatternPart(pattern), Seq.empty)(pos)

  def match_(
    pattern: PatternElement,
    matchMode: MatchMode = MatchMode.default(pos),
    where: Option[Where] = None
  ): Match =
    Match(optional = false, matchMode = matchMode, patternForMatch(pattern), Seq(), where)(pos)

  def optionalMatch(pattern: PatternElement, where: Option[Where] = None): Match = {
    Match(optional = true, MatchMode.default(pos), patternForMatch(pattern), Seq(), where)(pos)
  }

  def match_(patterns: Seq[PatternElement], where: Option[Where]): Match =
    Match(optional = false, MatchMode.default(pos), patternForMatch(patterns: _*), Seq(), where)(pos)

  def match_shortest(
    selector: Selector,
    pattern: PatternElement,
    matchMode: MatchMode = MatchMode.default(pos),
    where: Option[Where] = None
  ): Match =
    Match(
      optional = false,
      matchMode = matchMode,
      Pattern.ForMatch(Seq(PatternPartWithSelector(selector, PatternPart(pattern))))(pos),
      Seq(),
      where
    )(pos)

  def patternForMatch(parts: NonPrefixedPatternPart*): Pattern.ForMatch = {
    Pattern.ForMatch(parts.map(_.withAllPathsSelector))(pos)
  }

  def patternForMatch(elements: PatternElement*)(implicit dummy: DummyImplicit): Pattern.ForMatch = {
    patternForMatch(elements.map(e => PatternPart(e)): _*)
  }

  def with_(items: ReturnItem*): With =
    With(ReturnItems(includeExisting = false, items)(pos))(pos)

  def withAll(items: ReturnItem*): With =
    With(ReturnItems(includeExisting = true, items)(pos))(pos)

  def withAll(where: Option[Where] = None): With =
    With(distinct = false, returnAllItems, None, None, None, where = where)(pos)

  def set_(items: Seq[SetItem]): SetClause =
    SetClause(items)(pos)

  def setLabelItem(node: String, labels: Seq[String], containsIs: Boolean = false): SetLabelItem =
    SetLabelItem(varFor(node), labels.map(label => LabelName(label)(pos)), containsIs)(pos)

  def setPropertyItem(map: String, propertyName: String, expr: Expression): SetPropertyItem =
    SetPropertyItem(Property(Variable(map)(pos), PropertyKeyName(propertyName)(pos))(pos), expr)(pos)

  def remove(items: Seq[RemoveItem]): Remove = Remove(items)(pos)

  def removeLabelItem(node: String, labels: Seq[String], containsIs: Boolean = false): RemoveLabelItem =
    RemoveLabelItem(varFor(node), labels.map(label => LabelName(label)(pos)), containsIs)(pos)

  def removePropertyItem(map: String, propertyName: String): RemovePropertyItem =
    RemovePropertyItem(Property(Variable(map)(pos), PropertyKeyName(propertyName)(pos))(pos))

  def return_(items: ReturnItem*): Return =
    Return(ReturnItems(includeExisting = false, items)(pos))(pos)

  def return_(ob: OrderBy, items: ReturnItem*): Return =
    Return(distinct = false, ReturnItems(includeExisting = false, items)(pos), Some(ob), None, None)(pos)

  def return_(skip: Skip, items: ReturnItem*): Return =
    Return(distinct = false, ReturnItems(includeExisting = false, items)(pos), None, Some(skip), None)(pos)

  def return_(limit: Limit, items: ReturnItem*): Return =
    Return(distinct = false, ReturnItems(includeExisting = false, items)(pos), None, None, Some(limit))(pos)

  def return_(ob: OrderBy, skip: Skip, limit: Limit, items: ReturnItem*): Return =
    Return(distinct = false, ReturnItems(includeExisting = false, items)(pos), Some(ob), Some(skip), Some(limit))(pos)

  def returnDistinct(items: ReturnItem*): Return =
    Return(distinct = true, ReturnItems(includeExisting = false, items)(pos), None, None, None)(pos)

  def returnDistinct(ob: OrderBy, skip: Skip, limit: Limit, items: ReturnItem*): Return =
    Return(distinct = true, ReturnItems(includeExisting = false, items)(pos), Some(ob), Some(skip), Some(limit))(pos)

  def returnAll: Return = Return(returnAllItems)(pos)

  def returnAllItems: ReturnItems = ReturnItems(includeExisting = true, Seq.empty)(pos)

  def returnAllItems(position: InputPosition): ReturnItems = ReturnItems(includeExisting = true, Seq.empty)(position)

  def returnItems(items: ReturnItem*): ReturnItems = ReturnItems(includeExisting = false, items)(pos)

  def returnItem(expr: Expression, text: String, position: InputPosition = pos): UnaliasedReturnItem =
    UnaliasedReturnItem(expr, text)(position)

  def variableReturnItem(text: String, position: InputPosition = pos): UnaliasedReturnItem =
    returnItem(varFor(text, position), text, position)

  def aliasedReturnItem(variable: Variable): AliasedReturnItem = AliasedReturnItem(variable)

  def aliasedReturnItem(originalName: String, newName: String, position: InputPosition = pos): AliasedReturnItem =
    AliasedReturnItem(
      varFor(originalName, position),
      varFor(newName, increasePos(position, originalName.length + 4))
    )(position)

  def aliasedReturnItem(originalExpr: Expression, newName: String): AliasedReturnItem = AliasedReturnItem(
    originalExpr,
    varFor(newName, increasePos(originalExpr.position, originalExpr.asCanonicalStringVal.length + 4))
  )(originalExpr.position)

  def autoAliasedReturnItem(originalExpr: Expression): AliasedReturnItem = AliasedReturnItem(
    originalExpr,
    varFor(
      originalExpr.asCanonicalStringVal,
      increasePos(originalExpr.position, originalExpr.asCanonicalStringVal.length + 4)
    )
  )(originalExpr.position)

  def orderBy(items: SortItem*): OrderBy =
    OrderBy(items)(pos)

  def skip(value: Long, position: InputPosition = pos): Skip =
    Skip(literalInt(value, increasePos(position, 5)))(position)

  def limit(value: Long, position: InputPosition = pos): Limit =
    Limit(literalInt(value, increasePos(position, 6)))(position)

  def sortItem(e: Expression, ascending: Boolean = true, position: InputPosition = pos): SortItem = {
    if (ascending) {
      AscSortItem(e)(position)
    } else {
      DescSortItem(e)(position)
    }
  }

  def where(expr: Expression): Where = Where(expr)(pos)

  def input(variables: Variable*): InputDataStream =
    InputDataStream(variables)(pos)

  def unwind(e: Expression, v: Variable): Unwind =
    Unwind(e, v)(pos)

  def call(
    ns: Seq[String],
    name: String,
    args: Option[Seq[Expression]] = Some(Vector()),
    yields: Option[Seq[Variable]] = None
  ): UnresolvedCall =
    UnresolvedCall(
      Namespace(ns.toList)(pos),
      ProcedureName(name)(pos),
      args,
      yields.map(vs => ProcedureResult(vs.toIndexedSeq.map(ProcedureResultItem(_)(pos)))(pos))
    )(pos)

  def use(names: List[String]): UseGraph = {
    UseGraph(GraphDirectReference(CatalogName(names))(pos))(pos)
  }

  def use(function: FunctionInvocation): UseGraph = {
    UseGraph(GraphFunctionReference(function)(pos))(pos)
  }

  def use(graphReference: GraphReference): UseGraph = {
    UseGraph(graphReference)(pos)
  }

  def union(lhs: Query, rhs: SingleQuery): UnionDistinct = UnionDistinct(lhs, rhs)(pos)

  def yieldClause(
    returnItems: ReturnItems,
    orderBy: Option[OrderBy] = None,
    skip: Option[Skip] = None,
    limit: Option[Limit] = None,
    where: Option[Where] = None
  ): Yield =
    Yield(returnItems, orderBy, skip, limit, where)(pos)

  def range(lower: Option[Int], upper: Option[Int]): Range =
    Range(lower.map(literalUnsignedInt), upper.map(literalUnsignedInt))(pos)

  def point(x: Double, y: Double): Expression =
    function("point", mapOf("x" -> literal(x), "y" -> literal(y)))

  def pointWithinBBox(point: Expression, lowerLeft: Expression, upperRight: Expression): Expression =
    function(Seq("point"), "withinBBox", point, lowerLeft, upperRight)

  def pointDistance(fromPoint: Expression, toPoint: Expression): Expression =
    function(Seq("point"), "distance", fromPoint, toPoint)

  def assertIsNode(v: String): AssertIsNode = AssertIsNode(varFor(v))(pos)

  def caseExpression(
    expression: Option[Expression],
    default: Option[Expression],
    alternatives: (Expression, Expression)*
  ): CaseExpression = CaseExpression(expression, alternatives.toIndexedSeq, default)(pos)

  def simpleExistsExpression(
    pattern: Pattern.ForMatch,
    maybeWhere: Option[Where],
    matchMode: MatchMode = MatchMode.default(pos),
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): ExistsExpression = {

    val simpleMatchQuery = singleQuery(
      Match(optional = false, matchMode, pattern, Seq(), maybeWhere)(pos)
    )

    ExistsExpression(simpleMatchQuery)(pos, Some(introducedVariables), Some(scopeDependencies))
  }

  def simpleCollectExpression(
    pattern: Pattern.ForMatch,
    maybeWhere: Option[Where],
    returnItem: Return,
    matchMode: MatchMode = MatchMode.default(pos),
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): CollectExpression = {

    val simpleMatchQuery = singleQuery(
      Match(optional = false, matchMode, pattern, Seq(), maybeWhere)(pos),
      returnItem
    )

    CollectExpression(simpleMatchQuery)(pos, Some(introducedVariables), Some(scopeDependencies))
  }

  def simpleCountExpression(
    pattern: Pattern.ForMatch,
    maybeWhere: Option[Where],
    matchMode: MatchMode = MatchMode.default(pos),
    introducedVariables: Set[LogicalVariable] = Set.empty,
    scopeDependencies: Set[LogicalVariable] = Set.empty
  ): CountExpression = {

    val simpleMatchQuery = singleQuery(
      Match(optional = false, matchMode, pattern, Seq(), maybeWhere)(pos)
    )

    CountExpression(simpleMatchQuery)(pos, Some(introducedVariables), Some(scopeDependencies))
  }

  def differentRelationships(relVar1: String, relVar2: String): DifferentRelationships =
    DifferentRelationships(varFor(relVar1), varFor(relVar2))(pos)

  def differentRelationships(relVar1: LogicalVariable, relVar2: LogicalVariable): DifferentRelationships =
    DifferentRelationships(relVar1, relVar2)(pos)

  def noneOfRels(relVar: LogicalVariable, relListVar: LogicalVariable): NoneOfRelationships =
    NoneOfRelationships(relVar, relListVar)(pos)

  def unique(list: Expression): Unique =
    Unique(list)(pos)

  def isRepeatTrailUnique(rel: String): IsRepeatTrailUnique =
    IsRepeatTrailUnique(varFor(rel))(pos)

  def disjoint(lhs: Expression, rhs: Expression): Disjoint =
    Disjoint(lhs, rhs)(pos)

  def varLengthLowerLimitPredicate(relVar: String, limit: Long): VarLengthLowerBound =
    VarLengthLowerBound(varFor(relVar), limit)(pos)

  def varLengthUpperLimitPredicate(relVar: String, limit: Long): VarLengthUpperBound =
    VarLengthUpperBound(varFor(relVar), limit)(pos)

  def foreach(variable: String, listExpr: Expression, updates: Clause*): Foreach =
    Foreach(varFor(variable), listExpr, updates)(pos)

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
    def all: UnionAll = UnionAll(u.lhs, u.rhs)(pos)
  }

  implicit class NonPrefixedPatternPartOps(part: NonPrefixedPatternPart) {

    def withAllPathsSelector: PatternPartWithSelector =
      PatternPartWithSelector(allPathsSelector(), part)
  }

  def increasePos(position: InputPosition, inc: Int): InputPosition = {
    InputPosition(position.offset + inc, position.line, position.column + inc)
  }

  /**
   * Small utility to build PathExpressions.
   */
  object PathExpressionBuilder {
    def node(name: String): PathExpressionBuilder = PathExpressionBuilder(Seq(name))
  }

  /**
   * @param nodes the nodes
   * @param rels tuples for each relationship with (name, direction, isVarLength)
   */
  case class PathExpressionBuilder private (
    nodes: Seq[String] = Seq.empty,
    rels: Seq[(String, SemanticDirection, Boolean)] = Seq.empty
  ) {

    def outTo(relName: String, nodeName: String): PathExpressionBuilder =
      copy(nodes = nodes :+ nodeName, rels = rels :+ (relName, OUTGOING, false))

    def bothTo(relName: String, nodeName: String): PathExpressionBuilder =
      copy(nodes = nodes :+ nodeName, rels = rels :+ (relName, BOTH, false))

    def inTo(relName: String, nodeName: String): PathExpressionBuilder =
      copy(nodes = nodes :+ nodeName, rels = rels :+ (relName, INCOMING, false))

    def outToVarLength(relName: String, nodeName: String): PathExpressionBuilder =
      copy(nodes = nodes :+ nodeName, rels = rels :+ (relName, OUTGOING, true))

    def bothToVarLength(relName: String, nodeName: String): PathExpressionBuilder =
      copy(nodes = nodes :+ nodeName, rels = rels :+ (relName, BOTH, true))

    def inToVarLength(relName: String, nodeName: String): PathExpressionBuilder =
      copy(nodes = nodes :+ nodeName, rels = rels :+ (relName, INCOMING, true))

    def build(): PathExpression = {
      @tailrec
      def nextStep(
        reversedNodes: List[String],
        reversedRels: List[(String, SemanticDirection, Boolean)],
        currentPathStep: PathStep
      ): PathStep = {
        (reversedNodes, reversedRels) match {
          case (Nil, Nil) =>
            currentPathStep
          case (node :: nodeTail, Nil) =>
            val step = NodePathStep(varFor(node), currentPathStep)(pos)
            nextStep(nodeTail, Nil, step)
          case (node :: nodeTail, rel :: relTail) =>
            val step = rel match {
              case (relName, direction, false) =>
                SingleRelationshipPathStep(varFor(relName), direction, Some(varFor(node)), currentPathStep)(pos)
              case (relName, direction, true) =>
                MultiRelationshipPathStep(varFor(relName), direction, Some(varFor(node)), currentPathStep)(pos)
            }
            nextStep(nodeTail, relTail, step)
          case _ => throw new InternalError("there should never be more relationships than nodes")
        }
      }

      val pathStep = nextStep(nodes.reverse.toList, rels.reverse.toList, NilPathStep()(pos))
      PathExpression(pathStep)(pos)
    }
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
