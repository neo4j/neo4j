/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import java.util
import java.util.stream.Collectors

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Create
import org.neo4j.cypher.internal.ast.Delete
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.LoadCSV
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.OnCreate
import org.neo4j.cypher.internal.ast.OnMatch
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.PeriodicCommitHint
import org.neo4j.cypher.internal.ast.ProcedureResult
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Remove
import org.neo4j.cypher.internal.ast.RemoveItem
import org.neo4j.cypher.internal.ast.RemoveLabelItem
import org.neo4j.cypher.internal.ast.RemovePropertyItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.SeekOrScan
import org.neo4j.cypher.internal.ast.SetClause
import org.neo4j.cypher.internal.ast.SetExactPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetIncludingPropertiesFromMapItem
import org.neo4j.cypher.internal.ast.SetItem
import org.neo4j.cypher.internal.ast.SetLabelItem
import org.neo4j.cypher.internal.ast.SetPropertyItem
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.SubQuery
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.UsingHint
import org.neo4j.cypher.internal.ast.UsingJoinHint
import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.factory.ASTFactory
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AnonymousPatternPart
import org.neo4j.cypher.internal.expressions.AnyIterablePredicate
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ExtractExpression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FilterExpression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.MapProjectionElement
import org.neo4j.cypher.internal.expressions.Modulo
import org.neo4j.cypher.internal.expressions.Multiply
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NoneIterablePredicate
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.Pow
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.ProcedureOutput
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertySelector
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.RegexMatch
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SingleIterablePredicate
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnaryAdd
import org.neo4j.cypher.internal.expressions.UnarySubtract
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableSelector
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTAny

import scala.collection.JavaConverters.asScalaBufferConverter

class Neo4jASTFactory(query: String)
  extends ASTFactory[Statement,
                     Query,
                     Clause,
                     Return,
                     ReturnItem,
                     SortItem,
                     PatternPart,
                     NodePattern,
                     RelationshipPattern,
                     Option[Range],
                     SetClause,
                     SetItem,
                     RemoveItem,
                     ProcedureResultItem,
                     UsingHint,
                     Expression,
                     Variable,
                     Property,
                     MapProjectionElement,
                     InputPosition] {

  private lazy val lines = query.split(System.lineSeparator())

  override def newSingleQuery(clauses: util.List[Clause]): Query = {
    if (clauses.isEmpty) {
      throw new Neo4jASTConstructionException("A valid Cypher query has to contain at least 1 clause")
    }
    val pos = clauses.get(0).position
    Query(None, SingleQuery(clauses.asScala.toList)(pos))(pos)
  }

  override def newUnion(p: InputPosition,
                        lhs: Query,
                        rhs: Query,
                        all: Boolean): Query = {
    val rhsQuery =
      rhs.part match {
        case x: SingleQuery => x
        case other =>
          throw new Neo4jASTConstructionException(
            s"The Neo4j AST encodes Unions as a left-deep tree, so the rhs query must always be a SingleQuery. Got `$other`")
      }

    val union =
      if (all) UnionAll(lhs.part, rhsQuery)(p)
      else     UnionDistinct(lhs.part, rhsQuery)(p)
    Query(None, union)(p)
  }

  override def periodicCommitQuery(p: InputPosition,
                                   batchSize: String,
                                   loadCsv: Clause,
                                   query: Query): Query =
    Query(Some(PeriodicCommitHint(Option(batchSize).map(UnsignedDecimalIntegerLiteral(_)(p)))(p)),
      query.part match {
        case SingleQuery(clauses) => SingleQuery(loadCsv +: clauses)(p)
        case q => throw new UnsupportedOperationException("Cannot model periodic commit of query: "+q)
      })(p)

  override def newReturnClause(p: InputPosition,
                               distinct: Boolean,
                               returnAll: Boolean,
                               returnItems: util.List[ReturnItem],
                               order: util.List[SortItem],
                               skip: Expression,
                               limit: Expression): Return = {
    val items = ReturnItems(returnAll, returnItems.asScala.toList)(p)
    Return(distinct,
      items,
      if (order.isEmpty) None else Some(OrderBy(order.asScala.toList)(p)),
      Option(skip).map(e => Skip(e)(p)),
      Option(limit).map(e => Limit(e)(p)))(p)
  }

  override def newReturnItem(p: InputPosition, e: Expression,
                             v: Variable): ReturnItem = AliasedReturnItem(e, v)(p)

  override def newReturnItem(p: InputPosition, e: Expression,
                             eStartLine: Int,
                             eStartColumn: Int,
                             eEndLine: Int,
                             eEndColumn: Int): ReturnItem = {

    val name =
      if (eStartLine == eEndLine) lines(eStartLine-1).substring(eStartColumn-1, eEndColumn)
      else {
        val sb = new StringBuilder
        sb ++= lines(eStartLine-1).substring(eStartColumn-1)

        var l = eStartLine + 1
        while (l < eEndLine) {
          sb ++= lines(l)
          l += 1
        }

        sb ++= lines(eEndLine-1).substring(0, eEndColumn-1)
        sb.result()
      }

    UnaliasedReturnItem(e, name)(p)
  }

  override def orderDesc(e: Expression): SortItem = DescSortItem(e)(e.position)

  override def orderAsc(e: Expression): SortItem = AscSortItem(e)(e.position)

  override def createClause(p: InputPosition, patterns: util.List[PatternPart]): Clause =
    Create(Pattern(patterns.asScala.toList)(p))(p)

  override def matchClause(p: InputPosition,
                           optional: Boolean,
                           patterns: util.List[PatternPart],
                           hints: util.List[UsingHint],
                           where: Expression): Clause =
    Match(optional, Pattern(patterns.asScala.toList)(p), if (hints == null) Nil else hints.asScala.toList, Option(where).map(Where(_)(p)))(p)

  override def usingIndexHint(p: InputPosition,
                              v: Variable,
                              label: String,
                              properties: util.List[String],
                              seekOnly: Boolean): UsingHint =
    ast.UsingIndexHint(v,
      LabelName(label)(p),
      properties.asScala.toList.map(PropertyKeyName(_)(p)),
      if (seekOnly) SeekOnly else SeekOrScan)(p)

  override def usingJoin(p: InputPosition, joinVariables: util.List[Variable]): UsingHint =
    UsingJoinHint(joinVariables.asScala.toList)(p)

  override def usingScan(p: InputPosition,
                         v: Variable,
                         label: String): UsingHint =
    UsingScanHint(v, LabelName(label)(p))(p)

  override def withClause(p: InputPosition,
                          r: Return,
                          where: Expression): Clause =
    With(r.distinct,
      r.returnItems,
      r.orderBy,
      r.skip,
      r.limit,
      Option(where).map(e => Where(e)(e.position)))(p)

  override def setClause(p: InputPosition, setItems: util.List[SetItem]): SetClause =
    SetClause(setItems.asScala.toList)(p)

  override def setProperty(property: Property,
                           value: Expression): SetItem = SetPropertyItem(property, value)(property.position)

  override def setVariable(variable: Variable,
                           value: Expression): SetItem = SetExactPropertiesFromMapItem(variable, value)(variable.position)

  override def addAndSetVariable(variable: Variable,
                                 value: Expression): SetItem = SetIncludingPropertiesFromMapItem(variable, value)(variable.position)

  override def setLabels(variable: Variable,
                         labels: util.List[String]): SetItem =
    SetLabelItem(variable, labels.asScala.toList.map(LabelName(_)(variable.position)))(variable.position)

  override def removeClause(p: InputPosition, removeItems: util.List[RemoveItem]): Clause =
    Remove(removeItems.asScala.toList)(p)

  override def removeProperty(property: Property): RemoveItem = RemovePropertyItem(property)

  override def removeLabels(variable: Variable,
                            labels: util.List[String]): RemoveItem =
    RemoveLabelItem(variable, labels.asScala.toList.map(LabelName(_)(variable.position)))(variable.position)

  override def deleteClause(p: InputPosition,
                            detach: Boolean,
                            expressions: util.List[Expression]): Clause = Delete(expressions.asScala.toList, detach)(p)

  override def unwindClause(p: InputPosition,
                            e: Expression,
                            v: Variable): Clause = Unwind(e, v)(p)

  override def mergeClause(p: InputPosition,
                           pattern: PatternPart,
                           onMatch: util.List[SetClause],
                           onCreate: util.List[SetClause]): Clause =
    Merge(Pattern(Seq(pattern))(p),
      onMatch.asScala.toList.map(OnMatch(_)(p)) ++ onCreate.asScala.toList.map(OnCreate(_)(p)))(p)

  override def callClause(p: InputPosition,
                          namespace: util.List[String],
                          name: String,
                          arguments: util.List[Expression],
                          resultItems: util.List[ProcedureResultItem],
                          where: Expression): Clause =
    UnresolvedCall(
      Namespace(namespace.asScala.toList)(p),
      ProcedureName(name)(p),
      if (arguments == null) None else Some(arguments.asScala.toList),
      Option(resultItems).map(items => ProcedureResult(items.asScala.toList.toIndexedSeq, Option(where).map(Where(_)(p)))(p))
    )(p)

  override def callResultItem(name: String,
                              v: Variable): ProcedureResultItem =
    if (v == null) ProcedureResultItem(Variable(name)(InputPosition.NONE))(InputPosition.NONE)
    else ProcedureResultItem(ProcedureOutput(name)(v.position), v)(v.position)

  override def loadCsvClause(p: InputPosition,
                             headers: Boolean,
                             source: Expression,
                             v: Variable,
                             fieldTerminator: String): Clause =
    LoadCSV(headers, source, v, Option(fieldTerminator).map(StringLiteral(_)(p)))(p)

  override def foreachClause(p: InputPosition,
                             v: Variable,
                             list: Expression,
                             clauses: util.List[Clause]): Clause =
    Foreach(v, list, clauses.asScala.toList)(p)

  override def subqueryClause(p: InputPosition, subquery: Query): Clause =
    SubQuery(subquery.part)(p)

  // PATTERNS

  override def namedPattern(v: Variable,
                            pattern: PatternPart): PatternPart =
    NamedPatternPart(v, pattern.asInstanceOf[AnonymousPatternPart])(v.position)

  override def shortestPathPattern(p: InputPosition, pattern: PatternPart): PatternPart = ShortestPaths(pattern.element, true)(p)

  override def allShortestPathsPattern(p: InputPosition, pattern: PatternPart): PatternPart = ShortestPaths(pattern.element, false)(p)

  override def everyPathPattern(nodes: util.List[NodePattern],
                                relationships: util.List[RelationshipPattern]): PatternPart = {

    val nodeIter = nodes.iterator()
    val relIter = relationships.iterator()

    var patternElement: PatternElement = nodeIter.next()
    while (relIter.hasNext) {
      val relPattern = relIter.next()
      val rightNodePattern = nodeIter.next()
      patternElement = RelationshipChain(patternElement, relPattern, rightNodePattern)(nodes.get(0).position)
    }
    EveryPath(patternElement)
  }

  override def nodePattern(p: InputPosition,
                           v: Variable,
                           labels: util.List[String],
                           properties: Expression): NodePattern =
    NodePattern(Option(v), labels.asScala.toList.map(LabelName(_)(p)), Option(properties))(p)

  override def relationshipPattern(p: InputPosition,
                                   left: Boolean,
                                   right: Boolean,
                                   v: Variable,
                                   relTypes: util.List[String],
                                   pathLength: Option[Range],
                                   properties: Expression): RelationshipPattern = {
    val direction =
      if (left && !right) SemanticDirection.INCOMING
      else if (!left && right) SemanticDirection.OUTGOING
      else SemanticDirection.BOTH

    val range =
      pathLength match {
        case null => None
        case None => Some(None)
        case Some(r) => Some(Some(r))
      }

    RelationshipPattern(Option(v), relTypes.asScala.toList.map(RelTypeName(_)(p)), range, Option(properties), direction)(p)
  }

  override def pathLength(p: InputPosition, minLength: String, maxLength: String): Option[Range] = {
    if (minLength == null && maxLength == null) {
      None
    } else {
      val min = if (minLength == "") None else Some(UnsignedDecimalIntegerLiteral(minLength)(p))
      val max = if (maxLength == "") None else Some(UnsignedDecimalIntegerLiteral(maxLength)(p))
      Some(Range(min, max)(p))
    }
  }

  // EXPRESSIONS

  override def newVariable(p: InputPosition, name: String): Variable = Variable(name)(p)

  override def newParameter(p: InputPosition, v: Variable): Expression = Parameter(v.name, CTAny)(p)

  override def newParameter(p: InputPosition, offset: String): Expression = Parameter(offset, CTAny)(p)

  override def newDouble(p: InputPosition, image: String): Expression = DecimalDoubleLiteral(image)(p)

  override def newDecimalInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedDecimalIntegerLiteral("-"+image)(p)
    else SignedDecimalIntegerLiteral(image)(p)

  override def newHexInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedHexIntegerLiteral("-"+image)(p)
    else SignedHexIntegerLiteral(image)(p)

  override def newOctalInteger(p: InputPosition, image: String, negated: Boolean): Expression =
    if (negated) SignedOctalIntegerLiteral("-"+image)(p)
    else SignedOctalIntegerLiteral(image)(p)

  override def newString(p: InputPosition, image: String): Expression = StringLiteral(image)(p)

  override def newTrueLiteral(p: InputPosition): Expression = True()(p)

  override def newFalseLiteral(p: InputPosition): Expression = False()(p)

  override def newNullLiteral(p: InputPosition): Expression = Null()(p)

  override def listLiteral(p: InputPosition, values: util.List[Expression]): Expression = {
    ListLiteral(values.asScala.toList)(p)
  }

  override def mapLiteral(p: InputPosition,
                          keys: util.List[String],
                          values: util.List[Expression]): Expression = {

    if (keys.size() != values.size()) {
      throw new Neo4jASTConstructionException(
        s"Map have the same number of keys and values, but got keys `${pretty(keys)}` and values `${pretty(values)}`")
    }

    var i = 0
    val pairs = new Array[(PropertyKeyName, Expression)](keys.size())

    while (i < keys.size()) {
      pairs(i) = PropertyKeyName(keys.get(i))(p) -> values.get(i)
      i += 1
    }

    MapExpression(pairs)(p)
  }

  override def hasLabels(subject: Expression,
                         labels: util.List[String]): Expression =
    HasLabels(subject, labels.asScala.toList.map(LabelName(_)(subject.position)))(subject.position)

  override def property(subject: Expression,
                        propertyKeyName: String): Property =
    Property(subject, PropertyKeyName(propertyKeyName)(subject.position))(subject.position)

  override def or(lhs: Expression,
                  rhs: Expression): Expression = Or(lhs, rhs)(lhs.position)

  override def xor(lhs: Expression,
                   rhs: Expression): Expression = Xor(lhs, rhs)(lhs.position)

  override def and(lhs: Expression,
                   rhs: Expression): Expression = And(lhs, rhs)(lhs.position)

  override def ands(exprs: util.List[Expression]): Expression = Ands(exprs.asScala.toList.toSet)(exprs.get(0).position)

  override def not(e: Expression): Expression =
    e match {
      case IsNull(e) => IsNotNull(e)(e.position)
      case _ => Not(e)(e.position)
    }

  override def plus(lhs: Expression,
                    rhs: Expression): Expression = Add(lhs, rhs)(lhs.position)

  override def minus(lhs: Expression,
                     rhs: Expression): Expression = Subtract(lhs, rhs)(lhs.position)

  override def multiply(lhs: Expression,
                        rhs: Expression): Expression = Multiply(lhs, rhs)(lhs.position)

  override def divide(lhs: Expression,
                      rhs: Expression): Expression = Divide(lhs, rhs)(lhs.position)

  override def modulo(lhs: Expression,
                      rhs: Expression): Expression = Modulo(lhs, rhs)(lhs.position)

  override def pow(lhs: Expression,
                   rhs: Expression): Expression = Pow(lhs, rhs)(lhs.position)

  override def unaryPlus(e: Expression): Expression = UnaryAdd(e)(e.position)

  override def unaryMinus(e: Expression): Expression = UnarySubtract(e)(e.position)

  override def eq(lhs: Expression,
                  rhs: Expression): Expression = Equals(lhs, rhs)(lhs.position)

  override def neq(lhs: Expression,
                   rhs: Expression): Expression = NotEquals(lhs, rhs)(lhs.position)

  override def neq2(lhs: Expression,
                    rhs: Expression): Expression = NotEquals(lhs, rhs)(lhs.position)

  override def lte(lhs: Expression,
                   rhs: Expression): Expression = LessThanOrEqual(lhs, rhs)(lhs.position)

  override def gte(lhs: Expression,
                   rhs: Expression): Expression = GreaterThanOrEqual(lhs, rhs)(lhs.position)

  override def lt(lhs: Expression,
                  rhs: Expression): Expression = LessThan(lhs, rhs)(lhs.position)

  override def gt(lhs: Expression,
                  rhs: Expression): Expression = GreaterThan(lhs, rhs)(lhs.position)

  override def regeq(lhs: Expression,
                     rhs: Expression): Expression = RegexMatch(lhs, rhs)(lhs.position)

  override def startsWith(lhs: Expression,
                          rhs: Expression): Expression = StartsWith(lhs, rhs)(lhs.position)

  override def endsWith(lhs: Expression,
                        rhs: Expression): Expression = EndsWith(lhs, rhs)(lhs.position)

  override def contains(lhs: Expression,
                        rhs: Expression): Expression = Contains(lhs, rhs)(lhs.position)

  override def in(lhs: Expression,
                  rhs: Expression): Expression = In(lhs, rhs)(lhs.position)

  override def isNull(e: Expression): Expression = IsNull(e)(e.position)

  override def listLookup(list: Expression,
                          index: Expression): Expression = ContainerIndex(list, index)(index.position)

  override def listSlice(p: InputPosition,
                         list: Expression,
                         start: Expression,
                         end: Expression): Expression = {
    ListSlice(list, Option(start), Option(end))(p)
  }

  override def newCountStar(p: InputPosition): Expression = CountStar()(p)

  override def functionInvocation(p: InputPosition,
                                  namespace: util.List[String],
                                  name: String,
                                  distinct: Boolean,
                                  arguments: util.List[Expression]): Expression = {
    FunctionInvocation(Namespace(namespace.asScala.toList)(p),
      FunctionName(name)(p),
      distinct,
      arguments.asScala.toIndexedSeq)(p)
  }

  override def listComprehension(p: InputPosition,
                                 v: Variable,
                                 list: Expression,
                                 where: Expression,
                                 projection: Expression): Expression =
    ListComprehension(v, list, Option(where), Option(projection))(p)

  override def patternComprehension(p: InputPosition,
                                    v: Variable,
                                    pattern: PatternPart,
                                    where: Expression,
                                    projection: Expression): Expression =
    PatternComprehension(Option(v),
      RelationshipsPattern(pattern.element.asInstanceOf[RelationshipChain])(p),
      Option(where),
      projection)(p, Set.empty)

  override def filterExpression(p: InputPosition,
                                v: Variable,
                                list: Expression,
                                where: Expression): Expression =
    FilterExpression(v, list, Option(where))(p)

  override def extractExpression(p: InputPosition,
                                 v: Variable,
                                 list: Expression,
                                 where: Expression,
                                 projection: Expression): Expression =
    ExtractExpression(v, list, Option(where), Option(projection))(p)

  override def reduceExpression(p: InputPosition,
                                acc: Variable,
                                accExpr: Expression,
                                v: Variable,
                                list: Expression,
                                innerExpr: Expression): Expression =
    ReduceExpression(acc, accExpr, v, list, innerExpr)(p)

  override def allExpression(p: InputPosition,
                             v: Variable,
                             list: Expression,
                             where: Expression): Expression =
    AllIterablePredicate(v, list, Option(where))(p)

  override def anyExpression(p: InputPosition,
                             v: Variable,
                             list: Expression,
                             where: Expression): Expression =
    AnyIterablePredicate(v, list, Option(where))(p)

  override def noneExpression(p: InputPosition,
                              v: Variable,
                              list: Expression,
                              where: Expression): Expression =
    NoneIterablePredicate(v, list, Option(where))(p)

  override def singleExpression(p: InputPosition,
                                v: Variable,
                                list: Expression,
                                where: Expression): Expression =
    SingleIterablePredicate(v, list, Option(where))(p)

  override def patternExpression(pattern: PatternPart): Expression =
    PatternExpression(RelationshipsPattern(pattern.element.asInstanceOf[RelationshipChain])(pattern.position))

  override def existsSubQuery(p: InputPosition,
                              patterns: util.List[PatternPart],
                              where: Expression): Expression =
    ExistsSubClause(Pattern(patterns.asScala.toList)(p), Option(where))(p, Set.empty)

  override def mapProjection(p: InputPosition,
                             v: Variable,
                             items: util.List[MapProjectionElement]): Expression =
    MapProjection(v, items.asScala.toList)(p, None)

  override def mapProjectionLiteralEntry(property: String,
                                         value: Expression): MapProjectionElement =
    LiteralEntry(PropertyKeyName(property)(value.position), value)(value.position)

  override def mapProjectionProperty(property: String): MapProjectionElement =
    PropertySelector(Variable(property)(InputPosition.NONE))(InputPosition.NONE)


  override def mapProjectionVariable(v: Variable): MapProjectionElement =
    VariableSelector(v)(v.position)

  override def mapProjectionAll(p: InputPosition): MapProjectionElement =
    AllPropertiesSelector()(p)

  override def caseExpression(p: InputPosition,
                              e: Expression,
                              whens: util.List[Expression],
                              thens: util.List[Expression],
                              elze: Expression): Expression = {

    if (whens.size() != thens.size()) {
      throw new Neo4jASTConstructionException(
        s"Case expressions have the same number of whens and thens, but got whens `${pretty(whens)}` and thens `${pretty(thens)}`")
    }

    val alternatives = new Array[(Expression, Expression)](whens.size())
    var i = 0
    while (i < whens.size()) {
      alternatives(i) = whens.get(i) -> thens.get(i)
      i += 1
    }
    CaseExpression(Option(e), alternatives, Option(elze))(p)
  }

  override def inputPosition(offset: Int, line: Int, column: Int): InputPosition = InputPosition(offset, line, column)

  private def pretty[T <: AnyRef](ts: util.List[T]): String = {
    ts.stream().map[String](t => t.toString).collect(Collectors.joining( "," ))
  }
}
