/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import expressions.{ParameterExpression, Literal, Expression, AggregationExpression}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{True, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.UpdateAction

case class QueryBuilder(
  startItems: Seq[StartItem] = Seq(),
  updates: Seq[UpdateAction] = Seq(),
  matching: Seq[Pattern] = Seq(),
  where: Predicate = True(),
  optional: Boolean = false,
  aggregation: Option[Seq[AggregationExpression]] = None,
  orderBy: Seq[SortItem] = Seq(),
  skip: Option[Expression] = None,
  limit: Option[Expression] = None,
  namedPaths: Seq[NamedPath] = Seq(),
  using: Seq[StartItem with Hint] = Seq(),
  tail: Option[Query] = None,
  columns: Seq[ReturnColumn] => List[String] = (returnItems) => returnItems.map(_.name).toList) {

  def startItems(items: StartItem*): QueryBuilder = copy(startItems = items)

  def matches(patterns: Pattern*): QueryBuilder = copy(matching = patterns)

  def makeOptional() = copy(optional = true)

  def isOptional(opt:Boolean) = copy(optional = opt)

  def updates(cmds: UpdateAction*): QueryBuilder = copy(updates = cmds)

  def using(indexHints: StartItem with Hint*): QueryBuilder = copy(using = indexHints.toSeq)

  def where(predicate: Predicate): QueryBuilder = copy(where = predicate)

  def aggregation(aggregationItems: AggregationExpression*): QueryBuilder = copy(aggregation = Some(aggregationItems))

  def orderBy(sortItems: SortItem*): QueryBuilder = copy(orderBy = sortItems)

  def skip(skipTo: Int): QueryBuilder = copy(skip = Some(Literal(skipTo)))
  def skip(skipTo: String): QueryBuilder = copy(skip = Some(ParameterExpression(skipTo)))
  def skip(skipTo: Expression): QueryBuilder = copy(skip = Some(skipTo))

  def limit(limitTo: Int): QueryBuilder = copy(limit = Some(Literal(limitTo)))
  def limit(limitTo: String): QueryBuilder = copy(limit = Some(ParameterExpression(limitTo)))
  def limit(limitTo: Expression): QueryBuilder = copy(limit = Some(limitTo))

  def namedPaths(paths: NamedPath*): QueryBuilder = copy(namedPaths = paths)

  def columns(columnList: String*): QueryBuilder = copy(columns = (x) => columnList.toList)

  def tail(q: Query): QueryBuilder = copy(tail = Some(q))

  def slice: Option[Slice] = (skip, limit) match {
    case (None, None) => None
    case (s, l) => Some(Slice(skip, limit))
  }

  def returns(returnItems: ReturnColumn*): Query = if (optional && startItems.nonEmpty) {
    /*
    A Query is either all optional, or all mandatory.

    Given a query: START n=node(0) OPTIONAL MATCH n-->x RETURN x

    The Query object representation needs to have the START items in one Query, and the OPTIONAL
    MATCH patterns in another, so the START doesn't become optional.

    TODO: Kill the commands.Query class
     */
    val allIdentifierss = Seq[ReturnColumn](AllIdentifiers())

    val secondPart = Query(Return(columns(returnItems), returnItems: _*), Seq.empty, updates, matching, optional,
      using, where, aggregation, orderBy, slice, namedPaths, tail)

    Query.empty.copy(
      returns = Return(columns(allIdentifierss), allIdentifierss:_*),
      start = startItems,
      tail = Some(secondPart))
  } else
    Query(Return(columns(returnItems), returnItems: _*), startItems, updates, matching, optional,
      using, where, aggregation, orderBy, slice, namedPaths, tail)
}
