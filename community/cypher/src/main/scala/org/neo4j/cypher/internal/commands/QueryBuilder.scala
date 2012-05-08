/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands

class QueryBuilder(startItems: Seq[StartItem]) {
  var updates = Seq[UpdateCommand]()
  var matching: Option[Match] = None
  var where: Option[Predicate] = None
  var aggregation: Option[Aggregation] = None
  var orderBy: Option[Sort] = None
  var skip: Option[Expression] = None
  var limit: Option[Expression] = None
  var namedPaths: Option[NamedPaths] = None
  var tail: Option[Query] = None
  var columns: Seq[ReturnColumn] => List[String] = (returnItems) => returnItems.map(_.name).toList

  def matches(patterns: Pattern*): QueryBuilder = store {
    matching = Some(Match(patterns: _*))
  }

  def updates(cmds: UpdateCommand*): QueryBuilder = store {
    updates = cmds
  }

  def where(predicate: Predicate): QueryBuilder = store {
    where = Some(predicate)
  }

  def aggregation(aggregationItems: AggregationExpression*): QueryBuilder = store {
    aggregation = Some(Aggregation(aggregationItems: _*))
  }

  def orderBy(sortItems: SortItem*): QueryBuilder = store {
    orderBy = Some(Sort(sortItems: _*))
  }

  def skip(skipTo: Int): QueryBuilder = store {
    skip = Some(Literal(skipTo))
  }

  def skip(skipTo: String): QueryBuilder = store {
    skip = Some(ParameterExpression(skipTo))
  }

  def limit(limitTo: Int): QueryBuilder = store {
    limit = Some(Literal(limitTo))
  }

  def limit(limitTo: String): QueryBuilder = store {
    limit = Some(ParameterExpression(limitTo))
  }

  def namedPaths(paths: NamedPath*): QueryBuilder = store {
    namedPaths = Some(NamedPaths(paths: _*))
  }

  def columns(columnList: String*): QueryBuilder = store {
    columns = (x) => columnList.toList
  }

  def tail(q: Query): QueryBuilder = store {
    tail = Some(q)
  }

  def slice: Option[Slice] = (skip, limit) match {
    case (None, None) => None
    case (s, l) => Some(Slice(skip, limit))
  }

  private def store(f: => Unit): QueryBuilder = {
    f
    this
  }

  def returns(returnItems: ReturnColumn*): Query =
    Query(Return(columns(returnItems), returnItems: _*), Start(startItems: _*), updates, matching, where, aggregation, orderBy, slice, namedPaths, tail)
}