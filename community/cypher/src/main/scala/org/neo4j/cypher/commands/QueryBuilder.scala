/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.commands

class QueryBuilder(startItems: Seq[ StartItem ]) {
  var matching: Option[ Match ] = None
  var where: Option[ Clause ] = None
  var aggregation: Option[ Aggregation ] = None
  var orderBy: Option[ Sort ] = None
  var skip: Option[ Value ] = None
  var limit: Option[ Value ] = None
  var namedPaths: Option[ NamedPaths ] = None
  var columns: Seq[ ReturnItem ] => List[ String ] = (returnItems) => returnItems.map(_.identifier.name).toList

  def matches(patterns: Pattern*): QueryBuilder = store(() => matching = Some(Match(patterns: _*)))

  def where(clause: Clause): QueryBuilder = store(() => where = Some(clause))

  def aggregation(aggregationItems: AggregationItem*): QueryBuilder =
    store(() => aggregation = Some(Aggregation(aggregationItems: _*)))

  def orderBy(sortItems: SortItem*): QueryBuilder = store(() => orderBy = Some(Sort(sortItems: _*)))

  def skip(skipTo: Int): QueryBuilder = store(() => skip = Some(Literal(skipTo)))

  def skip(skipTo: String): QueryBuilder = store(() => skip = Some(ParameterValue(skipTo)))

  def limit(limitTo: Int): QueryBuilder = store(() => limit = Some(Literal(limitTo)))

  def limit(limitTo: String): QueryBuilder = store(() => limit = Some(ParameterValue(limitTo)))

  def namedPaths(paths: NamedPath*): QueryBuilder = store(() => namedPaths = Some(NamedPaths(paths: _*)))

  def columns(columnList: String*): QueryBuilder = store(() => columns = (x) => columnList.toList  )

  def slice: Option[ Slice ] = (skip, limit) match {
    case (None, None) => None
    case (s, l)       => Some(Slice(skip, limit))
  }

  private def store(f: () => Unit): QueryBuilder = {
    f()
    this
  }

  def returns(returnItems: ReturnItem*): Query =
    Query(Return(columns(returnItems), returnItems: _*), Start(startItems: _*), matching, where, aggregation,
          orderBy, slice, namedPaths)
}