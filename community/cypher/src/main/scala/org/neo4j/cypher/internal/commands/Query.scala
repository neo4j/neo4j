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


object Query {
  def start(startItems: StartItem*) = new QueryBuilder(startItems)
}

case class Query(returns: Return,
                 start: Start,
                 matching: Option[Match],
                 where: Option[Predicate],
                 aggregation: Option[Aggregation],
                 sort: Option[Sort],
                 slice: Option[Slice],
                 namedPaths: Option[NamedPaths],
                 having: Option[Predicate],
                 queryString: String = "") {
  override def equals(p1: Any): Boolean =
    if (p1 == null)
      false
    else if (!p1.isInstanceOf[Query])
      false
    else {
      val other = p1.asInstanceOf[Query]
      returns == other.returns &&
        start == other.start &&
        matching == other.matching &&
        where == other.where &&
        aggregation == other.aggregation &&
        sort == other.sort &&
        slice == other.slice &&
        namedPaths == other.namedPaths &&
        having == other.having
    }
}

case class Return(columns: List[String], returnItems: ReturnItem*)

case class Start(startItems: StartItem*)

case class Match(patterns: Pattern*)

case class NamedPaths(paths: NamedPath*)

case class Aggregation(aggregationItems: AggregationExpression*)

case class Sort(sortItems: SortItem*)

case class Slice(from: Option[Expression], limit: Option[Expression])