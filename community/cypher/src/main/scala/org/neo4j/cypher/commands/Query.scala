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


object Query {
  def start(startItems: StartItem*) = new QueryBuilder(startItems)
}

case class Query(returns: Return, start: Start, matching: Option[Match], where: Option[Clause], aggregation: Option[Aggregation],
                 sort: Option[Sort], slice: Option[Slice], namedPaths: Option[NamedPaths])

case class Return(returnItems: ReturnItem*)

case class Start(startItems: StartItem*)

case class Match(patterns: Pattern*)

case class NamedPaths(paths: NamedPath*) extends Traversable[Pattern] {
  def foreach[U](f: (Pattern) => U) {
    paths.flatten.foreach(f)
  }
}

case class Aggregation(aggregationItems: AggregationItem*)

case class Sort(sortItems: SortItem*)

case class Slice(from: Option[Int], limit: Option[Int])