/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.cypher.internal.mutation.{CreateUniqueAction, UniqueLink, UpdateAction}
import expressions.{Expression, AggregationExpression}

object Query {
  def start(startItems: StartItem*) = new QueryBuilder(startItems)
  def updates(cmds:UpdateAction*) = new QueryBuilder(Seq()).updates(cmds:_*)
  def unique(cmds:UniqueLink*) = new QueryBuilder(Seq(CreateUniqueStartItem(CreateUniqueAction(cmds:_*))))
}

trait AbstractQuery {
  def setQueryString(t:String):AbstractQuery
}

case class Query(returns: Return,
                 start: Seq[StartItem],
                 updatedCommands:Seq[UpdateAction],
                 matching: Seq[Pattern],
                 where: Option[Predicate],
                 aggregation: Option[Seq[AggregationExpression]],
                 sort: Seq[SortItem],
                 slice: Option[Slice],
                 namedPaths: Seq[NamedPath],
                 tail:Option[Query] = None,
                 queryString: String = "") extends AbstractQuery {

  override def equals(p1: Any): Boolean = p1 match {
    case null         => false

    case other: Query => returns == other.returns &&
      start == other.start &&
      updatedCommands == other.updatedCommands &&
      matching == other.matching &&
      where == other.where &&
      aggregation == other.aggregation &&
      sort == other.sort &&
      slice == other.slice &&
      namedPaths == other.namedPaths &&
      tail == other.tail

    case _            => false
  }

  override def toString: String =
"""
start  : %s
updates: %s
match  : %s
paths  : %s
where  : %s
aggreg : %s
return : %s
order  : %s
slice  : %s
next   : %s
""".format(
  start.mkString(","),
  updatedCommands.mkString(","),
  matching,
  namedPaths,
  where,
  aggregation,
  returns.returnItems.mkString(","),
  sort,
  slice,
  tail
)

  def setQueryString(t: String): Query = copy(queryString = t)
}

case class Return(columns: List[String], returnItems: ReturnColumn*)

case class Slice(from: Option[Expression], limit: Option[Expression])