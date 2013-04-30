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
  def matches(patterns:Pattern*) = new QueryBuilder(Seq.empty).matches(patterns:_*)
  def updates(cmds:UpdateAction*) = new QueryBuilder(Seq()).updates(cmds:_*)
  def unique(cmds:UniqueLink*) = new QueryBuilder(Seq(CreateUniqueStartItem(CreateUniqueAction(cmds:_*))))

  def empty = Query(
    start = Seq.empty,
    updatedCommands = Seq.empty,
    matching = Seq.empty,
    hints = Seq.empty,
    sort = Seq.empty,
    namedPaths = Seq.empty,
    where = True(),
    slice = None,
    aggregation = None,
    returns = Return(columns = List.empty)
  )
}

trait AbstractQuery {
  def queryString: QueryString
  def setQueryText(t:String):AbstractQuery
  def getQueryText: String = queryString.text
  def verifySemantics() {}
}

case class Query(returns: Return,
                 start: Seq[StartItem],
                 updatedCommands:Seq[UpdateAction],
                 matching: Seq[Pattern],
                 hints:Seq[SchemaIndex],
                 where: Predicate,
                 aggregation: Option[Seq[AggregationExpression]],
                 sort: Seq[SortItem],
                 slice: Option[Slice],
                 namedPaths: Seq[NamedPath],
                 tail:Option[Query] = None,
                 queryString: QueryString = QueryString.empty) extends AbstractQuery {

  override def toString: String =
    """
start  : %s
updates: %s
match  : %s
paths  : %s
hints  : %s
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
  hints,
  where,
  aggregation,
  returns.returnItems.mkString(","),
  sort,
  slice,
  tail
)

  def setQueryText(t: String): Query = copy(queryString = QueryString(t))

  def columns: List[String] = {
    var last: Query = this

    while (last.tail.nonEmpty)
      last = last.tail.get

    last.returns.columns
  }

}

case class Return(columns: List[String], returnItems: ReturnColumn*)

case class Slice(from: Option[Expression], limit: Option[Expression])