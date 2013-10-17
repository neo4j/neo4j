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
import org.neo4j.cypher.internal.commands

object Query {
  def start(startItems: StartItem*) = new QueryBuilder(startItems)
  def matches(patterns:Pattern*) = new QueryBuilder(Seq.empty).matches(patterns:_*)
  def optionalMatches(patterns:Pattern*) = new QueryBuilder(Seq.empty).matches(patterns:_*).makeOptional()
  def updates(cmds:UpdateAction*) = new QueryBuilder(Seq()).updates(cmds:_*)
  def unique(cmds:UniqueLink*) = new QueryBuilder(Seq(CreateUniqueStartItem(CreateUniqueAction(cmds:_*))))

  def empty = Query(
    start = Seq.empty,
    updatedCommands = Seq.empty,
    matching = Seq.empty,
    optional = false,
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
                 optional: Boolean,
                 hints:Seq[StartItem with Hint],
                 where: Predicate,
                 aggregation: Option[Seq[AggregationExpression]],
                 sort: Seq[SortItem],
                 slice: Option[Slice],
                 namedPaths: Seq[NamedPath],
                 tail:Option[Query] = None,
                 queryString: QueryString = QueryString.empty) extends AbstractQuery {

  def compact: Query = {
    val compactableStart = start.forall(_.mutating) &&
        start.forall(!_.isInstanceOf[CreateUniqueStartItem]) &&
        hints.isEmpty &&
        returns == Return(List("*"), AllIdentifiers()) &&
        where == True() &&
        matching.isEmpty &&
        sort.isEmpty &&
        slice.isEmpty

    lazy val tailQ = tail.map(_.compact).get

    val compactableEnd = tail.nonEmpty &&
      tailQ.matching.isEmpty &&
      tailQ.where == True() &&
      tailQ.start.forall(_.mutating) &&
      tailQ.start.forall(!_.isInstanceOf[CreateUniqueStartItem]) &&
      tailQ.hints.isEmpty &&
      tailQ.sort.isEmpty &&
      tailQ.slice.isEmpty &&
      tailQ.aggregation.isEmpty

    if (compactableStart && compactableEnd) {
      val result = commands.Query(
        hints = hints ++ tailQ.hints,
        start = start ++ tailQ.start,
        returns = tailQ.returns,
        updatedCommands = updatedCommands ++ tailQ.updatedCommands,
        matching = Seq(),
        optional = false,
        where = True(),
        aggregation = None,
        sort = Seq(),
        slice = None,
        namedPaths = Seq(),
        tail = tailQ.tail
      )
      result
    } else this

  }

  def includeIfNotEmpty(title:String, objects:Iterable[_]):String = if(objects.isEmpty) "" else
    String.format("%s%s%n", title, objects.mkString(", "))

  override def toString: String =  "\n" +
    includeIfNotEmpty("start  : ", start) +
      includeIfNotEmpty("updates: ", updatedCommands) +
      includeIfNotEmpty((if(optional) "optional " else "") + "match  : ", matching) +
      includeIfNotEmpty("paths  : ", namedPaths) +
      includeIfNotEmpty("hints  : ", hints) +
      (if (where == True()) "" else where.toString) +
      includeIfNotEmpty("aggreg : ", aggregation) +
      includeIfNotEmpty("return : ", returns.returnItems) +
      includeIfNotEmpty("order  : ", sort) +
      includeIfNotEmpty("slice  : ", slice) +
      includeIfNotEmpty("next   : ", tail) + "\n"


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