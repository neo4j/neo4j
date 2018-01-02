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

import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{True, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.mutation._
import expressions.{Expression, AggregationExpression}
import org.neo4j.cypher.internal.compiler.v2_3.commands
import scala.annotation.tailrec

object Query {
  def start(startItems: StartItem*) = new QueryBuilder().startItems(startItems:_*)
  def matches(patterns:Pattern*) = new QueryBuilder().matches(patterns:_*)
  def optionalMatches(patterns:Pattern*) = new QueryBuilder().matches(patterns:_*).makeOptional()
  def updates(cmds:UpdateAction*) = new QueryBuilder().updates(cmds:_*)
  def unique(cmds:UniqueLink*) = new QueryBuilder().startItems(Seq(CreateUniqueStartItem(CreateUniqueAction(cmds:_*))):_*)

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
  def setQueryText(t: String): AbstractQuery
  def getQueryText: String = queryString.text
}

case class PeriodicCommitQuery(query: AbstractQuery, batchSize: Option[Long]) extends AbstractQuery {

  override def setQueryText(t: String): AbstractQuery = {
    query.setQueryText(t)
    this
  }

  override def queryString: QueryString = query.queryString
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
    @tailrec
    def allTails(acc: Vector[Query], query: Query): Vector[Query] = query.tail match {
      case Some(remaining) => allTails(acc :+ query.copy(tail = None), remaining)
      case None            => acc :+ query
    }

    def isMergeAction(action: UpdateAction) =
      action.isInstanceOf[MergeNodeAction] || action.isInstanceOf[MergePatternAction]

    allTails(Vector.empty, this).reduceRight[Query] {
      case (head, remaining) =>
        if (head.compactableStart &&
          remaining.compactableTail &&
          // Merges are both reads and writes, and can't be compacted with something else
          !remaining.updatedCommands.exists(isMergeAction) &&
          // If we have updating actions, we can't merge with a tail part that has updating start items
          // That would mess with the order of actions
          !(head.updatedCommands.nonEmpty && remaining.start.exists(_.mutating))) {
          head.compactWith(remaining)
        } else
          head.copy(tail = Some(remaining))
    }
  }

  private def compactableStart =
    compactable && returns == Return(List("*"), AllIdentifiers())

  private def compactableTail =
    compactable && aggregation.isEmpty && !updatedCommands.exists(containsMergeForPattern)

  private def compactable =
    start.forall(!_.isInstanceOf[CreateUniqueStartItem]) &&
    hints.isEmpty &&
    matching.isEmpty &&
    sort.isEmpty &&
    slice.isEmpty &&
    where == True() &&
    start.forall(_.mutating)

  private def compactWith(other: Query) =
    commands.Query(
      hints = hints ++ other.hints,
      start = start ++ other.start,
      returns = other.returns,
      updatedCommands = updatedCommands ++ other.updatedCommands,
      matching = Seq(),
      optional = false,
      where = True(),
      aggregation = None,
      sort = Seq(),
      slice = None,
      namedPaths = namedPaths ++ other.namedPaths,
      tail = other.tail
    )

  private def containsMergeForPattern(action: UpdateAction): Boolean = action match {
    case _: MergePatternAction => true
    case ForeachAction(_, _, actions) => actions.exists(containsMergeForPattern)
    case _ => false
  }

  def includeIfNotEmpty(title:String, objects:Iterable[_]):String = if(objects.isEmpty) "" else
    String.format("%s%s%n", title, objects.mkString(", "))

  override def toString: String =  "\n" +
    includeIfNotEmpty("start  : ", start) +
      includeIfNotEmpty("updates: ", updatedCommands) +
      includeIfNotEmpty((if(optional) "optional " else "") + "match  : ", matching) +
      includeIfNotEmpty("paths  : ", namedPaths) +
      includeIfNotEmpty("hints  : ", hints) +
      (if (where == True()) "" else "where  : " + where.toString + "\n") +
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
