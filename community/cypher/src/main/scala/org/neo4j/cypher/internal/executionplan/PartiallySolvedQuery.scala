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
package org.neo4j.cypher.internal.executionplan

import org.neo4j.cypher.internal.commands._
import collection.Seq
import org.neo4j.helpers.ThisShouldNotHappenError


object PartiallySolvedQuery {

  // Creates a fully unsolved query
  def apply(q: Query) = {
    val patterns = q.matching.toSeq.flatMap(_.patterns.map(Unsolved(_))) ++
      q.namedPaths.toSeq.flatMap(_.paths.flatMap(_.pathPattern.map(Unsolved(_))))

    new PartiallySolvedQuery(
      returns = q.returns.returnItems.map(Unsolved(_)),
      start = q.start.startItems.map(Unsolved(_)),
      patterns = patterns,
      where = q.where.toSeq.flatMap(_.atoms.map(Unsolved(_))),
      aggregation = q.aggregation.toSeq.flatMap(_.aggregationItems.map(Unsolved(_))),
      sort = q.sort.toSeq.flatMap(_.sortItems.map(Unsolved(_))),
      slice = q.slice.toSeq.map(Unsolved(_)),
      namedPaths = q.namedPaths.toSeq.flatMap(_.paths.map(Unsolved(_))),
      aggregateQuery = if (q.aggregation.isDefined)
        Unsolved(true)
      else
        Solved(false),
      extracted = false
    )
  }


  def apply() = new PartiallySolvedQuery(
    returns = Seq(),
    start = Seq(),
    patterns = Seq(),
    where = Seq(),
    aggregation = Seq(),
    sort = Seq(),
    slice = Seq(),
    namedPaths = Seq(),
    aggregateQuery = Solved(false),
    extracted = false
  )
}

/*
PSQ is used to keep count of which parts of the query that have already been
solved, and which parts are not yet finished.
 */
case class PartiallySolvedQuery(returns: Seq[QueryToken[ReturnItem]],
                                start: Seq[QueryToken[StartItem]],
                                patterns: Seq[QueryToken[Pattern]],
                                where: Seq[QueryToken[Predicate]],
                                aggregation: Seq[QueryToken[AggregationExpression]],
                                sort: Seq[QueryToken[SortItem]],
                                slice: Seq[QueryToken[Slice]],
                                namedPaths: Seq[QueryToken[NamedPath]],
                                aggregateQuery: QueryToken[Boolean],
                                extracted: Boolean) {

  def isSolved = returns.filterNot(_.solved).isEmpty &&
    start.filterNot(_.solved).isEmpty &&
    patterns.filterNot(_.solved).isEmpty &&
    where.filterNot(_.solved).isEmpty &&
    aggregation.filterNot(_.solved).isEmpty &&
    sort.filterNot(_.solved).isEmpty &&
    slice.filterNot(_.solved).isEmpty &&
    namedPaths.filterNot(_.solved).isEmpty

  def readyToAggregate = !(start.exists(_.unsolved) ||
    patterns.exists(_.unsolved) ||
    where.exists(_.unsolved) ||
    namedPaths.exists(_.unsolved))

  def rewrite(f: Expression => Expression):PartiallySolvedQuery = {
    this.copy(
      returns = returns.map {
        case Unsolved(ReturnItem(expression, name)) => Unsolved(ReturnItem(expression.rewrite(f), name))
        case x => x
      },
      where = where.map {
        case Unsolved(pred) => Unsolved(pred.rewrite(f))
        case x => x
      },
      sort = sort.map {
        case Unsolved(SortItem(expression, asc)) => Unsolved(SortItem(expression.rewrite(f),asc))
        case x => x
      },
      patterns = patterns.map {
        case Unsolved(p) => Unsolved(p.rewrite(f))
        case x => x
      },
      aggregation = aggregation.map {
        case Unsolved(exp) => Unsolved(exp.rewrite(f) match {
                  case x: AggregationExpression => x
                  case _ => throw new ThisShouldNotHappenError("Andrés & Michael","aggregation expressions should never be rewritten to non-aggregation-expressions")
        })
        case x => x
      },
      namedPaths = namedPaths.map {
        case Unsolved(namedPath) => Unsolved(namedPath.rewrite(f))
        case x => x
      }
    )
  }

  def unsolvedExpressions = {
    val rExpressions = returns.flatMap {
      case Unsolved(ReturnItem(expression, _)) => expression.filter( e=>true )
      case _ => None
    }

    val wExpressions = where.flatMap {
      case Unsolved(pred) => pred.filter( e=>true )
      case _ => Seq()
    }

    val aExpressions = aggregation.flatMap {
      case Unsolved(expression) => expression.filter( e=>true )
      case _ => Seq()
    }

   rExpressions ++ wExpressions ++ aExpressions
  }
}

abstract class QueryToken[T](val token: T) {
  def solved: Boolean

  def unsolved = !solved

  def solve: QueryToken[T] = Solved(token)
}

case class Solved[T](t: T) extends QueryToken[T](t) {
  val solved = true
}

case class Unsolved[T](t: T) extends QueryToken[T](t) {
  val solved = false
}