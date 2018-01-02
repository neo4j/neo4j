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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{AggregationExpression, Expression}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{True, Predicate}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.{PatternGraphBuilder, QueryToken, Unsolved}
import org.neo4j.cypher.internal.compiler.v2_3.mutation.UpdateAction
import org.neo4j.cypher.internal.compiler.v2_3.pipes.Pipe
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.helpers.ThisShouldNotHappenError

import scala.collection.Seq


object PartiallySolvedQuery {

  // Creates a fully unsolved query
  def apply(in:Query): PartiallySolvedQuery = {

    val q = in.compact

    val patterns = q.matching.map(Unsolved(_))

    val predicates = if (q.where == True()) Seq()
    else q.where.atoms.map(Unsolved(_))

    new PartiallySolvedQuery(
      returns = q.returns.returnItems.map(Unsolved(_)),
      start = (q.start ++ q.hints).map(Unsolved(_)),
      updates = q.updatedCommands.map(Unsolved(_)),
      patterns = patterns,
      where = predicates,
      aggregation = q.aggregation.toSeq.flatten.map(Unsolved(_)),
      sort = q.sort.map(Unsolved(_)),
      slice = q.slice.map(Unsolved(_)),
      namedPaths = q.namedPaths.map(Unsolved(_)),
      aggregateToDo = q.aggregation.isDefined,
      extracted = false,
      optional = q.optional,
      tail = q.tail.map(q => PartiallySolvedQuery(q))
    )
  }

  def apply() = new PartiallySolvedQuery(
    returns = Seq(),
    start = Seq(),
    updates = Seq(),
    patterns = Seq(),
    where = Seq(),
    aggregation = Seq(),
    sort = Seq(),
    slice = None,
    namedPaths = Seq(),
    aggregateToDo = false,
    extracted = false,
    optional = false,
    tail = None
  )
}

/**
 * PSQ is used to keep count of which parts of the query that have already been
 * solved, and which parts are not yet finished. The PSQ has two states - it
 * is either building up necessary parts to create a projection (RETURN/WITH), or
 * it has done the projection and is doing post-projection  work (ORDER BY/LIMIT/SKIP)
 *
 * @param extracted shows whether the query has created a projection or not
 */
case class PartiallySolvedQuery(returns: Seq[QueryToken[ReturnColumn]],
                                start: Seq[QueryToken[StartItem]],
                                updates: Seq[QueryToken[UpdateAction]],
                                patterns: Seq[QueryToken[Pattern]],
                                where: Seq[QueryToken[Predicate]],
                                aggregation: Seq[QueryToken[AggregationExpression]],
                                sort: Seq[QueryToken[SortItem]],
                                slice: Option[QueryToken[Slice]],
                                namedPaths: Seq[QueryToken[NamedPath]],
                                aggregateToDo: Boolean,
                                extracted: Boolean,
                                optional: Boolean,
                                tail: Option[PartiallySolvedQuery]) extends EffectfulAstNode[PartiallySolvedQuery] with PatternGraphBuilder  {

  val matchPattern : MatchPattern = MatchPattern(patterns.map(_.token))

  def isSolved = returns.forall(_.solved) &&
    start.forall(_.solved) &&
    updates.forall(_.solved) &&
    patterns.forall(_.solved) &&
    where.forall(_.solved) &&
    aggregation.forall(_.solved) &&
    sort.forall(_.solved) &&
    slice.forall(_.solved) &&
    namedPaths.forall(_.solved)

  def readyToAggregate = !(start.exists(_.unsolved) ||
    patterns.exists(_.unsolved) ||
    where.exists(_.unsolved) ||
    namedPaths.exists(_.unsolved) ||
    updates.exists(_.unsolved))

  def rewrite(f: Expression => Expression): PartiallySolvedQuery = {
    this.copy(
      returns = returns.map {
        case Unsolved( item @ ReturnItem(expression, name) ) => Unsolved[ReturnColumn](ReturnItem(expression.rewrite(f), name))
        case x => x
      },
      where = where.map {
        case Unsolved(pred) => Unsolved(pred.rewriteAsPredicate(f))
        case x => x
      },
      updates = updates.map {
        case Unsolved(cmd)  => Unsolved(cmd.rewrite(f))
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
          case _                        => throw new ThisShouldNotHappenError("Andrés & Michael", "aggregation expressions should never be rewritten to non-aggregation-expressions")
        })
        case x => x
      },
      namedPaths = namedPaths.map {
        case Unsolved(namedPath) => Unsolved(namedPath.rewrite(f))
        case x => x
      },
      start = start.map { (qt: QueryToken[StartItem]) => qt.map( _.rewrite(f) ) } )
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

    val updateExpressions = updates.flatMap {
      case Unsolved(cmd)  => cmd.filter(e=>true)
      case _ => Seq()
    }

   rExpressions ++ wExpressions ++ aExpressions ++ updateExpressions
  }

  def children = {
    val returnExpressions = returns.flatMap(_.token.expressions(SymbolTable()).map(_._2))
    val wherePredicates = where.map(_.token)
    val aggregateExpressions = aggregation.map(_.token)
    val sortExpressions = sort.map(_.token.expression)
    val tailNodes = tail.toSeq.flatMap(_.children)
    val startItems = start.map(_.token)
    val patternsX = patterns.map(_.token)

    returnExpressions ++ wherePredicates ++ aggregateExpressions ++ sortExpressions ++ tailNodes ++ startItems ++ patternsX
  }

  def containsAggregation: Boolean = aggregation.nonEmpty || tail.exists(_.containsAggregation)

  /* This methods is used to rewrite the queries from the end of the query line to the beginning of it */
  def rewriteFromTheTail(f: PartiallySolvedQuery => PartiallySolvedQuery): PartiallySolvedQuery =
    f(copy(tail = tail.map(_.rewriteFromTheTail(f))))

  def localEffects(symbols: SymbolTable) = Effects()
}

case class ExecutionPlanInProgress(query: PartiallySolvedQuery, pipe: Pipe, isUpdating: Boolean = false)
