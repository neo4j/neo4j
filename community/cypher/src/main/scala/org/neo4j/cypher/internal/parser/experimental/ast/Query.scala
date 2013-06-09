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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.SyntaxException
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions}

sealed trait Query extends Statement

case class SingleQuery(
    start: Option[Start],
    matches: Option[Match],
    hints: Seq[Hint],
    where: Option[Where],
    updates: Seq[UpdateClause],
    close: Option[QueryClose],
    token: InputToken) extends Query
{
  def isEmpty = (start, matches, hints, where, updates, close) match {
    case (None, None, Seq(), None, Seq(), None) => true
    case _ => false
  }
  
  def semanticCheck : Seq[SemanticError] = checkUntilConsistent(semanticCheck)
  
  def semanticCheck(implicit d: DummyImplicit) : SemanticCheck = {
    checkStart >>=
    checkConclusion >>=
    (start ++ matches ++ where ++ updates ++ close).semanticCheck
  }

  private def checkStart : SemanticState => Option[SemanticError] = state => {
    if (state.isClean && !where.isEmpty && (start.isEmpty && matches.isEmpty))
      Some(SemanticError("Query must begin with START, MATCH or CREATE", token.startOnly))
    else
      None
  }
  private def checkConclusion : SemanticState => Option[SemanticError] = state => {
    if (updates.isEmpty && close.isEmpty)
      Some(SemanticError("Query must conclude with RETURN, WITH or an update clause", token.endOnly))
    else
      None
  }

  private def checkUntilConsistent(check: SemanticCheck) : Seq[SemanticError] = {
    repeatUntil((SemanticCheckResult.success(SemanticState.clean), 10)) { case (previous, n) =>
      val latest = check(previous.state)
      if (!latest.errors.isEmpty || latest.state == previous.state)
        ((latest, n-1), true)
      else if (n == 0)
        throw new ThisShouldNotHappenError("chris", "Too many semantic check passes")
      else
        ((latest, n-1), !latest.errors.isEmpty || latest.state == previous.state)
    }._1.errors    
  }

  private def repeatUntil[A](seed: A)(f: A => (A, Boolean)): A = f(seed) match {
    case (a, false) => repeatUntil(a)(f)
    case (a, true)  => a
  }

  def toLegacyQuery = toLegacyQuery(true)
  def toLegacyQuery(top: Boolean) = addToLegacyQuery(new commands.QueryBuilder(), top)
  def addToLegacyQuery(builder: commands.QueryBuilder, top: Boolean) : commands.Query = {
    val updateGroups = groupUpdates(updates)

    val rest = if (start.isDefined || matches.isDefined || where.isDefined || updateGroups.isEmpty) {
      val startItems = start match {
        case Some(s) => s.items.map(_.toCommand)
        case None    => Seq()
      }

      val (patterns, namedMatchPaths, patternPredicates) = matches match {
        case Some(Match(ps, _)) => (ps.flatMap(_.toLegacyPatterns), ps.flatMap(_.toLegacyNamedPath), ps.flatMap(_.toLegacyPredicates))
        case None               => (Seq(), Seq(), Seq())
      }

      val indexHints = hints.map(_.toLegacySchemaIndex)

      val wherePredicate = where match {
        case Some(Where(e, _)) => e.toCommand match {
          case p: commands.Predicate => Some(p)
          case _                     => throw new SyntaxException(s"WHERE clause expression must return a boolean (${e.token.startPosition})")
        }
        case None => None
      }

      val predicate = wherePredicate ++ patternPredicates match {
        case Seq()  => commands.True()
        case Seq(p) => p
        case s      => s.reduceLeft(commands.And(_, _))
      }

      builder.startItems(startItems:_*).matches(patterns:_*).namedPaths(namedMatchPaths:_*).using(indexHints:_*).where(predicate)
      updateGroups
    } else {
      val firstUpdates = updateGroups.head
      val acceptableAtQueryStart = firstUpdates.head.isInstanceOf[Create] || firstUpdates.head.isInstanceOf[Merge]

      if (top && !acceptableAtQueryStart) {
        throw new SyntaxException(s"Invalid update clause at start of query (${firstUpdates.head.token.startPosition})")
      }
      addUpdateGroupToBuilder(builder, updateGroups.head)
      updateGroups.tail
    }

    val closeFunc = close match {
      case Some(c) => c.addToLegacyQuery(_)
      case None    => (b: commands.QueryBuilder) => b.returns()
    }

    rest.foldRight(closeFunc)((group, closer) => {
      val tail = closer(addUpdateGroupToBuilder(new commands.QueryBuilder, group))
      queryBuilder => queryBuilder.tail(tail).returns(commands.AllIdentifiers())
    })(builder)
  }

  private def groupUpdates(updates: Seq[UpdateClause]) : Seq[Seq[UpdateClause]] = updates match {
    case Seq() => List()
    case _ => {
      val updateBlock = updates.head +: updates.tail.takeWhile(!_.isInstanceOf[Create])
      val rest = updates.splitAt(updateBlock.length)._2
      updateBlock +: groupUpdates(rest)
    }
  }

  private def addUpdateGroupToBuilder(builder: commands.QueryBuilder, updates: Seq[UpdateClause]) = {
    val (createItems, namedCreatePaths, updateItems) = updates.head match {
      case c: Create => (c.toLegacyStartItems, c.toLegacyNamedPaths, updates.tail.flatMap(_.toLegacyUpdateActions))
      case _         => (Seq(), Seq(), updates.flatMap(_.toLegacyUpdateActions))
    }
    builder.startItems(createItems:_*).namedPaths(namedCreatePaths:_*).updates(updateItems:_*)
  }
}

sealed trait QueryClose extends AstNode with SemanticCheckable {
  def returnItems: ReturnItems
  def orderBy: Option[OrderBy]
  def skip: Option[Skip]
  def limit: Option[Limit]

  def semanticCheck = {
    returnItems.semanticCheck >>=
    checkSortItems >>=
    checkSkipLimit
  }

  // use a scoped state containing the aliased return items for the sort expressions
  private def checkSortItems : SemanticState => Seq[SemanticError] =
      s => (returnItems.declareSubqueryIdentifiers(s) >>= orderBy.semanticCheck)(s.newScope).errors

  // use an empty state when checking skip & limit, as these have isolated scope
  private def checkSkipLimit : SemanticState => Seq[SemanticError] =
      s => (skip ++ limit).semanticCheck(SemanticState.clean).errors

  def addToLegacyQuery(builder: commands.QueryBuilder) = {
    val returns = returnItems.toCommands
    extractAggregationExpressions(returns).foreach { builder.aggregation(_:_*) }
    skip.foreach { s => builder.skip(s.toCommand) }
    limit.foreach { l => builder.limit(l.toCommand) }
    orderBy.foreach { o => builder.orderBy(o.sortItems.map(_.toCommand):_*) }
    builder.returns(returns:_*)
  }

  protected def extractAggregationExpressions(items: Seq[commands.ReturnColumn]) = {
    val aggregationExpressions = items.collect {
      case commands.ReturnItem(expression, _, _) => (expression.subExpressions :+ expression).collect {
        case agg : commandexpressions.AggregationExpression => agg
      }
    }.flatten

    aggregationExpressions match {
      case Seq() => None
      case _     => Some(aggregationExpressions)
    }
  }
}

trait DistinctQueryClose extends QueryClose {
  abstract override def extractAggregationExpressions(items: Seq[commands.ReturnColumn]) = {
    Some(super.extractAggregationExpressions(items).getOrElse(Seq()))
  }
}

case class With(
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    token: InputToken,
    query: SingleQuery) extends QueryClose
{
  override def semanticCheck = {
    super.semanticCheck >>=
    checkAliasedReturnItems >>=
    checkSubQuery
  }

  private def checkAliasedReturnItems : SemanticState => Seq[SemanticError] = state => {
    returnItems match {
      case li: ListedReturnItems => li.items.filter(!_.alias.isDefined).map { i =>
        SemanticError("Expression in WITH must be aliased (use AS)", i.token)
      }
      case _ => Seq()
    }
  }

  private def checkSubQuery : SemanticState => Seq[SemanticError] = state => {
    // check the subquery with a clean state
    ( returnItems.declareSubqueryIdentifiers(state) >>= query.semanticCheck )(SemanticState.clean).errors
  }

  override def addToLegacyQuery(builder: commands.QueryBuilder) = {
    super.addToLegacyQuery(builder.tail(query.toLegacyQuery(top = false)))
  }
}

case class Return(
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    token: InputToken) extends QueryClose

trait Union extends Query {
  def statement: Query
  def query: SingleQuery

  def semanticCheck = checkUnionAggregation.toSeq ++ statement.semanticCheck ++ query.semanticCheck
  
  private def checkUnionAggregation = (statement, this) match {
    case (_: SingleQuery, _)                  => None
    case (_: UnionAll, _: UnionAll)           => None
    case (_: UnionDistinct, _: UnionDistinct) => None
    case _                                    => Some(SemanticError("Invalid combination of UNION and UNION ALL", token))
  }

  protected def unionedQueries : Seq[SingleQuery] = statement match {
    case q: SingleQuery => Seq(query, q)
    case u: Union       => query +: u.unionedQueries
  }
}

case class UnionAll(statement: Query, token: InputToken, query: SingleQuery) extends Union {
  def toLegacyQuery = commands.Union(unionedQueries.reverseMap(_.toLegacyQuery), commands.QueryString.empty, distinct = false)
}

case class UnionDistinct(statement: Query, token: InputToken, query: SingleQuery) extends Union {
  def toLegacyQuery = commands.Union(unionedQueries.reverseMap(_.toLegacyQuery), commands.QueryString.empty, distinct = true)
}
