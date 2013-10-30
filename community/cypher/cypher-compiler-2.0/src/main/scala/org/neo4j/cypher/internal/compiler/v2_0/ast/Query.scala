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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_0.commands

sealed trait Query extends Statement

case class SingleQuery(clauses: Seq[Clause], token: InputToken) extends Query {
  def semanticCheck: SemanticCheck = checkOrder(clauses) then checkClauses

  private def checkOrder(clauses: Seq[Clause]): SemanticCheck = clauses match {
    case Seq() => Seq()
    case _     => (clauses.take(2) match {
      case Seq(_: With, _: Start)                => Seq()
      case Seq(clause, start: Start)             => Seq(SemanticError(s"WITH is required between ${clause.name} and ${start.name}", clause.token, start.token))
      case Seq(match1: Match, match2: Match) if match1.optional && !match2.optional => Seq(SemanticError(s"${match2.name} cannot follow OPTIONAL ${match1.name} (perhaps use a WITH clause between them)", match2.token, match1.token))
      case Seq(clause: Return, _)                => Seq(SemanticError(s"${clause.name} can only be used at the end of the query", clause.token))
      case Seq(_: UpdateClause, _: UpdateClause) => Seq()
      case Seq(_: UpdateClause, _: With)         => Seq()
      case Seq(_: UpdateClause, _: Return)       => Seq()
      case Seq(update: UpdateClause, clause)     => Seq(SemanticError(s"WITH is required between ${update.name} and ${clause.name}", clause.token, update.token))
      case Seq(_: UpdateClause)                  => Seq()
      case Seq(_: Return)                        => Seq()
      case Seq(clause)                           => Seq(SemanticError(s"Query cannot conclude with ${clause.name} (must be RETURN or an update clause)", clause.token))
      case _                                     => Seq()
    }) then checkOrder(clauses.tail)
  }

  private def checkClauses: SemanticCheck = s => {
    def checkClause(clause: Clause, last: SemanticCheckResult) = {
      val result = clause.semanticCheck(last.state)
      SemanticCheckResult(result.state, last.errors ++ result.errors)
    }

    clauses.foldLeft(SemanticCheckResult.success(s.clearSymbols))((last, clause) => clause match {
      case c: With =>
        val result = checkClause(clause, last)
        val subState = c.returnItems.declareIdentifiers(result.state)(result.state.clearSymbols)
        SemanticCheckResult(subState.state, result.errors ++ subState.errors)
      case _       => checkClause(clause, last)
    })
  }

  def toLegacyQuery =
    groupClauses(clauses).foldRight(None: Option[commands.Query], (_: commands.QueryBuilder).returns()) {
      case (group, (tail, defaultClose)) =>
        val builder = tail.foldLeft(new commands.QueryBuilder())((b, t) => b.tail(t))

        group.foldLeft(builder)((b, clause) => clause match {
          case c: Start        => c.addToLegacyQuery(b)
          case c: Match        => c.addToLegacyQuery(b)
          case c: Merge        => c.addToLegacyQuery(b)
          case c: Create       => c.addToLegacyQuery(b)
          case c: CreateUnique => c.addToLegacyQuery(b)
          case c: SetClause    => c.addToLegacyQuery(b)
          case c: Delete       => c.addToLegacyQuery(b)
          case c: Remove       => c.addToLegacyQuery(b)
          case c: Foreach      => c.addToLegacyQuery(b)
          case _: With         => b
          case _: Return       => b
          case _               => throw new ThisShouldNotHappenError("cleishm", "Unknown clause while grouping")
        })

        val query = Some(group.takeRight(2) match {
          case Seq(w: With, r: Return)  => w.closeLegacyQueryBuilder(builder, r.closeLegacyQueryBuilder)
          case Seq(_, c: ClosingClause) => c.closeLegacyQueryBuilder(builder)
          case Seq(c: ClosingClause)    => c.closeLegacyQueryBuilder(builder)
          case _                        => defaultClose(builder)
        })

        (query, (_: commands.QueryBuilder).returns(commands.AllIdentifiers()))
    }._1.get

  private def groupClauses(clauses: Seq[Clause]): IndexedSeq[IndexedSeq[Clause]] = {
    def split = Vector(clauses.head) +: groupClauses(clauses.tail)

    def group = {
      val tailGroups = groupClauses(clauses.tail)
      (clauses.head +: tailGroups.head) +: tailGroups.tail
    }

    clauses.take(2) match {
      case Seq(clause)                           => Vector(Vector(clause))
      case Seq(_: With, _: Return)               => group
      case Seq(_: ClosingClause, _)              => split
      case Seq(_, _: ClosingClause)              => group
      case Seq(_: UpdateClause, _: Create)       => split
      case Seq(_: UpdateClause, _: CreateUnique) => split
      case Seq(_: UpdateClause, _: Merge)        => split
      case Seq(_: UpdateClause, _: UpdateClause) => group
      case Seq(_: UpdateClause, _)               => split
      case Seq(_, _: UpdateClause)               => split
      case Seq(_: Match, _)                      => split
      case Seq(_, _)                             => group
    }
  }
}

trait Union extends Query {
  def statement: Query
  def query: SingleQuery

  def semanticCheck: SemanticCheck =
    checkUnionAggregation then
      statement.semanticCheck then
      query.semanticCheck

  private def checkUnionAggregation: SemanticCheck = (statement, this) match {
    case (_: SingleQuery, _)                  => None
    case (_: UnionAll, _: UnionAll)           => None
    case (_: UnionDistinct, _: UnionDistinct) => None
    case _                                    => Some(SemanticError("Invalid combination of UNION and UNION ALL", token))
  }

  protected def unionedQueries: Seq[SingleQuery] = statement match {
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
