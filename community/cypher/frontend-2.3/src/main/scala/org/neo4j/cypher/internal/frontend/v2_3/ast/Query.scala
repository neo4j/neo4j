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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticChecking, SemanticError, _}

case class Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart)(val position: InputPosition)
  extends Statement with SemanticChecking {

  override def semanticCheck =
    part.semanticCheck chain
    periodicCommitHint.semanticCheck chain
    when(periodicCommitHint.nonEmpty && !part.containsUpdates) {
      SemanticError("Cannot use periodic commit in a non-updating query", periodicCommitHint.get.position)
    }
}

sealed trait QueryPart extends ASTNode with ASTPhrase with SemanticCheckable {
  def containsUpdates: Boolean
}

case class SingleQuery(clauses: Seq[Clause])(val position: InputPosition) extends QueryPart {
  assert(clauses.nonEmpty)

  def containsUpdates:Boolean =
    clauses.exists {
      case _: UpdateClause => true
      case _               => false
    }

  def semanticCheck: SemanticCheck =
    checkOrder chain
    checkClauses chain
    checkIndexHints

  private def checkIndexHints: SemanticCheck = s => {
    val hints = clauses.collect { case m: Match => m.hints }.flatten
    val hasStartClause = clauses.exists(_.isInstanceOf[Start])
    if (hints.nonEmpty && hasStartClause) {
      SemanticCheckResult.error(s, SemanticError("Cannot use index hints with start clause", hints.head.position))
    } else {
      SemanticCheckResult.success(s)
    }
  }

  private def checkOrder: SemanticCheck = s => {
    val (lastPair, errors) = clauses.sliding(2).foldLeft(Seq.empty[Clause], Vector.empty[SemanticError]) {
      case ((_, errors), pair) =>
        val optError = pair match {
          case Seq(_: With, _: Start) =>
            None
          case Seq(clause, start: Start) =>
            Some(SemanticError(s"WITH is required between ${clause.name} and ${start.name}", clause.position, start.position))
          case Seq(match1: Match, match2: Match) if match1.optional && !match2.optional =>
            Some(SemanticError(s"${match2.name} cannot follow OPTIONAL ${match1.name} (perhaps use a WITH clause between them)", match2.position, match1.position))
          case Seq(clause: Return, _) =>
            Some(SemanticError(s"${clause.name} can only be used at the end of the query", clause.position))
          case Seq(_: UpdateClause, _: UpdateClause) =>
            None
          case Seq(_: UpdateClause, _: With) =>
            None
          case Seq(_: UpdateClause, _: Return) =>
            None
          case Seq(update: UpdateClause, clause) =>
            Some(SemanticError(s"WITH is required between ${update.name} and ${clause.name}", clause.position, update.position))
          case _ =>
            None
        }
        (pair, optError.fold(errors)(errors :+ _))
    }

    val lastError = lastPair.last match {
      case _: UpdateClause =>
        None
      case _: Return =>
        None
      case clause =>
        Some(SemanticError(s"Query cannot conclude with ${clause.name} (must be RETURN or an update clause)", clause.position))
    }

    SemanticCheckResult(s, errors ++ lastError)
  }

  private def checkClauses: SemanticCheck = s => {
    val result = clauses.foldLeft(SemanticCheckResult.success(s.newChildScope))((lastResult, clause) => clause match {
      case c: HorizonClause =>
        val closingResult = c.semanticCheck(lastResult.state)
        val nextState = closingResult.state.newSiblingScope
        val continuationResult = c.semanticCheckContinuation(closingResult.state.currentScope.scope)(nextState)
        SemanticCheckResult(continuationResult.state, lastResult.errors ++ closingResult.errors ++ continuationResult.errors)

      case _ =>
        val result = clause.semanticCheck(lastResult.state)
        SemanticCheckResult(result.state, lastResult.errors ++ result.errors)
    })
    SemanticCheckResult(result.state.popScope, result.errors)
  }
}

sealed trait Union extends QueryPart with SemanticChecking {
  def part: QueryPart
  def query: SingleQuery

  def containsUpdates:Boolean = part.containsUpdates || query.containsUpdates

  def semanticCheck: SemanticCheck =
    checkUnionAggregation chain
    withScopedState(part.semanticCheck) chain
    withScopedState(query.semanticCheck) chain
    checkColumnNamesAgree

  private def checkColumnNamesAgree: SemanticCheck = (state: SemanticState) => {
    val rootScope: Scope = state.currentScope.scope

    // UNION queries form a chain in the shape of a reversed linked list.
    // Therefore, the second scope is always a shallow scope, where the last one corresponds to the RETURN clause.
    // The first one may either be another UNION query part or a single query.
    val first :: second :: Nil = rootScope.children
    val newFirst = part match {
      case _: Union => // Union query parts always have two child scopes.
        val _ :: newFirst :: Nil = first.children
        newFirst
      case _ => first
    }
    val errors = if (newFirst.children.last.symbolNames == second.children.last.symbolNames) {
      Seq.empty
    } else {
      Seq(SemanticError("All sub queries in an UNION must have the same column names", position))
    }
    SemanticCheckResult(state, errors)
  }

  private def checkUnionAggregation: SemanticCheck = (part, this) match {
    case (_: SingleQuery, _)                  => None
    case (_: UnionAll, _: UnionAll)           => None
    case (_: UnionDistinct, _: UnionDistinct) => None
    case _                                    => Some(SemanticError("Invalid combination of UNION and UNION ALL", position))
  }

  def unionedQueries: Seq[SingleQuery] = unionedQueries(Vector.empty)
  private def unionedQueries(accum: Seq[SingleQuery]): Seq[SingleQuery] = part match {
    case q: SingleQuery => accum :+ query :+ q
    case u: Union       => u.unionedQueries(accum :+ query)
  }
}

final case class UnionAll(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends Union
final case class UnionDistinct(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends Union
