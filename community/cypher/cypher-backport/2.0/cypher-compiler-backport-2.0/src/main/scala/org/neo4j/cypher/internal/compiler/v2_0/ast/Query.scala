/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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


case class Query(part: QueryPart)(val position: InputPosition) extends Statement {
  override def semanticCheck = part.semanticCheck
}

sealed trait QueryPart extends ASTNode with SemanticCheckable

case class SingleQuery(clauses: Seq[Clause])(val position: InputPosition) extends QueryPart {
  assert(clauses.nonEmpty)

  def semanticCheck: SemanticCheck = checkOrder(clauses) then checkClauses

  private def checkOrder(clauses: Seq[Clause]): SemanticCheck = s => {
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
}

sealed trait Union extends QueryPart {
  def part: QueryPart
  def query: SingleQuery

  def semanticCheck: SemanticCheck =
    checkUnionAggregation then
    part.semanticCheck then
    query.semanticCheck

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

case class UnionAll(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends Union
case class UnionDistinct(part: QueryPart, query: SingleQuery)(val position: InputPosition) extends Union
