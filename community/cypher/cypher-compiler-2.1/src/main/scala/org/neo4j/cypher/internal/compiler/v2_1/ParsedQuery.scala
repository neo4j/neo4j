/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticQuery
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.verifiers.Verifier
import org.neo4j.cypher.internal.compiler.v2_1.commands.{PeriodicCommitQuery, AbstractQuery}
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters.{normalizeEqualsArgumentOrder, normalizeMatchPredicates, nameMatchPatternElements, normalizeArithmeticExpressions}
import ast.convert.StatementConverters._

object ParsedQuery {

  def fromStatement(queryText: String, parsedStatement: Statement)
                   (implicit rewritingMonitor: AstRewritingMonitor, semanticCheckMonitor: SemanticCheckMonitor) = {
    val statement = normalizeStatement(queryText, parsedStatement)
    val semanticQuery = semanticCheckStatement(queryText, statement)
    val abstractQuery = ReattachAliasedExpressions(statement.asQuery.setQueryText(queryText))

    ParsedQuery(statement, abstractQuery, semanticQuery)
  }

  private def semanticCheckStatement(queryText: String, statement: Statement)
                                    (implicit semanticCheckMonitor: SemanticCheckMonitor): SemanticQuery = {
    semanticCheckMonitor.startSemanticCheck(queryText)
    val SemanticCheckResult(semanticState, semanticErrors) = statement.semanticCheck(SemanticState.clean)
    if (semanticErrors.nonEmpty)
      semanticCheckMonitor.finishSemanticCheckError(queryText, semanticErrors)
    else
      semanticCheckMonitor.finishSemanticCheckSuccess(queryText)

    semanticErrors.map {
      error => throw new SyntaxException(s"${error.msg} (${error.position})", queryText, error.position.offset)
    }

    SemanticQuery(types = semanticState.typeTable)
  }

  private def normalizeStatement(queryText: String, statement: Statement)(implicit rewritingMonitor: AstRewritingMonitor): Statement = {
    rewritingMonitor.startRewriting(queryText, statement)
    val rewrittenStatement = statement.rewrite(bottomUp(
      normalizeArithmeticExpressions,
      nameMatchPatternElements,
      normalizeMatchPredicates,
      normalizeEqualsArgumentOrder
    )).asInstanceOf[ast.Statement]
    rewritingMonitor.finishRewriting(queryText, rewrittenStatement)
    rewrittenStatement
  }
}

case class ParsedQuery(statement: Statement, abstractQuery: AbstractQuery, semanticQuery: SemanticQuery) {
  def verified(verifiers: Seq[Verifier]): ParsedQuery = {
    abstractQuery.verifySemantics()
    verifiers.foreach(_.verify(abstractQuery))
    this
  }

  def isPeriodicCommit = abstractQuery.isInstanceOf[PeriodicCommitQuery]
}

