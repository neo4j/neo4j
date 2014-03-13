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
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.verifiers.Verifier
import org.neo4j.cypher.internal.compiler.v2_1.commands.AbstractQuery
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters._
import ast.convert.StatementConverters._
import org.neo4j.cypher.internal.compiler.v2_1.commands.PeriodicCommitQuery
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticQuery

object ParsedQuery {

  def fromStatement(queryText: String, parsedStatement: Statement)
                   (implicit rewritingMonitor: AstRewritingMonitor, semanticCheckMonitor: SemanticCheckMonitor) = {
    semanticCheckStatement(queryText, parsedStatement)
    val (statement, extractedParams) = normalizeStatement(queryText, parsedStatement)
    val semanticQuery = semanticCheckStatement(queryText, statement)
    val abstractQuery = ReattachAliasedExpressions(statement.asQuery.setQueryText(queryText))

    ParsedQuery(statement, abstractQuery, semanticQuery, extractedParams)
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

  private def normalizeStatement(queryText: String, statement: Statement)(implicit rewritingMonitor: AstRewritingMonitor): (Statement, Map[String, Any]) = {
    rewritingMonitor.startRewriting(queryText, statement)

    val (extractParameters: Rewriter, extractedParameters: Map[String, Any]) = literalReplacement(statement)

    val rewriter: Rewriter = bottomUp(
      normalizeArithmeticExpressions,
      extractParameters,
      nameMatchPatternElements,
      normalizeMatchPredicates,
      normalizeEqualsArgumentOrder
    )

    val rewrittenStatement = statement.rewrite(rewriter).asInstanceOf[ast.Statement]
    rewritingMonitor.finishRewriting(queryText, rewrittenStatement)
    (rewrittenStatement, extractedParameters)
  }
}

case class ParsedQuery(statement: Statement,
                       abstractQuery: AbstractQuery,
                       semanticQuery: SemanticQuery,
                       extractedParameters: Map[String, Any]) {
  def verified(verifiers: Seq[Verifier]): ParsedQuery = {
    abstractQuery.verifySemantics()
    verifiers.foreach(_.verify(abstractQuery))
    this
  }

  def isPeriodicCommit = abstractQuery.isInstanceOf[PeriodicCommitQuery]
}

