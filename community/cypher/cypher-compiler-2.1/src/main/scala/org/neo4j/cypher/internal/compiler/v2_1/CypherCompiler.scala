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

import org.neo4j.cypher.internal.compiler.v2_1.commands.{PeriodicCommitQuery, AbstractQuery}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{NewQueryPlanSuccessRateMonitor, ExecutionPlanBuilder, ExecutionPlan}
import executionplan.verifiers.HintVerifier
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import spi.PlanContext
import org.neo4j.cypher.SyntaxException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters.{normalizeEqualsArgumentOrder, normalizeMatchPredicates, namePatternElements, normalizeArithmeticExpressions}
import ast.convert.StatementConverters._

trait SemanticCheckMonitor {
  def startSemanticCheck(query: String)
  def finishSemanticCheckSuccess(query: String)
  def finishSemanticCheckError(query: String, errors: Seq[SemanticError])
}

trait AstRewritingMonitor {
  def startRewriting(queryText: String, statement: Statement)
  def finishRewriting(queryText: String, statement: Statement)
}

case class CypherCompiler(graph: GraphDatabaseService, monitors: Monitors, semanticCheckMonitor: SemanticCheckMonitor, rewritingMonitor: AstRewritingMonitor, queryCache: (String, => Object) => Object) {
  val verifiers = Seq(HintVerifier)
  private val monitorTag: String = "compiler2.1"
  val planBuilder = new ExecutionPlanBuilder(graph, monitors.newMonitor(classOf[NewQueryPlanSuccessRateMonitor], monitorTag))
  val parser = CypherParser(monitors.newMonitor(classOf[ParserMonitor], monitorTag))

  monitors.addMonitorListener(CountNewQueryPlanSuccessRateMonitor, monitorTag)

  @throws(classOf[SyntaxException])
  def isPeriodicCommit(queryText: String) = cachedQuery(queryText) match {
    case (_: PeriodicCommitQuery, _) => true
    case _ => false
  }

  @throws(classOf[SyntaxException])
  def prepare(queryText: String, context: PlanContext): ExecutionPlan = {
    val (query: AbstractQuery, statement: Statement) = cachedQuery(queryText)
    planBuilder.build(context, query, statement)
  }

  private def cachedQuery(queryText: String): (AbstractQuery, Statement) =
    queryCache(queryText, {
      verify(parseToQuery(queryText))
    }).asInstanceOf[(AbstractQuery, Statement)]

  private def verify(query: (AbstractQuery, Statement)): (AbstractQuery, Statement) = {
    query._1.verifySemantics()
    for (verifier <- verifiers)
      verifier.verify(query._1)
    query
  }

  private def parseToQuery(queryText: String): (AbstractQuery, ast.Statement) = {
    val statement: ast.Statement = parser.parse(queryText)
    semanticCheckMonitor.startSemanticCheck(queryText)
    val semanticErrors: Seq[SemanticError] = statement.semanticCheck(SemanticState.clean).errors

    if (semanticErrors.nonEmpty)
      semanticCheckMonitor.finishSemanticCheckError(queryText, semanticErrors)
    else
      semanticCheckMonitor.finishSemanticCheckSuccess(queryText)

    semanticErrors.map { error =>
      throw new SyntaxException(s"${error.msg} (${error.position})", queryText, error.position.offset)
    }

    rewritingMonitor.startRewriting(queryText, statement)
    val normalizedStatement = statement.rewrite(bottomUp(
      normalizeArithmeticExpressions,
      namePatternElements,
      normalizeMatchPredicates,
      normalizeEqualsArgumentOrder
    )).asInstanceOf[ast.Statement]
    rewritingMonitor.finishRewriting(queryText, normalizedStatement)

    (ReattachAliasedExpressions(normalizedStatement.asQuery.setQueryText(queryText)), normalizedStatement)
  }


  case class CountNewQueryPlanSuccessRateMonitor(var queries: Long = 0L, var fallbacks: Long = 0L) extends NewQueryPlanSuccessRateMonitor {
    override def newQuerySeen(queryText: String, ast: Statement) {
      queries += 1
    }

    override def unableToHandleQuery(queryText: String, ast: Statement) {
      fallbacks += 1
    }
  }
}
