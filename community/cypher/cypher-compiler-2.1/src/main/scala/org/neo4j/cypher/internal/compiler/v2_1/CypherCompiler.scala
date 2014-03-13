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

import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{NewQueryPlanSuccessRateMonitor, ExecutionPlanBuilder, ExecutionPlan}
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.verifiers.HintVerifier
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import spi.PlanContext
import org.neo4j.cypher.SyntaxException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.kernel.monitoring.Monitors

trait SemanticCheckMonitor {
  def startSemanticCheck(query: String)
  def finishSemanticCheckSuccess(query: String)
  def finishSemanticCheckError(query: String, errors: Seq[SemanticError])
}

trait AstRewritingMonitor {
  def startRewriting(queryText: String, statement: Statement)
  def finishRewriting(queryText: String, statement: Statement)
}

object CypherCompiler {
  type CacheKey = Statement
  type CacheValue = ExecutionPlan
  type PlanCache = (Statement, => CacheValue) => CacheValue
}

case class CypherCompiler(graph: GraphDatabaseService, monitors: Monitors, semanticCheckMonitor: SemanticCheckMonitor, rewritingMonitor: AstRewritingMonitor, planCache: CypherCompiler.PlanCache) {
  val verifiers = Seq(HintVerifier)
  private val monitorTag: String = "compiler2.1"
  val planBuilder = new ExecutionPlanBuilder(graph, monitors.newMonitor(classOf[NewQueryPlanSuccessRateMonitor], monitorTag))
  val parser = CypherParser(monitors.newMonitor(classOf[ParserMonitor], monitorTag))

  monitors.addMonitorListener(CountNewQueryPlanSuccessRateMonitor, monitorTag)

  @throws(classOf[SyntaxException])
  def isPeriodicCommit(queryText: String) = parseToQuery(queryText).isPeriodicCommit

  @throws(classOf[SyntaxException])
  def prepare(queryText: String, context: PlanContext): (ExecutionPlan, Map[String, Any]) = {
    val parsedQuery: ParsedQuery = parseToQuery(queryText)
    val plan: ExecutionPlan = planCache(parsedQuery.statement, planBuilder.build(context, parsedQuery))

    (plan, parsedQuery.extractedParameters)
  }

  private def parseToQuery(queryText: String): ParsedQuery =
    ParsedQuery.fromStatement(queryText, parser.parse(queryText))(rewritingMonitor, semanticCheckMonitor).
      verified(verifiers)

  case class CountNewQueryPlanSuccessRateMonitor(var queries: Long = 0L, var fallbacks: Long = 0L) extends NewQueryPlanSuccessRateMonitor {
    override def newQuerySeen(queryText: String, ast: Statement) {
      queries += 1
    }

    override def unableToHandleQuery(queryText: String, ast: Statement) {
      fallbacks += 1
    }
  }
}

