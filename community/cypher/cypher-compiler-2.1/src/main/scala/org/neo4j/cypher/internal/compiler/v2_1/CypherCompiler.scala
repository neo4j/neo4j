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

import org.neo4j.cypher.internal.compiler.v2_1.executionplan.{ExecutionPlanBuilder, ExecutionPlan}
import org.neo4j.cypher.internal.compiler.v2_1.parser.CypherParser
import spi.PlanContext
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Query, Statement}
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.StatementConverters._
import org.neo4j.kernel.monitoring.Monitors
import org.neo4j.cypher.internal.compiler.v2_1.commands.AbstractQuery

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

case class CypherCompiler(parser: CypherParser,
                          semanticChecker: SemanticChecker,
                          executionPlanBuilder: ExecutionPlanBuilder,
                          astRewriter: ASTRewriter,
                          cacheFactory: () => CypherCompiler.PlanCache,
                          monitors: Monitors) {

  def prepare(queryText: String, context: PlanContext): (ExecutionPlan, Map[String, Any]) = {
    val parsedStatement = parser.parse(queryText)
    semanticChecker.check(queryText, parsedStatement)
    val (rewrittenStatement, extractedParams) = astRewriter.rewrite(queryText, parsedStatement)
    val table = semanticChecker.check(queryText, parsedStatement)
    val query: AbstractQuery = rewrittenStatement.asQuery.setQueryText(queryText)
    val parsedQuery = ParsedQuery(rewrittenStatement, query, table, queryText)

    val cache = context.getOrCreateFromSchemaState(this, cacheFactory())

    val plan = cache(rewrittenStatement, executionPlanBuilder.build(context, parsedQuery))
    (plan, extractedParams)
  }

  @throws(classOf[SyntaxException])
  def isPeriodicCommit(queryText: String) = parser.parse(queryText) match {
    case q:Query => q.periodicCommitHint.nonEmpty
    case _       => false
  }
}
