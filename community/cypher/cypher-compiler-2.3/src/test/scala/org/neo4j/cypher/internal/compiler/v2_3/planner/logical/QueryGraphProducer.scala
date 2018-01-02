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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Query, Statement}
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, inSequence}
import org.scalatest.mock.MockitoSugar

trait QueryGraphProducer extends MockitoSugar {

  self: LogicalPlanningTestSupport =>

  import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.StatementConverters._

  def producePlannerQueryForPattern(query: String): (PlannerQuery, SemanticTable) = {
    val q = query + " RETURN 1 AS Result"
    val ast = parser.parse(q)
    val mkException = new SyntaxExceptionCreator(query, Some(pos))
    val semanticChecker = new SemanticChecker
    val cleanedStatement: Statement = ast.endoRewrite(inSequence(normalizeReturnClauses(mkException), normalizeWithClauses(mkException)))
    val semanticState = semanticChecker.check(query, cleanedStatement, mkException)

    val (firstRewriteStep, _, postConditions) = astRewriter.rewrite(query, cleanedStatement, semanticState)
    val semanticTable = SemanticTable(types = semanticState.typeTable, recordedScopes = semanticState.recordedScopes)
    val (rewrittenAst, rewrittenTable) = CostBasedExecutablePlanBuilder.rewriteStatement(firstRewriteStep, semanticState.scopeTree, semanticTable, rewriterSequencer, semanticChecker, postConditions, mock[AstRewritingMonitor])
    (rewrittenAst.asInstanceOf[Query].asUnionQuery.queries.head, rewrittenTable)
  }

  def produceQueryGraphForPattern(query: String): (QueryGraph, SemanticTable) = {
    val (plannerQuery, table) = producePlannerQueryForPattern(query)
    (plannerQuery.lastQueryGraph, table)
  }
}
