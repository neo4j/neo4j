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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters.{normalizeReturnClauses, normalizeWithClauses}
import org.neo4j.cypher.internal.compiler.v2_2.ast.{Query, Statement}
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.scalatest.mock.MockitoSugar

trait QueryGraphProducer extends MockitoSugar {

  self: LogicalPlanningTestSupport =>

  import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.StatementConverters._

  def producePlannerQueryForPattern(query: String): PlannerQuery = {
    val q = query + " RETURN 1"
    val ast = parser.parse(q)
    val semanticChecker = new SemanticChecker(mock[SemanticCheckMonitor])
    val cleanedStatement: Statement = ast.endoRewrite(inSequence(normalizeReturnClauses, normalizeWithClauses))
    val semanticState = semanticChecker.check(query, cleanedStatement)

    val firstRewriteStep = astRewriter.rewrite(query, cleanedStatement, semanticState)._1
    val (rewrittenAst: Statement, _) = Planner.rewriteStatement(firstRewriteStep, semanticState.scopeTree, SemanticTable(types = semanticState.typeTable))
    rewrittenAst.asInstanceOf[Query].asUnionQuery.queries.head
  }

  def produceQueryGraphForPattern(query: String): QueryGraph =
    producePlannerQueryForPattern(query).lastQueryGraph
}
