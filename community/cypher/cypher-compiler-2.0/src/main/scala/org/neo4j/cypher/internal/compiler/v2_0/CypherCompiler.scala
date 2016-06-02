/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import ast.convert.StatementConverters._
import ast.rewriters._
import commands.AbstractQuery
import executionplan.{ExecutionPlanBuilder, ExecutionPlan}
import executionplan.verifiers.HintVerifier
import parser.CypherParser
import spi.PlanContext
import org.neo4j.cypher.SyntaxException
import org.neo4j.graphdb.GraphDatabaseService

case class CypherCompiler(graph: GraphDatabaseService, queryCache: (Object, => Object) => Object) {
  val parser = CypherParser()
  val verifiers = Seq(HintVerifier)
  val planBuilder = new ExecutionPlanBuilder(graph)

  @throws(classOf[SyntaxException])
  def prepare(query: String, context: PlanContext): (ExecutionPlan, Map[String,Any]) = {

    def semanticCheck(statement: ast.Statement): Unit =
      statement.semanticCheck(SemanticState.clean).errors.map { error =>
        throw new SyntaxException(s"${error.msg} (${error.position})", query, error.position.offset)
      }

    val statement = parser.parse(query)

    val (rewrittenStatement, extractedParameters) = try {
      rewriteStatement(statement)
    } catch {
      case e: Exception =>
        // if rewriting failed, it could be due to semantic errors in the AST
        semanticCheck(statement)
        throw e
    }

    val plan = queryCache(rewrittenStatement, {
      // check original statement, not rewritten one
      semanticCheck(statement)
      val parsedQuery = ReattachAliasedExpressions(rewrittenStatement.asQuery.setQueryText(query))
      planBuilder.build(context, verify(parsedQuery))
    }).asInstanceOf[ExecutionPlan]

    (plan, extractedParameters)
  }

  def rewriteStatement(statement: ast.Statement) = {
    val (extractParameters, extractedParameters) = literalReplacement(statement)
    val result = statement.rewrite(bottomUpExpressions(extractParameters)).asInstanceOf[ast.Statement]
    (result, extractedParameters)
  }

  def verify(query: AbstractQuery): AbstractQuery = {
    query.verifySemantics()
    for (verifier <- verifiers)
      verifier.verify(query)
    query
  }
}
