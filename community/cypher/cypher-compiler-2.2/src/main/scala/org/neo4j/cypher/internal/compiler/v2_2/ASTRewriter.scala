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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_2.planner._

class ASTRewriter(rewritingMonitor: AstRewritingMonitor, shouldExtractParameters: Boolean = true) {

  import RewriterStep._

  def rewrite(queryText: String, statement: Statement): (Statement, Map[String, Any]) = {
    rewritingMonitor.startRewriting(queryText, statement)

    val (extractParameters, extractedParameters) = if (shouldExtractParameters)
      literalReplacement(statement)
    else
      (Rewriter.lift(PartialFunction.empty), Map.empty[String, Any])

    // val rewriter = TracingRewriterStepSequencer("ast")(
    val rewriter = RewriterStepSequencer("ast")(
      foldConstants,
      NamedRewriter("extractParameters", extractParameters),
      nameMatchPatternElements,
      normalizeMatchPredicates,
      normalizeNotEquals,
      normalizeEqualsArgumentOrder,
      addUniquenessPredicates,
      expandStar,
      isolateAggregation,
      aliasReturnItems
    )

    val rewrittenStatement = statement.rewrite(rewriter).asInstanceOf[ast.Statement]

    rewritingMonitor.finishRewriting(queryText, rewrittenStatement)
    (rewrittenStatement, extractedParameters)
  }
}
