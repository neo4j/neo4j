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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.internal.compiler.v2_1.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters._

class ASTRewriter(rewritingMonitor: AstRewritingMonitor, shouldExtractParameters: Boolean = true) {

  def rewrite(queryText: String, statement: Statement): (Statement, Map[String, Any]) = {
    rewritingMonitor.startRewriting(queryText, statement)

    val (extractParameters, extractedParameters) = if (shouldExtractParameters)
      literalReplacement(statement)
    else
      (Rewriter.lift(PartialFunction.empty), Map.empty[String, Any])

    val rewriters = Seq(
      foldConstants,
      extractParameters,
      nameMatchPatternElements,
      normalizeMatchPredicates,
      normalizeNotEquals,
      normalizeEqualsArgumentOrder,
      reattachAliasedExpressions,
      addUniquenessPredicates,
      expandStar,
      isolateAggregation,
      aliasReturnItems)

    val rewriter = inSequence(rewriters: _*)
    val rewrittenStatement = statement.rewrite(rewriter).asInstanceOf[ast.Statement]

    rewritingMonitor.finishRewriting(queryText, rewrittenStatement)
    (rewrittenStatement, extractedParameters)
  }
}
