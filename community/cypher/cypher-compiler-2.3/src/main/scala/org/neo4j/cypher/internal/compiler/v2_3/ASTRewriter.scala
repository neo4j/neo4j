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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.compiler.v2_3.ast.conditions._
import org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters._
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.{ApplyRewriter, RewriterCondition, RewriterStepSequencer}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{Rewriter, SemanticState}

class ASTRewriter(rewriterSequencer: (String) => RewriterStepSequencer, shouldExtractParameters: Boolean = true) {

  import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStep._

  def rewrite(queryText: String, statement: Statement, semanticState: SemanticState): (Statement, Map[String, Any], Set[RewriterCondition]) = {
    val (extractParameters, extractedParameters) = if (shouldExtractParameters)
      literalReplacement(statement)
    else
      (Rewriter.lift(PartialFunction.empty), Map.empty[String, Any])

    val contract = rewriterSequencer("ASTRewriter")(
      normalizeComparisons,
      enableCondition(noReferenceEqualityAmongIdentifiers),
      enableCondition(containsNoNodesOfType[UnaliasedReturnItem]),
      enableCondition(orderByOnlyOnIdentifiers),
      enableCondition(noDuplicatesInReturnItems),
      expandStar(semanticState),
      enableCondition(containsNoReturnAll),
      foldConstants,
      ApplyRewriter("extractParameters", extractParameters),
      nameMatchPatternElements,
      enableCondition(noUnnamedPatternElementsInMatch),
      normalizeMatchPredicates,
      normalizeNotEquals,
      enableCondition(containsNoNodesOfType[NotEquals]),
      normalizeArgumentOrder,
      normalizeSargablePredicates,
      enableCondition(normalizedEqualsArguments),
      addUniquenessPredicates,
      isolateAggregation,
      enableCondition(aggregationsAreIsolated),
      replaceLiteralDynamicPropertyLookups
    )

    val rewrittenStatement = statement.endoRewrite(contract.rewriter)

    (rewrittenStatement, extractedParameters, contract.postConditions)
  }
}
