/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2

import org.neo4j.cypher.internal.compiler.v3_2.ast.conditions._
import org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_2.ast.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.frontend.v3_2.ast.{NotEquals, Statement, UnaliasedReturnItem}
import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.{ApplyRewriter, RewriterCondition, RewriterStepSequencer}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, SemanticState}

class ASTRewriter(rewriterSequencer: (String) => RewriterStepSequencer, shouldExtractParameters: Boolean = true) {

  import org.neo4j.cypher.internal.frontend.v3_2.helpers.rewriting.RewriterStep._

  def rewrite(queryText: String, statement: Statement, semanticState: SemanticState): (Statement, Map[String, Any], Set[RewriterCondition]) = {
    val (extractParameters, extractedParameters) = if (shouldExtractParameters)
      literalReplacement(statement)
    else
      (Rewriter.lift(PartialFunction.empty), Map.empty[String, Any])

    val contract = rewriterSequencer("ASTRewriter")(
      recordScopes(semanticState),
      desugarMapProjection(semanticState),
      normalizeComparisons,
      enableCondition(noReferenceEqualityAmongVariables),
      enableCondition(containsNoNodesOfType[UnaliasedReturnItem]),
      enableCondition(orderByOnlyOnVariables),
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
      replaceLiteralDynamicPropertyLookups,
      namePatternComprehensionPatternElements,
      enableCondition(noUnnamedPatternElementsInPatternComprehension),
      inlineNamedPathsInPatternComprehensions
    )

    val rewrittenStatement = statement.endoRewrite(contract.rewriter)

    (rewrittenStatement, extractedParameters, contract.postConditions)
  }
}
