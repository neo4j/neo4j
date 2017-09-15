/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast.conditions._
import org.neo4j.cypher.internal.frontend.v3_4.ast.{NotEquals, Statement, UnaliasedReturnItem}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.{ApplyRewriter, RewriterCondition, RewriterStepSequencer}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState

class ASTRewriter(rewriterSequencer: (String) => RewriterStepSequencer, literalExtraction: LiteralExtraction) {

  import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStep._

  def rewrite(queryText: String, statement: Statement, semanticState: SemanticState): (Statement, Map[String, Any], Set[RewriterCondition]) = {
    val (extractParameters, extractedParameters) = literalReplacement(statement, literalExtraction)

    val contract = rewriterSequencer("ASTRewriter")(
      recordScopes(semanticState),
      desugarMapProjection(semanticState),
      normalizeComparisons,
      enableCondition(noReferenceEqualityAmongVariables),
      enableCondition(containsNoNodesOfType[UnaliasedReturnItem]),
      enableCondition(orderByOnlyOnVariables),
      enableCondition(noDuplicatesInReturnItems),
      enableCondition(noUnnamedGraphs),
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
