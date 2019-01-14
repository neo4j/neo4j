/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.frontend.v3_4.ast.{Statement, UnaliasedReturnItem}
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.{RewriterCondition, RewriterStepSequencer}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState
import org.neo4j.cypher.internal.v3_4.expressions.NotEquals

class ASTRewriter(rewriterSequencer: (String) => RewriterStepSequencer,
                  literalExtraction: LiteralExtraction,
                  getDegreeRewriting: Boolean) {

  import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStep._

  def rewrite(queryText: String, statement: Statement, semanticState: SemanticState): (Statement, Map[String, Any], Set[RewriterCondition]) = {

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
      nameMatchPatternElements,
      nameUpdatingClauses,
      enableCondition(noUnnamedPatternElementsInMatch),
      nameGraphOfPatternElements,
      enableCondition(noUnnamedPatternElementsInGraphOf),
      normalizeMatchPredicates(getDegreeRewriting),
      normalizeNotEquals,
      enableCondition(containsNoNodesOfType[NotEquals]),
      normalizeArgumentOrder,
      normalizeSargablePredicates,
      enableCondition(normalizedEqualsArguments),
      addUniquenessPredicates,
      replaceLiteralDynamicPropertyLookups,
      namePatternComprehensionPatternElements,
      enableCondition(noUnnamedPatternElementsInPatternComprehension),
      inlineNamedPathsInPatternComprehensions
    )

    val rewrittenStatement = statement.endoRewrite(contract.rewriter)
    val (extractParameters, extractedParameters) = literalReplacement(rewrittenStatement, literalExtraction)

    (rewrittenStatement.endoRewrite(extractParameters), extractedParameters, contract.postConditions)
  }
}
