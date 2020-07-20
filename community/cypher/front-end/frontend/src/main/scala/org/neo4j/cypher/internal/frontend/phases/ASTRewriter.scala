/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AdministrationCommand
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.rewriting.RewriterCondition
import org.neo4j.cypher.internal.rewriting.RewriterStep.enableCondition
import org.neo4j.cypher.internal.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.conditions.noDuplicatesInReturnItems
import org.neo4j.cypher.internal.rewriting.conditions.noReferenceEqualityAmongVariables
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedPatternElementsInMatch
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedPatternElementsInPatternComprehension
import org.neo4j.cypher.internal.rewriting.conditions.normalizedEqualsArguments
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtraction
import org.neo4j.cypher.internal.rewriting.rewriters.desugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.rewriting.rewriters.inlineNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.rewriting.rewriters.literalReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.moveWithPastMatch
import org.neo4j.cypher.internal.rewriting.rewriters.nameMatchPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.namePatternComprehensionPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.nameUpdatingClauses
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeArgumentOrder
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeComparisons
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeMatchPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeNotEquals
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.parameterValueTypeReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.recordScopes
import org.neo4j.cypher.internal.rewriting.rewriters.replaceLiteralDynamicPropertyLookups
import org.neo4j.cypher.internal.rewriting.rewriters.sensitiveLiteralReplacement
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.CypherType

class ASTRewriter(rewriterSequencer: String => RewriterStepSequencer,
                  literalExtraction: LiteralExtraction,
                  innerVariableNamer: InnerVariableNamer) {

  def rewrite(statement: Statement,
              semanticState: SemanticState,
              parameterTypeMapping: Map[String, CypherType],
              cypherExceptionFactory: CypherExceptionFactory): (Statement, Map[String, Any], Set[RewriterCondition]) = {

    val contract = rewriterSequencer("ASTRewriter")(
      recordScopes(semanticState),
      expandStar(semanticState),
      desugarMapProjection(semanticState),
      moveWithPastMatch,
      normalizeComparisons,
      enableCondition(noReferenceEqualityAmongVariables),
      enableCondition(containsNoNodesOfType[UnaliasedReturnItem]),
      enableCondition(noDuplicatesInReturnItems),
      enableCondition(containsNoReturnAll),
      foldConstants(cypherExceptionFactory),
      nameMatchPatternElements,
      nameUpdatingClauses,
      enableCondition(noUnnamedPatternElementsInMatch),
      normalizeExistsPatternExpressions(semanticState),
      normalizeMatchPredicates,
      normalizeNotEquals,
      enableCondition(containsNoNodesOfType[NotEquals]),
      normalizeArgumentOrder,
      normalizeSargablePredicates,
      enableCondition(normalizedEqualsArguments),
      AddUniquenessPredicates(innerVariableNamer),
      replaceLiteralDynamicPropertyLookups,
      namePatternComprehensionPatternElements,
      enableCondition(noUnnamedPatternElementsInPatternComprehension),
      inlineNamedPathsInPatternComprehensions
    )

    val rewrittenStatement = statement.endoRewrite(contract.rewriter)

    val replaceParameterValueTypes = parameterValueTypeReplacement(rewrittenStatement, parameterTypeMapping)
    val rewrittenStatementWithParameterTypes = rewrittenStatement.endoRewrite(replaceParameterValueTypes)
    val (extractParameters, extractedParameters) = statement match {
      case _ : AdministrationCommand => sensitiveLiteralReplacement(rewrittenStatementWithParameterTypes)
      case _ => literalReplacement(rewrittenStatementWithParameterTypes, literalExtraction)
    }

    (rewrittenStatementWithParameterTypes.endoRewrite(extractParameters), extractedParameters, contract.postConditions)
  }
}
