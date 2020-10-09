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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.rewriting.RewritingStepSequencer
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.InnerVariableNamer
import org.neo4j.cypher.internal.rewriting.rewriters.desugarMapProjection
import org.neo4j.cypher.internal.rewriting.rewriters.expandStar
import org.neo4j.cypher.internal.rewriting.rewriters.foldConstants
import org.neo4j.cypher.internal.rewriting.rewriters.inlineNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.rewriting.rewriters.moveWithPastMatch
import org.neo4j.cypher.internal.rewriting.rewriters.nameAllPatternElements
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeArgumentOrder
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeComparisons
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeExistsPatternExpressions
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeHasLabelsAndHasType
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeMatchPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeNotEquals
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeSargablePredicates
import org.neo4j.cypher.internal.rewriting.rewriters.parameterValueTypeReplacement
import org.neo4j.cypher.internal.rewriting.rewriters.replaceLiteralDynamicPropertyLookups
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CypherType

class ASTRewriter(innerVariableNamer: InnerVariableNamer) {

  def rewrite(statement: Statement,
              semanticState: SemanticState,
              parameterTypeMapping: Map[String, CypherType],
              cypherExceptionFactory: CypherExceptionFactory): Statement = {

    val orderedSteps = RewritingStepSequencer.orderSteps(Set(
      expandStar(semanticState),
      normalizeHasLabelsAndHasType(semanticState),
      desugarMapProjection(semanticState),
      moveWithPastMatch,
      normalizeComparisons,
      foldConstants(cypherExceptionFactory),
      normalizeExistsPatternExpressions(semanticState),
      nameAllPatternElements,
      normalizeMatchPredicates,
      normalizeNotEquals,
      normalizeArgumentOrder,
      normalizeSargablePredicates,
      AddUniquenessPredicates(innerVariableNamer),
      replaceLiteralDynamicPropertyLookups,
      inlineNamedPathsInPatternComprehensions,
      parameterValueTypeReplacement(parameterTypeMapping),
    ))
    val combined = inSequence(orderedSteps: _*)

    statement.endoRewrite(combined)
  }
}
