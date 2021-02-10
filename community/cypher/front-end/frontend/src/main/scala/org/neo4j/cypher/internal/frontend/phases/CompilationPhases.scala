/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.rewriting.Deprecations
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.rewriting.rewriters.SameNameNamer

object CompilationPhases {

  def parsing(literalExtractionStrategy: LiteralExtractionStrategy = IfNoParameter,
              deprecations: Deprecations = Deprecations.V1
             ): Transformer[BaseContext, BaseState, BaseState] =
    Parsing andThen
      SyntaxDeprecationWarnings(deprecations) andThen
      PreparatoryRewriting(deprecations) andThen
      SemanticAnalysis(warn = true) andThen
      AstRewriting(innerVariableNamer = SameNameNamer) andThen
      LiteralExtraction(literalExtractionStrategy)

  def lateAstRewriting: Transformer[BaseContext, BaseState, BaseState] =
    isolateAggregation andThen
      SemanticAnalysis(warn = false) andThen
      Namespacer andThen
      transitiveClosure andThen
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      collapseMultipleInPredicates andThen
      SemanticAnalysis(warn = false)
}
