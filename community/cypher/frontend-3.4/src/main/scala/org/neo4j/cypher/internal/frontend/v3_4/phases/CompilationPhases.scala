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
package org.neo4j.cypher.internal.frontend.v3_4.phases

import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.rewriting.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_4.rewriters.{IfNoParameter, LiteralExtraction}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState

object CompilationPhases {

  def parsing(sequencer: String => RewriterStepSequencer, literalExtraction: LiteralExtraction = IfNoParameter): Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings andThen
      PreparatoryRewriting andThen
      SemanticAnalysis(warn = true).adds(BaseContains[SemanticState]) andThen
      AstRewriting(sequencer, literalExtraction)

  def lateAstRewriting: Transformer[BaseContext, BaseState, BaseState] =
    SemanticAnalysis(warn = false) andThen
      Namespacer andThen
      transitiveClosure andThen
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      LateAstRewriting
}
