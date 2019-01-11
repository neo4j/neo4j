/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.v4_0.phases

import org.neo4j.cypher.internal.v4_0.ast.Statement
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticFeature.Cypher9Comparability
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticState
import org.neo4j.cypher.internal.v4_0.frontend.phases._
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{IfNoParameter, LiteralExtraction}
import org.neo4j.cypher.internal.v4_0.rewriting.{Deprecations, RewriterStepSequencer}

object CompilationPhases {

  def parsing(sequencer: String => RewriterStepSequencer,
              literalExtraction: LiteralExtraction = IfNoParameter
             ): Transformer[BaseContext, BaseState, BaseState] =
    Parsing.adds(BaseContains[Statement]) andThen
      SyntaxDeprecationWarnings(Deprecations.V2) andThen
      PreparatoryRewriting(Deprecations.V2) andThen
      SemanticAnalysis(warn = true, Cypher9Comparability).adds(BaseContains[SemanticState]) andThen
      AstRewriting(sequencer, literalExtraction)

  def lateAstRewriting: Transformer[BaseContext, BaseState, BaseState] =
    isolateAggregation andThen
      SemanticAnalysis(warn = false, Cypher9Comparability) andThen
      Namespacer andThen
      transitiveClosure andThen
      rewriteEqualityToInPredicate andThen
      CNFNormalizer andThen
      LateAstRewriting andThen
      SemanticAnalysis(warn = false, Cypher9Comparability)
}
