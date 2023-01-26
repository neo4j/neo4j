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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CypherTypeInfo

case object ParenthesizedPathUnwrapped extends Condition

/**
 * We parse parenthesized path patterns and represent them in the AST as they are, as we have the information anyways and will need it later on.
 * As of now, we do not allow anything particular useful to be done with them which is why we can avoid treating them in the rest of the system
 * and remove them as part of this rewriter.
 *
 * This will probably change when we allow
 *   - juxtaposing (non-quantified) parenthesized path patterns with other parts of the query
 *
 */
case object unwrapParenthesizedPath extends StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(ParenthesizedPathUnwrapped)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, CypherTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = {
    unwrapParenthesizedPath.instance
  }

  val instance: Rewriter = bottomUp(
    Rewriter.lift {
      case p: ParenthesizedPath => p.part.element
    }
  )
}
