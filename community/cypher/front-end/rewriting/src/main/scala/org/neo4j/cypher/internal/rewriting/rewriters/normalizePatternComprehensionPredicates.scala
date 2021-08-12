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
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.topDown

case object NoNodePatternPredicatesInPatternComprehension extends StepSequencer.Condition

object normalizePatternComprehensionPredicates extends Rewriter with StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(NoNodePatternPredicatesInPatternComprehension)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getRewriter(semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory,
                           anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = this

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val normalizer = NodePatternPredicateNormalizer

  private val rewriter = Rewriter.lift {
    case p: PatternComprehension =>
      val predicates = normalizer.extractAllFrom(p.pattern)
      val rewrittenPredicates = predicates ++ p.predicate
      val newPredicate: Option[Expression] = rewrittenPredicates.reduceOption(And(_, _)(p.position))

      p.copy(
        pattern = p.pattern.endoRewrite(topDown(Rewriter.lift(normalizer.replace))),
        predicate = newPredicate
      )(p.position, p.outerScope, p.variableToCollectName, p.collectionName)
  }

  private val instance = topDown(rewriter)
}
