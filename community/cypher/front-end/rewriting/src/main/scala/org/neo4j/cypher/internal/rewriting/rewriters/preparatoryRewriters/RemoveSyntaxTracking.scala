/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.bottomUp

case object RemoveSyntaxTracking extends Step with DefaultPostCondition with PreparatoryRewritingRewriterFactory {

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    versionOpt: Option[CypherVersion]
  ): Rewriter = instance

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  private val rewriter = Rewriter.lift {
    case v: Variable if v.isIsolated           => v.copy()(v.position, Variable.isIsolatedDefault)
    case it: IsTyped if it.withDoubleColonOnly => it.copy()(it.position, IsTyped.withDoubleColonOnlyDefault)
    case lep: LabelExpressionPredicate if lep.isParenthesized || lep.hasLabeledKeyword || lep.hasNotKeyword =>
      lep.copy()(
        lep.position,
        LabelExpressionPredicate.isParenthesizedDefault,
        lep.isPostfix,
        LabelExpressionPredicate.hasLabeledKeywordDefault,
        LabelExpressionPredicate.hasNotKeywordDefault
      )
  }

  val instance: Rewriter = bottomUp(rewriter)
}
