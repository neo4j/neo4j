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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.simplifyPredicates.coerceInnerExpressionToBooleanIfNecessary
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.topDown

case class mergeDuplicateBooleanOperators(additionalPreConditions: Set[StepSequencer.Condition] = Set.empty) extends ASTRewriterFactory with CnfPhase {
  override def preConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable ++ Set(!AndRewrittenToAnds) ++ additionalPreConditions

  override def postConditions: Set[StepSequencer.Condition] = Set(NoDuplicateNeighbouringBooleanOperands)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getRewriter(semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory,
                           anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Rewriter = mergeDuplicateBooleanOperatorsRewriter(semanticState)

  override def getRewriter(from: BaseState,
                           context: BaseContext): Rewriter = mergeDuplicateBooleanOperatorsRewriter(from.semantics())
}

case class mergeDuplicateBooleanOperatorsRewriter(semanticState: SemanticState) extends Rewriter {

  private def instance(semanticState: SemanticState) = fixedPoint(topDown(Rewriter.lift {
    case p@And(lhs, rhs) if (lhs == rhs) => coerceInnerExpressionToBooleanIfNecessary(semanticState, p, lhs)
    case p@Or(lhs, rhs) if (lhs == rhs) => coerceInnerExpressionToBooleanIfNecessary(semanticState, p, lhs)
  }))

  def apply(that: AnyRef): AnyRef = instance(semanticState).apply(that)

}