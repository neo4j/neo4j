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
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.simplifyPredicates.coerceInnerExpressionToBooleanIfNecessary
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.topDown

case class simplifyPredicates(semanticState: SemanticState) extends Rewriter {
  private val T = True()(null)
  private val F = False()(null)

  private val step: Rewriter = Rewriter.lift { case e: Expression => computeReplacement(e) }

  private val instance = fixedPoint(topDown(step))

  def apply(that: AnyRef): AnyRef = {
    instance.apply(that)
  }

  private def computeReplacement: Expression => Expression = {
    case n@Not(Not(innerExpression)) => simplifyToInnerExpression(n, innerExpression)
    case Ands(exps)   if exps.isEmpty => throw new IllegalStateException("Found an instance of Ands with empty expressions")
    case Ors(exps)    if exps.isEmpty => throw new IllegalStateException("Found an instance of Ors with empty expressions")
    case p@Ands(exps) if exps.contains(F) => False()(p.position)
    case p@Ors(exps)  if exps.contains(T) => True()(p.position)
    case p@Ands(exps) if exps.size == 1   => simplifyToInnerExpression(p, exps.head)
    case p@Ors(exps)  if exps.size == 1   => simplifyToInnerExpression(p, exps.head)
    case p@Ands(exps) if exps.contains(T) =>
      val nonTrue = exps.filterNot(T == _)
      if (nonTrue.isEmpty)
        True()(p.position)
      else if(nonTrue.size == 1)
        simplifyToInnerExpression(p, nonTrue.head)
      else
        Ands(nonTrue)(p.position)
    case p@Ors(exps) if exps.contains(F) =>
      val nonFalse = exps.filterNot(F == _)
      if (nonFalse.isEmpty)
        False()(p.position)
      else if(nonFalse.size == 1)
        simplifyToInnerExpression(p, nonFalse.head)
      else
        Ors(nonFalse)(p.position)
    case expression => expression
  }

  private def simplifyToInnerExpression(outerExpression: BooleanExpression,
                                        innerExpression: Expression) = {
    val newExpression = computeReplacement(innerExpression)
    coerceInnerExpressionToBooleanIfNecessary(semanticState, outerExpression, newExpression)
  }
}

case object simplifyPredicates extends StepSequencer.Step with PlanPipelineTransformerFactory with CnfPhase {

  override def preConditions: Set[StepSequencer.Condition] = Set(AndRewrittenToAnds) ++ SemanticInfoAvailable

  override def postConditions: Set[StepSequencer.Condition] = Set(PredicatesSimplified)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def getRewriter(from: BaseState,
                           context: BaseContext): Rewriter = this (from.semantics())

  def coerceInnerExpressionToBooleanIfNecessary(semanticState: SemanticState,
                                                outerExpression: BooleanExpression,
                                                innerExpression: Expression): Expression = {
    if (needsToBeExplicitlyCoercedToBoolean(semanticState, outerExpression, innerExpression)) {
      CoerceToPredicate(innerExpression)
    } else {
      innerExpression
    }
  }

  /**
   * We intend to remove `outerExpression` from the AST and replace it with `innerExpression`.
   *
   * While `outerExpression` would have converted the value to boolean, we check here whether that information would be lost.
   */
  private def needsToBeExplicitlyCoercedToBoolean(semanticState: SemanticState, outerExpression: BooleanExpression, innerExpression: Expression) = {
    val expectedToBeBoolean = semanticState.expressionType(outerExpression).expected.exists(_.contains(CTBoolean))
    val specifiedToBeBoolean = semanticState.expressionType(innerExpression).specified.contains(CTBoolean)
    !expectedToBeBoolean && !specifiedToBeBoolean
  }
}