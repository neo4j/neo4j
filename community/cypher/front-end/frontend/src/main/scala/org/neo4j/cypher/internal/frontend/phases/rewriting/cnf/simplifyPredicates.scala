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
package org.neo4j.cypher.internal.frontend.phases.rewriting.cnf

import org.neo4j.cypher.internal.ast.IsNormalized
import org.neo4j.cypher.internal.ast.IsNotNormalized
import org.neo4j.cypher.internal.ast.IsNotTyped
import org.neo4j.cypher.internal.ast.IsTyped
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.OrRewrittenToOrs
import org.neo4j.cypher.internal.rewriting.conditions.PredicatesSimplified
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.copyVariables
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
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
    case n @ Not(Not(innerExpression))                  => simplifyToInnerExpression(n, innerExpression)
    case n @ Not(IsNull(innerExpression))               => IsNotNull(innerExpression)(n.position)
    case n @ Not(IsNotNull(innerExpression))            => IsNull(innerExpression)(n.position)
    case n @ Not(IsNotTyped(innerExpression, typeName)) => IsTyped(innerExpression, typeName)(n.position)
    case n @ IsNotTyped(innerExpression, typeName) => Not(IsTyped(innerExpression, typeName)(n.position))(n.position)
    case n @ Not(IsNotNormalized(innerExpression, normalForm)) => IsNormalized(innerExpression, normalForm)(n.position)
    case n @ IsNotNormalized(innerExpression, normalForm) =>
      Not(IsNormalized(innerExpression, normalForm)(n.position))(n.position)
    case Ands(exps) if exps.isEmpty =>
      throw new IllegalStateException("Found an instance of Ands with empty expressions")
    case Ors(exps) if exps.isEmpty => throw new IllegalStateException("Found an instance of Ors with empty expressions")
    case p @ Ands(exps) if exps.contains(F) => False()(p.position)
    case p @ Ors(exps) if exps.contains(T)  => True()(p.position)
    case p @ Ands(exps) if exps.size == 1   => simplifyToInnerExpression(p, exps.head)
    case p @ Ors(exps) if exps.size == 1    => simplifyToInnerExpression(p, exps.head)
    case p @ Ands(exps) if exps.contains(T) =>
      val nonTrue = exps.filterNot(T == _)
      if (nonTrue.isEmpty)
        True()(p.position)
      else if (nonTrue.size == 1)
        simplifyToInnerExpression(p, nonTrue.head)
      else
        Ands(nonTrue)(p.position)
    case p @ Ors(exps) if exps.contains(F) =>
      val nonFalse = exps.filterNot(F == _)
      if (nonFalse.isEmpty)
        False()(p.position)
      else if (nonFalse.size == 1)
        simplifyToInnerExpression(p, nonFalse.head)
      else
        Ors(nonFalse)(p.position)
    // technically, this is not simplification but it helps addressing the separate predicates in the conjunction
    case all @ AllIterablePredicate(fs @ FilterScope(variable, Some(Ands(preds))), expression) =>
      val predicates: ListSet[Expression] = preds.map { predicate =>
        AllIterablePredicate(FilterScope(variable, Some(predicate))(fs.position), expression)(all.position)
      }
      Ands(predicates.endoRewrite(copyVariables))(all.position)
    case p @ Not(True())  => False()(p.position)
    case p @ Not(False()) => True()(p.position)
    case expression       => expression
  }

  private def simplifyToInnerExpression(outerExpression: BooleanExpression, innerExpression: Expression) = {
    val newExpression = computeReplacement(innerExpression)
    if (needsToBeExplicitlyCoercedToBoolean(outerExpression, newExpression)) {
      CoerceToPredicate(newExpression)
    } else {
      newExpression
    }
  }

  /**
   * We intend to remove `outerExpression` from the AST and replace it with `innerExpression`.
   *
   * While `outerExpression` would have converted the value to boolean, we check here whether that information would be lost.
   */
  private def needsToBeExplicitlyCoercedToBoolean(outerExpression: BooleanExpression, innerExpression: Expression) = {
    val expectedToBeBoolean = semanticState.expressionType(outerExpression).expected.exists(_.contains(CTBoolean))
    val specifiedToBeBoolean = semanticState.expressionType(innerExpression).specified.contains(CTBoolean)
    !expectedToBeBoolean && !specifiedToBeBoolean
  }
}

case object simplifyPredicates extends StepSequencer.Step with PlanPipelineTransformerFactory with CnfPhase {

  object SetExtractor {
    def unapplySeq[T](s: Set[T]): Option[Seq[T]] = Some(s.toSeq)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    AndRewrittenToAnds,
    OrRewrittenToOrs
  ) ++ SemanticInfoAvailable

  override def postConditions: Set[StepSequencer.Condition] = Set(PredicatesSimplified)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  override def instance(from: BaseState, context: BaseContext): Rewriter = this(from.semantics())

}
