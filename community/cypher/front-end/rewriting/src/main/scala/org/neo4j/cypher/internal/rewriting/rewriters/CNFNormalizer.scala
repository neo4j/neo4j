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
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.Xor
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.conditions.PatternExpressionsHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.topDown

case object AndRewrittenToAnds extends StepSequencer.Condition

case class deMorganRewriter()(implicit monitor: AstRewritingMonitor) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val step = Rewriter.lift {
    case p@Xor(expr1, expr2) =>
      And(Or(expr1, expr2)(p.position), Not(And(expr1.endoRewrite(copyVariables), expr2.endoRewrite(copyVariables))(p.position))(p.position))(p.position)
    case p@Not(And(exp1, exp2)) =>
      Or(Not(exp1)(p.position), Not(exp2)(p.position))(p.position)
    case p@Not(Or(exp1, exp2)) =>
      And(Not(exp1)(p.position), Not(exp2)(p.position))(p.position)
  }

  private val instance: Rewriter = repeatWithSizeLimit(bottomUp(step))(monitor)
}

object distributeLawsRewriter {
  // converting from DNF to CNF is exponentially expensive, so we only do it for a small amount of clauses
  // see https://en.wikipedia.org/wiki/Conjunctive_normal_form#Conversion_into_CNF
  val DNF_CONVERSION_LIMIT = 8
}

case class distributeLawsRewriter()(implicit monitor: AstRewritingMonitor) extends Rewriter {
  def apply(that: AnyRef): AnyRef = {
    if (dnfCounts(that) < distributeLawsRewriter.DNF_CONVERSION_LIMIT)
      instance(that)
    else {
      monitor.abortedRewritingDueToLargeDNF(that)
      that
    }
  }

  private def dnfCounts(value: Any) = value.treeFold(1) {
    case Or(lhs, a: And) => acc => TraverseChildren(acc + 1)
    case Or(a: And, rhs) => acc => TraverseChildren(acc + 1)
  }

  private val step = Rewriter.lift {
    case p@Or(exp1, And(exp2, exp3)) => And(Or(exp1, exp2)(p.position), Or(exp1.endoRewrite(copyVariables), exp3)(p.position))(p.position)
    case p@Or(And(exp1, exp2), exp3) => And(Or(exp1, exp3)(p.position), Or(exp2, exp3.endoRewrite(copyVariables))(p.position))(p.position)
  }

  private val instance: Rewriter = repeatWithSizeLimit(bottomUp(step))(monitor)
}

case object flattenBooleanOperators extends Rewriter with StepSequencer.Step {
  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private val firstStep: Rewriter = Rewriter.lift {
    case p@And(lhs, rhs) => Ands(Seq(lhs, rhs))(p.position)
    case p@Or(lhs, rhs)  => Ors(Seq(lhs, rhs))(p.position)
  }

  private val secondStep: Rewriter = Rewriter.lift {
    case p@Ands(exprs) => Ands(exprs.flatMap {
      case Ands(inner) => inner
      case x => Set(x)
    })(p.position)
    case p@Ors(exprs) => Ors(exprs.flatMap {
      case Ors(inner) => inner
      case x => Set(x)
    })(p.position)
  }

  private val instance = inSequence(bottomUp(firstStep), fixedPoint(bottomUp(secondStep)))

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(AndRewrittenToAnds)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty
}

case class simplifyPredicates(semanticState: SemanticState) extends Rewriter {
  private val T = True()(null)
  private val F = False()(null)

  private val step: Rewriter = Rewriter.lift({ case e: Expression => computeReplacement(e) })

  private val instance = fixedPoint(topDown(step))

  def apply(that: AnyRef): AnyRef = instance.apply(that)

  private def computeReplacement(e: Expression): Expression = e match {
    case n@Not(Not(innerExpression))  => {
      val newExp = computeReplacement(innerExpression)
      if (needsToBeExplicitlyCoercedToBoolean(n, newExp))
        CoerceToPredicate(newExp)
      else
        newExp
    }
    case Ands(exps) if exps.isEmpty     =>  throw new IllegalStateException("Found an instance of Ands with empty expressions")
    case Ors(exps) if exps.isEmpty      =>  throw new IllegalStateException("Found an instance of Ors with empty expressions")
    case p@Ands(exps) if exps.contains(T) =>
      val nonTrue = exps.filterNot(T == _)
      if (nonTrue.isEmpty) True()(p.position) else Ands(nonTrue)(p.position)
    case p@Ors(exps) if exps.contains(F)  =>
      val nonFalse = exps.filterNot(F == _)
      if (nonFalse.isEmpty) False()(p.position) else Ors(nonFalse)(p.position)
    case p@Ors(exps) if exps.contains(T)  => True()(p.position)
    case p@Ands(exps) if exps.contains(F) => False()(p.position)
    case a => a
  }

  /**
   * We intend to remove {@code not} from the AST and replace it with {@code exp}.
   *
   * While {@code not} would have converted the value to boolean, we check here whether that information would be lost.
   */
  private def needsToBeExplicitlyCoercedToBoolean(not: Not, exp: Expression) = {
    val expectedToBeBoolean = semanticState.expressionType(not).expected.exists(_.contains(CTBoolean))
    val specifiedToBeBoolean = semanticState.expressionType(exp).specified.contains(CTBoolean)
    !expectedToBeBoolean && !specifiedToBeBoolean
  }
}

object simplifyPredicates extends ASTRewriterFactory {
  override def getRewriter(innerVariableNamer: InnerVariableNamer,
                           semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory): Rewriter = simplifyPredicates(semanticState)
}

case object NoInequalityInsideNot extends StepSequencer.Condition

case object normalizeSargablePredicates extends Rewriter with StepSequencer.Step with ASTRewriterFactory {

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(NoInequalityInsideNot)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    ProjectionClausesHaveSemanticInfo, // It can invalidate this condition by rewriting things inside WITH/RETURN.
    PatternExpressionsHaveSemanticInfo, // It can invalidate this condition by rewriting things inside PatternExpressions.
  )

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {

    // remove not from inequality expressions by negating them
    case Not(inequality: InequalityExpression) =>
      inequality.negated
  })

  override def getRewriter(innerVariableNamer: InnerVariableNamer,
                           semanticState: SemanticState,
                           parameterTypeMapping: Map[String, CypherType],
                           cypherExceptionFactory: CypherExceptionFactory): Rewriter = instance
}
