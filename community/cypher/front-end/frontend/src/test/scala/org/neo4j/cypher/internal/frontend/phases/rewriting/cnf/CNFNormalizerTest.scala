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

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStats
import org.neo4j.cypher.internal.frontend.phases.InternalSyntaxUsageStatsNoOp
import org.neo4j.cypher.internal.frontend.phases.Monitors
import org.neo4j.cypher.internal.frontend.phases.PreparatoryRewriting.SemanticAnalysisPossible
import org.neo4j.cypher.internal.frontend.phases.SemanticAnalysis
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.CNFNormalizer.steps
import org.neo4j.cypher.internal.frontend.phases.transitiveEqualities
import org.neo4j.cypher.internal.rewriting.AstRewritingMonitor
import org.neo4j.cypher.internal.rewriting.PredicateTestSupport
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatestplus.mockito.MockitoSugar

class CNFNormalizerTest extends CypherFunSuite with PredicateTestSupport {

  final private val cnfNormalizerTransformer = CNFNormalizerTest.getTransformer(Nil)
  var rewriter: Rewriter = _
  var astRewritingMonitor: AstRewritingMonitor = _

  test("should not touch a simple predicate") {
    P <=> P
  }

  test("should lift plain AND") {
    and(P, Q) <=> ands(P, Q)
  }

  test("should flatten multiple ANDs in a ANDS") {
    and(P, and(R, S)) <=> ands(P, R, S)
    and(and(R, S), P) <=> ands(R, S, P)
  }

  test("should be able to convert a dnf to cnf") {
    or(and(P, Q), and(R, S)) <=> ands(ors(P, R), ors(Q, R), ors(P, S), ors(Q, S))
  }

  test("should be able to convert a dnf with nested ands to cnf") {
    or(and(P, and(Q, V)), and(R, S)) <=> ands(ors(P, R), ors(Q, R), ors(V, R), ors(P, S), ors(Q, S), ors(V, S))
  }

  test("should be able to convert a complex formula to cnf") {
    or(xor(P, Q), xor(R, S)) <=> ands(
      ors(P, Q, R, S),
      ors(not(P), not(Q), R, S),
      ors(P, Q, not(R), not(S)),
      ors(not(P), not(Q), not(R), not(S))
    )
  }

  test("should be able to simplify conjunction/disjunction with boolean literal") {
    and(P, TRUE) <=> bool(P)
    and(P, FALSE) <=> FALSE
    or(P, TRUE) <=> TRUE
    or(P, FALSE) <=> bool(P)

    and(TRUE, P) <=> bool(P)
    and(FALSE, P) <=> FALSE
    or(TRUE, P) <=> TRUE
    or(FALSE, P) <=> bool(P)
  }

  test("should be able to simplify nested conjunction/disjunction with boolean literal") {
    and(and(P, Q), TRUE) <=> ands(P, Q)
    and(and(P, Q), FALSE) <=> FALSE
    or(and(P, Q), TRUE) <=> TRUE
    or(and(P, Q), FALSE) <=> ands(P, Q)

    and(TRUE, and(P, Q)) <=> ands(P, Q)
    and(FALSE, and(P, Q)) <=> FALSE
    or(TRUE, and(P, Q)) <=> TRUE
    or(FALSE, and(P, Q)) <=> ands(P, Q)
  }

  test("aborts cnf-rewriting for the worst case scenarios") {
    /* GIVEN A PATHOLOGICAL CASE FOR CNF
    When we get predicates in certain shapes, the normalized form for the predicate is so large it becomes
    intractable to plan on, and the better alternative is to simply give up on normalizing and let the planner
    produces a sub-optimal plan instead.
     */

    val p1 = anExp("p1")
    val p2 = anExp("p2")
    val p3 = anExp("p3")
    val p4 = anExp("p4")
    val p5 = anExp("p5")
    val p6 = anExp("p6")
    val p7 = anExp("p7")
    val p8 = anExp("p8")
    val p9 = anExp("p9")
    val p10 = anExp("p10")
    val p11 = anExp("p11")
    val p12 = anExp("p12")
    val p13 = anExp("p13")
    val p14 = anExp("p14")
    val p15 = anExp("p15")
    val p16 = anExp("p16")
    val p17 = anExp("p17")
    val p18 = anExp("p18")
    val p19 = anExp("p19")
    val p20 = anExp("p20")

    val bigPredicate =
      or(
        or(
          or(
            or(
              and(p1, p2),
              and(p3, p4)
            ),
            or(
              and(p5, p6),
              and(p7, p8)
            )
          ),
          or(
            or(
              and(p9, p10),
              and(p11, p12)
            ),
            or(
              and(p13, p14),
              and(p15, p16)
            )
          )
        ),
        or(
          and(p17, p18),
          and(p19, p20)
        )
      )

    // When
    bigPredicate.rewrite(rewriter)

    // Then the rewriting was aborted
    verify(astRewritingMonitor).abortedRewritingDueToLargeDNF(any())
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    val monitors = mock[Monitors]
    astRewritingMonitor = mock[AstRewritingMonitor]
    when(monitors.newMonitor[AstRewritingMonitor]()).thenReturn(astRewritingMonitor)
    rewriter = {
      case e: Expression =>
        val initialState =
          InitialState("", None, NoPlannerName, new AnonymousVariableNameGenerator()).withStatement(TestStatement(e))
        val finalState = cnfNormalizerTransformer.transform(initialState, new TestContext(monitors))
        val expression = finalState.statement() match {
          case TestStatement(e) => e
          case x                => fail(s"Expected TestStatement but was ${x.getClass}")
        }
        expression
      case x => fail(s"Expected Expression but was ${x.getClass}")
    }
  }
}

object CNFNormalizerTest {

  case class SemanticWrapper(semanticFeatures: List[SemanticFeature])
      extends Transformer[BaseContext, BaseState, BaseState] with StepSequencer.Step {

    private val transformer =
      SemanticAnalysis.getTransformer(
        pushdownPropertyReads = false,
        semanticFeatures
      )

    override def preConditions: Set[Condition] = SemanticAnalysis.preConditions

    override def postConditions: Set[Condition] = SemanticAnalysis.postConditions

    override def invalidatedConditions: Set[Condition] = SemanticAnalysis.invalidatedConditions

    override def transform(from: BaseState, context: BaseContext): BaseState = transformer.transform(from, context)

    override def name: String = transformer.name
  }

  def getTransformer(semanticFeatures: List[SemanticFeature]): Transformer[BaseContext, BaseState, BaseState] = {
    val orderedSteps: Seq[Transformer[BaseContext, BaseState, BaseState] with StepSequencer.Step] =
      StepSequencer[Transformer[BaseContext, BaseState, BaseState] with StepSequencer.Step]()
        .orderSteps(
          Set[Transformer[BaseContext, BaseState, BaseState] with StepSequencer.Step](
            transitiveEqualities,
            SemanticWrapper(semanticFeatures)
          ) ++ steps,
          initialConditions = Set(
            BaseContains[Statement](),
            SemanticAnalysisPossible
          )
        )
        .steps

    orderedSteps.reduceLeft[Transformer[BaseContext, BaseState, BaseState]]((t1, t2) => t1 andThen t2)
  }
}

/**
 * Rewriters work on states, which reference a statement. As we often care about the rewriting of expressions (instead of statements), this object helps to bridge that gap.
 */
object TestStatement {

  def apply(e: Expression): Statement = {
    val returnClause = Return(ReturnItems(
      includeExisting = false,
      Seq(AliasedReturnItem(e, Variable("")(InputPosition.NONE))(InputPosition.NONE))
    )(InputPosition.NONE))(InputPosition.NONE)
    SingleQuery(Seq(returnClause))(InputPosition.NONE)
  }

  def unapply(s: Statement): Option[Expression] = s match {
    case SingleQuery(Seq(Return(_, ReturnItems(_, Seq(AliasedReturnItem(expression, _)), _), _, _, _, _, _))) =>
      Some(expression)
    case _ => None
  }
}

object TestContext extends MockitoSugar {
  def apply() = new TestContext(mock[Monitors])

  def apply(monitors: Monitors): TestContext = new TestContext(monitors)
}

class TestContext(override val monitors: Monitors) extends BaseContext {
  override def tracer: CompilationPhaseTracer = CompilationPhaseTracer.NO_TRACING

  override def notificationLogger: InternalNotificationLogger = ???

  override def cypherExceptionFactory: CypherExceptionFactory = ???

  override def errorHandler: Seq[SemanticErrorDef] => Unit = _ => ()

  override def errorMessageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  override def cancellationChecker: CancellationChecker = CancellationChecker.NeverCancelled

  override def internalSyntaxUsageStats: InternalSyntaxUsageStats = InternalSyntaxUsageStatsNoOp
}
