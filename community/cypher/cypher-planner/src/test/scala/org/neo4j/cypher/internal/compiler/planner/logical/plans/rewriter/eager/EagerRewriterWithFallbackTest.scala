package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanTestOps
import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport
import org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter.eager.EagerRewriterWithFallbackTest.ThrowingEagerRewriter
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Attributes
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.LoneElement

class EagerRewriterWithFallbackTest extends CypherFunSuite with LogicalPlanTestOps with ProcedureTestSupport
    with LoneElement {

  test("should report errors from both rewriters") {
    val rewriter = EagerRewriterWithFallback(
      ThrowingEagerRewriter("primary error", null),
      ThrowingEagerRewriter("fallback error", null),
      null
    )

    val planBuilder = new LogicalPlanBuilder()
      .produceResults("n")
      .limit(5)
      .allNodeScan("n")

    val e = the[RuntimeException] thrownBy {
      rewriter.eagerize(planBuilder.build(), planBuilder.getSemanticTable, new AnonymousVariableNameGenerator())
    }

    e.getMessage shouldBe "fallback error"
    e.getSuppressed.toList.loneElement.getMessage shouldBe "primary error"
  }
}

class EagerRewriterWithFallbackPrimaryFailTest extends EagerEverywhereRewriterTestBase {

  override protected def rewriter(planBuilder: LogicalPlanBuilder): EagerRewriter = {
    val attributes: Attributes[LogicalPlan] = Attributes(planBuilder.idGen)
    EagerRewriterWithFallback(
      primaryRewriter = ThrowingEagerRewriter("error", attributes),
      fallbackRewriter = EagerEverywhereRewriter(attributes),
      attributes
    )
  }
}

class EagerRewriterWithFallbackFallbackFailTest extends EagerEverywhereRewriterTestBase {

  override protected def rewriter(planBuilder: LogicalPlanBuilder): EagerRewriter = {
    val attributes: Attributes[LogicalPlan] = Attributes(planBuilder.idGen)
    EagerRewriterWithFallback(
      primaryRewriter = EagerEverywhereRewriter(attributes),
      fallbackRewriter = ThrowingEagerRewriter("error", attributes),
      attributes
    )
  }
}

object EagerRewriterWithFallbackTest {

  case class ThrowingEagerRewriter(msg: String, attributes: Attributes[LogicalPlan]) extends EagerRewriter(attributes) {

    override def eagerize(
      plan: LogicalPlan,
      semanticTable: SemanticTable,
      anonymousVariableNameGenerator: AnonymousVariableNameGenerator
    ): LogicalPlan = {
      throw new RuntimeException(msg)
    }
  }
}
