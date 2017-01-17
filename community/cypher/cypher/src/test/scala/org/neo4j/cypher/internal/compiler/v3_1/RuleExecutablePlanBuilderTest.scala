/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_1

import java.time.Clock
import java.util.Collections
import java.util.concurrent._

import org.junit.Assert._
import org.mockito.Mockito._
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.compatibility.WrappedMonitors3_1
import org.neo4j.cypher.internal.compiler.v3_1.commands.expressions.{Literal, Variable}
import org.neo4j.cypher.internal.compiler.v3_1.commands.predicates.HasLabel
import org.neo4j.cypher.internal.compiler.v3_1.commands.values.TokenType.{Label, PropertyKey}
import org.neo4j.cypher.internal.compiler.v3_1.commands.{ReturnItem, _}
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{ExecutionPlanInProgress, _}
import org.neo4j.cypher.internal.compiler.v3_1.helpers.IdentityTypeConverter
import org.neo4j.cypher.internal.compiler.v3_1.mutation.{CreateNode, DeletePropertyAction}
import org.neo4j.cypher.internal.compiler.v3_1.pipes._
import org.neo4j.cypher.internal.compiler.v3_1.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_1.planner.CostBasedPipeBuilderFactory
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.{DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v3_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v3_1.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.frontend.v3_1.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_1.{InternalException, Rewriter, Scope, SemanticTable}
import org.neo4j.cypher.internal.spi.TransactionalContextWrapperv3_1
import org.neo4j.cypher.internal.spi.v3_1.TransactionBoundQueryContext
import org.neo4j.cypher.internal.spi.v3_1.codegen.GeneratedQueryStructure
import org.neo4j.graphdb.Label.label
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.security.{AnonymousContext, SecurityContext}
import org.neo4j.kernel.impl.coreapi.PropertyContainerLocker
import org.neo4j.kernel.impl.query.{Neo4jTransactionalContextFactory, QuerySource}
import org.scalatest.mock.MockitoSugar

import scala.collection.Seq

class RuleExecutablePlanBuilderTest
  extends CypherFunSuite
  with GraphDatabaseTestSupport
  with Timed
  with MockitoSugar {

  val locker: PropertyContainerLocker = new PropertyContainerLocker
  val ast = mock[Statement]
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  val queryPlanner = DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))
  val planner = CostBasedPipeBuilderFactory.create(
    monitors = mock[Monitors],
    metricsFactory = SimpleMetricsFactory,
    queryPlanner = queryPlanner,
    rewriterSequencer = rewriterSequencer,
    plannerName = None,
    runtimeBuilder =  SilentFallbackRuntimeBuilder(InterpretedPlanBuilder(Clock.systemUTC(), mock[Monitors], IdentityTypeConverter), CompiledPlanBuilder(Clock.systemUTC(),GeneratedQueryStructure)),
    semanticChecker = mock[SemanticChecker],
    updateStrategy = None,
    config = config,
    publicTypeConverter = identity
  )

  class FakePreparedSemanticQuery(q: AbstractQuery)
    extends PreparedQuerySemantics(mock[Statement], "q", None, Map.empty, mock[SemanticTable], mock[Scope])() {

    override def abstractQuery(notificationLogger: InternalNotificationLogger): AbstractQuery = q

    override def isPeriodicCommit: Boolean = q.isInstanceOf[PeriodicCommitQuery]

    override def rewrite(rewriter: Rewriter): PreparedQuerySemantics = this
  }

  test("should not accept returning the input execution plan") {
    val q = Query.empty
    val planContext = mock[PlanContext]
    when(planContext.notificationLogger()).thenReturn(devNullLogger)

    val exception = intercept[ExecutionException](timeoutAfter(5) {
      val pipeBuilder = new LegacyExecutablePlanBuilderWithCustomPlanBuilders(Seq(new BadBuilder), WrappedMonitors3_1(kernelMonitors), config)
      val query = new FakePreparedSemanticQuery(q)
      pipeBuilder.producePipe(query, planContext, CompilationPhaseTracer.NO_TRACING)
    })

    assertTrue("Execution plan builder didn't throw expected exception - was " + exception.getMessage,
      exception.getCause.isInstanceOf[InternalException])
  }

  test("should resolve property keys") {
    // given
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.writeToken() )
    try {
      val node = graph.createNode()
      node.setProperty("foo", 12l)

      val variable = Variable("x")
      val q = Query
        .start(NodeById("x", node.getId))
        .updates(DeletePropertyAction(variable, PropertyKey("foo")))
        .returns(ReturnItem(Variable("x"), "x"))

      val pipeBuilder = new LegacyExecutablePlanBuilder(new WrappedMonitors3_1(kernelMonitors), config,
        RewriterStepSequencer.newValidating, typeConverter = IdentityTypeConverter)

      val contextFactory = Neo4jTransactionalContextFactory.create(graph, locker)
      val transactionalContext = TransactionalContextWrapperv3_1(contextFactory.newContext(QuerySource.UNKNOWN, tx, "X", Collections.emptyMap()))
      val queryContext = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
      val pkId = queryContext.getPropertyKeyId("foo")
      val parsedQ = new FakePreparedSemanticQuery(q)

      // when

      val commands = pipeBuilder.producePipe(parsedQ, planContext, CompilationPhaseTracer.NO_TRACING).pipe.asInstanceOf[ExecuteUpdateCommandsPipe].commands

      assertTrue("Property was not resolved", commands == Seq(DeletePropertyAction(variable, PropertyKey("foo", pkId))))
    } finally {
      tx.close()
    }
  }

  test("should resolve label ids") {
    // given
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.writeToken() )
    try {
      val node = graph.createNode(label("Person"))

      val q = Query
        .start(NodeById("x", node.getId))
        .where(HasLabel(Variable("x"), Label("Person")))
        .returns(ReturnItem(Variable("x"), "x"))

      val execPlanBuilder = new LegacyExecutablePlanBuilder(WrappedMonitors3_1(kernelMonitors), config,
                                                            RewriterStepSequencer.newValidating, typeConverter = IdentityTypeConverter)
      val contextFactory = Neo4jTransactionalContextFactory.create(graph, locker)
      val transactionalContext = TransactionalContextWrapperv3_1(contextFactory.newContext(QuerySource.UNKNOWN, tx, "X", Collections.emptyMap()))
      val queryContext = new TransactionBoundQueryContext(transactionalContext)(indexSearchMonitor)
      val labelId = queryContext.getLabelId("Person")
      val parsedQ = new FakePreparedSemanticQuery(q)

      // when
      val predicate = execPlanBuilder.producePipe(parsedQ, planContext,CompilationPhaseTracer.NO_TRACING).pipe.asInstanceOf[FilterPipe].predicate

      assertTrue("Label was not resolved", predicate == HasLabel(Variable("x"), Label("Person", labelId)))
    } finally {
      tx.close()
    }
  }

  def toSeq(pipe: Pipe): Seq[Class[_ <: Pipe]] = {
    Seq(pipe.getClass) ++ pipe.sources.headOption.toSeq.flatMap(toSeq)
  }

  test("should not wrap a lazy pipe in an eager pipe if the query contains updates, if the lazy pipe is a 'left-side' leaf") {
    graph.inTx {
      // MATCH n CREATE ()
      val q = Query
        .matches(SingleNode("n"))
        .tail(Query
          .updates(CreateNode("  UNNAMED3456", Map.empty, Seq.empty))
          .returns()
        )
        .returns(AllVariables())
      val parsedQ = new FakePreparedSemanticQuery(q)

      val pipeBuilder = new LegacyExecutablePlanBuilder(WrappedMonitors3_1(kernelMonitors), config, RewriterStepSequencer.newValidating,
        typeConverter = IdentityTypeConverter)
      val pipe = pipeBuilder.producePipe(parsedQ, planContext, CompilationPhaseTracer.NO_TRACING).pipe

      toSeq(pipe) should equal (Seq(
        classOf[EmptyResultPipe],
        classOf[ExecuteUpdateCommandsPipe],
        classOf[NodeStartPipe],
        classOf[SingleRowPipe]
      ))
    }
  }

  test("should wrap a lazy pipe in an eager pipe if the query contains updates, if the lazy pipe is a not a 'left-side' leaf") {
    graph.inTx {
      // MATCH n CREATE ()
      val q = Query
        .matches(SingleNode("n"))
        .tail(Query
          .matches(SingleNode("n2"))
          .tail(Query
            .updates(CreateNode("  UNNAMED3456", Map.empty, Seq.empty))
            .returns()
          )
          .returns()
        )
        .returns(AllVariables())
      val parsedQ = new FakePreparedSemanticQuery(q)

      val pipeBuilder = new LegacyExecutablePlanBuilder(WrappedMonitors3_1(kernelMonitors), config, RewriterStepSequencer.newValidating,
        typeConverter = IdentityTypeConverter)
      val pipe = pipeBuilder.producePipe(parsedQ, planContext, CompilationPhaseTracer.NO_TRACING).pipe

      toSeq(pipe) should equal (Seq(
        classOf[EmptyResultPipe],
        classOf[ExecuteUpdateCommandsPipe],
        classOf[EagerPipe],
        classOf[NodeStartPipe],
        classOf[NodeStartPipe],
        classOf[SingleRowPipe]
      ))
    }
  }

  test("should not wrap a LOAD CSV pipe in an eager pipe if the query contains updates") {
    graph.inTx {
      // LOAD CSV "file:///tmp/foo.csv" AS line CREATE ()
      val q = Query
        .start(LoadCSV(withHeaders = false, Literal("file:///tmp/foo.csv"), "line", None))
        .tail(Query
          .updates(CreateNode("  UNNAMED3456", Map.empty, Seq.empty))
          .returns()
        )
        .returns(AllVariables())
      val parsedQ = new FakePreparedSemanticQuery(q)


      val execPlanBuilder = new LegacyExecutablePlanBuilder(WrappedMonitors3_1(kernelMonitors), config, RewriterStepSequencer.newValidating,
        typeConverter = IdentityTypeConverter)
      val pipe = execPlanBuilder.producePipe(parsedQ, planContext, CompilationPhaseTracer.NO_TRACING).pipe

      toSeq(pipe) should equal (Seq(
        classOf[EmptyResultPipe],
        classOf[ExecuteUpdateCommandsPipe],
        classOf[LoadCSVPipe],
        classOf[SingleRowPipe]
      ))
    }
  }

  test("should set the periodic commit flag") {
    // given
    val tx = graph.beginTransaction(KernelTransaction.Type.explicit, AnonymousContext.read())
    try {
      val q = PeriodicCommitQuery(
        Query.
          start(CreateNodeStartItem(CreateNode("n", Map.empty, Seq.empty))).
          returns(),
        None
      )
      val parsedQ = new FakePreparedSemanticQuery(q)

      val pipeBuilder = new LegacyExecutablePlanBuilder(WrappedMonitors3_1(kernelMonitors), config, RewriterStepSequencer.newValidating,
        typeConverter = IdentityTypeConverter)

      // when
      val periodicCommit = pipeBuilder.producePipe(parsedQ, planContext, CompilationPhaseTracer.NO_TRACING).periodicCommit

      assert(periodicCommit === Some(PeriodicCommitInfo(None)))
    } finally {
      tx.close()
    }
  }
}

class LegacyExecutablePlanBuilderWithCustomPlanBuilders(innerBuilders: Seq[PlanBuilder], monitors: Monitors, config: CypherCompilerConfiguration)
  extends LegacyExecutablePlanBuilder(monitors, config, RewriterStepSequencer.newValidating, typeConverter = IdentityTypeConverter) {
  override val phases = new Phase { def myBuilders: Seq[PlanBuilder] = innerBuilders }
}

// This is a builder that accepts everything, but changes nothing
// It's a never ending loop waiting to happen
class BadBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = plan

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = true
}

class ExplodingPipeBuilder extends PlanBuilder with MockitoSugar {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) =
    !plan.pipe.isInstanceOf[ExplodingPipe]

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext)(implicit pipeMonitor: PipeMonitor) = {
      val psq = mock[PartiallySolvedQuery]
      when(psq.isSolved).thenReturn(true)
      when(psq.tail).thenReturn(None)

      plan.copy(pipe = new ExplodingPipe, query = psq)
    }

  class ExplodingPipe extends Pipe {
    def internalCreateResults(state: QueryState) = throw new ExplodingException
    def symbols: SymbolTable = SymbolTable()
    def planDescription: InternalPlanDescription = null
    def exists(pred: Pipe => Boolean) = ???
    val monitor = mock[PipeMonitor]
    def dup(sources: List[Pipe]): Pipe = ???
    def sources: scala.Seq[Pipe] = ???
    def localEffects: Effects = ???
  }
}

class ExplodingException extends Exception

trait Timed {
  def timeoutAfter(timeout: Long)(codeToTest: => Unit) {
    val executor = Executors.newSingleThreadExecutor()
    val future = executor.submit(new Runnable {
      def run() {
        codeToTest
      }
    })

    future.get(1, TimeUnit.SECONDS)

    executor.shutdownNow()
  }
}
