/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2

import java.util.concurrent._

import org.junit.Assert._
import org.mockito.Mockito._
import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compatibility.WrappedMonitors
import org.neo4j.cypher.internal.compiler.v2_2.ast.Statement
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.{Identifier, Literal}
import org.neo4j.cypher.internal.compiler.v2_2.commands.values.TokenType.{Label, PropertyKey}
import org.neo4j.cypher.internal.compiler.v2_2.commands.{ReturnItem, _}
import org.neo4j.cypher.internal.compiler.v2_2.executionplan.{ExecutionPlanInProgress, _}
import org.neo4j.cypher.internal.compiler.v2_2.mutation.{CreateNode, DeletePropertyAction}
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.LogicalPlanRewriter
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.{DefaultQueryPlanner, SimpleMetricsFactory}
import org.neo4j.cypher.internal.compiler.v2_2.planner.{CostBasedPipeBuilderFactory, PlanningMonitor, SemanticTable}
import org.neo4j.cypher.internal.compiler.v2_2.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_2.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.spi.v2_2.TransactionBoundQueryContext
import org.neo4j.graphdb.DynamicLabel
import org.neo4j.helpers.Clock
import org.scalatest.mock.MockitoSugar

import scala.collection.Seq

class RulePipeBuilderTest
  extends CypherFunSuite
  with GraphDatabaseTestSupport
  with Timed
  with MockitoSugar {

  val ast = mock[Statement]
  val rewriterSequencer = RewriterStepSequencer.newValidating _
  val queryPlanner = new DefaultQueryPlanner(LogicalPlanRewriter(rewriterSequencer))
  val planner = CostBasedPipeBuilderFactory(mock[Monitors], SimpleMetricsFactory, mock[PlanningMonitor], Clock.SYSTEM_CLOCK, queryPlanner, rewriterSequencer, mock[SemanticChecker])

  class FakePreparedQuery(q: AbstractQuery)
    extends PreparedQuery(mock[Statement], "q", Map.empty)(SemanticTable(), Set.empty, Scope(Map.empty, Seq.empty)) {

    override def abstractQuery: AbstractQuery = q

    override def isPeriodicCommit: Boolean = q.isInstanceOf[PeriodicCommitQuery]

    override def rewrite(rewriter: Rewriter): PreparedQuery = this
  }


  test("should not accept returning the input execution plan") {
    val q = Query.empty
    val planContext = mock[PlanContext]

    val exception = intercept[ExecutionException](timeoutAfter(5) {
      val pipeBuilder = new LegacyPipeBuilderWithCustomPlanBuilders(Seq(new BadBuilder), new WrappedMonitors(kernelMonitors))
      val query = new FakePreparedQuery(q)
      pipeBuilder.producePlan(query, planContext)
    })

    assertTrue("Execution plan builder didn't throw expected exception - was " + exception.getMessage,
      exception.getCause.isInstanceOf[InternalException])
  }

  test("should resolve property keys") {
    // given
    val tx = graph.beginTx()
    try {
      val node = graph.createNode()
      node.setProperty("foo", 12l)

      val identifier = Identifier("x")
      val q = Query
        .start(NodeById("x", node.getId))
        .updates(DeletePropertyAction(identifier, PropertyKey("foo")))
        .returns(ReturnItem(Identifier("x"), "x"))

      val pipeBuilder = new LegacyPipeBuilder(new WrappedMonitors(kernelMonitors), RewriterStepSequencer.newValidating _)
      val queryContext = new TransactionBoundQueryContext(graph, tx, isTopLevelTx = true, statement)
      val pkId = queryContext.getPropertyKeyId("foo")
      val parsedQ = new FakePreparedQuery(q)

      // when

      val commands = pipeBuilder.producePlan(parsedQ, planContext).pipe.asInstanceOf[ExecuteUpdateCommandsPipe].commands

      assertTrue("Property was not resolved", commands == Seq(DeletePropertyAction(identifier, PropertyKey("foo", pkId))))
    } finally {
      tx.close()
    }
  }

  test("should resolve label ids") {
    // given
    val tx = graph.beginTx()
    try {
      val node = graph.createNode(DynamicLabel.label("Person"))

      val q = Query
        .start(NodeById("x", node.getId))
        .where(HasLabel(Identifier("x"), Label("Person")))
        .returns(ReturnItem(Identifier("x"), "x"))

      val execPlanBuilder = new LegacyPipeBuilder(new WrappedMonitors(kernelMonitors), RewriterStepSequencer.newValidating _)
      val queryContext = new TransactionBoundQueryContext(graph, tx, isTopLevelTx = true, statement)
      val labelId = queryContext.getLabelId("Person")
      val parsedQ = new FakePreparedQuery(q)

      // when
      val predicate = execPlanBuilder.producePlan(parsedQ, planContext).pipe.asInstanceOf[FilterPipe].predicate

      assertTrue("Label was not resolved", predicate == HasLabel(Identifier("x"), Label("Person", labelId)))
    } finally {
      tx.close()
    }
  }

  def toSeq(pipe: Pipe): Seq[Class[_ <: Pipe]] = {
    Seq(pipe.getClass) ++ pipe.sources.headOption.toSeq.flatMap(toSeq)
  }

  test("should wrap a lazy pipe in an eager pipe if the query contains updates") {
    graph.inTx {
      // MATCH n CREATE ()
      val q = Query
        .matches(SingleNode("n"))
        .tail(Query
          .updates(CreateNode("  UNNAMED3456", Map.empty, Seq.empty))
          .returns()
        )
        .returns(AllIdentifiers())
      val parsedQ = new FakePreparedQuery(q)

      val pipeBuilder = new LegacyPipeBuilder(new WrappedMonitors(kernelMonitors), RewriterStepSequencer.newValidating _)
      val pipe = pipeBuilder.producePlan(parsedQ, planContext).pipe

      toSeq(pipe) should equal (Seq(
        classOf[EmptyResultPipe],
        classOf[ExecuteUpdateCommandsPipe],
        classOf[EagerPipe],
        classOf[NodeStartPipe],
        classOf[SingleRowPipe]
      ))
    }
  }

  test("should not wrap a LOAD CSV pipe in an eager pipe if the query contains updates") {
    graph.inTx {
      // LOAD CSV "file:///tmp/foo.csv" AS line CREATE ()
      val q = Query
        .start(LoadCSV(withHeaders = false, new Literal("file:///tmp/foo.csv"), "line", None))
        .tail(Query
          .updates(CreateNode("  UNNAMED3456", Map.empty, Seq.empty))
          .returns()
        )
        .returns(AllIdentifiers())
      val parsedQ = new FakePreparedQuery(q)


      val execPlanBuilder = new LegacyPipeBuilder(new WrappedMonitors(kernelMonitors), RewriterStepSequencer.newValidating _)
      val pipe = execPlanBuilder.producePlan(parsedQ, planContext).pipe

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
    val tx = graph.beginTx()
    try {
      val q = PeriodicCommitQuery(
        Query.
          start(CreateNodeStartItem(CreateNode("n", Map.empty, Seq.empty))).
          returns(),
        None
      )
      val parsedQ = new FakePreparedQuery(q)

      val pipeBuilder = new LegacyPipeBuilder(new WrappedMonitors(kernelMonitors), RewriterStepSequencer.newValidating _)

      // when
      val periodicCommit = pipeBuilder.producePlan(parsedQ, planContext).periodicCommit

      assert(periodicCommit === Some(PeriodicCommitInfo(None)))
    } finally {
      tx.close()
    }
  }
}

class LegacyPipeBuilderWithCustomPlanBuilders(builders: Seq[PlanBuilder], monitors:Monitors) extends LegacyPipeBuilder(monitors, RewriterStepSequencer.newValidating _) {
  override val phases = new Phase { def myBuilders: Seq[PlanBuilder] = builders }
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
    def symbols: SymbolTable = new SymbolTable()
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
