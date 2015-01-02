/**
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
package org.neo4j.cypher.internal.compiler.v2_0

import org.neo4j.cypher.internal.compiler.v2_0.commands._
import commands.expressions.Identifier
import commands.values.TokenType.{Label, PropertyKey}
import pipes._
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.{GraphDatabaseTestSupport, GraphDatabaseJUnitSuite, InternalException}
import org.neo4j.graphdb.{Transaction, DynamicLabel, GraphDatabaseService}
import scala.collection.Seq
import org.junit.Test
import org.junit.Assert._
import java.util.concurrent._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.cypher.internal.spi.v2_0.TransactionBoundQueryContext
import org.neo4j.cypher.internal.compiler.v2_0.executionplan._
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_0.mutation.{CreateNode, DeletePropertyAction}
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import javax.transaction.TransactionManager
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_0.commands.ReturnItem
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.ExecutionPlanInProgress
import org.neo4j.cypher.internal.compiler.v2_0.pipes.ExecuteUpdateCommandsPipe
import org.neo4j.cypher.internal.compiler.v2_0.commands.HasLabel
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryState
import java.net.URL

class ExecutionPlanBuilderTest extends CypherFunSuite with GraphDatabaseTestSupport with Timed with MockitoSugar {
  test("should not accept returning the input execution plan") {
    val q = Query.empty
    val planContext = mock[PlanContext]

    val exception = intercept[ExecutionException](timeoutAfter(5) {
      val epi = new FakeExecPlanBuilder(graph, Seq(new BadBuilder))
      epi.build(planContext, q)
    })

    assertTrue("Execution plan builder didn't throw expected exception - was " + exception.getMessage,
      exception.getCause.isInstanceOf[InternalException])
  }

  test("should close transactions if pipe throws when creating iterator") {
    // given
    val tx = graph.beginTx()
    val q = Query.start(NodeById("x", 0)).returns(ReturnItem(Identifier("x"), "x"))

    val execPlanBuilder = new FakeExecPlanBuilder(graph, Seq(new ExplodingPipeBuilder))
    val queryContext = new TransactionBoundQueryContext(graph, tx, statement)

    // when
    intercept[ExplodingException] {
      val executionPlan = execPlanBuilder.build(planContext, q)
      executionPlan.execute(queryContext, Map())
    }

    // then
    val txManager: TransactionManager = graph.getDependencyResolver.resolveDependency(classOf[TransactionManager])
    assertNull("Expected no transactions left open", txManager.getTransaction)
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

      val execPlanBuilder = new ExecutionPlanBuilder(graph)
      val queryContext = new TransactionBoundQueryContext(graph, tx, statement)
      val pkId = queryContext.getPropertyKeyId("foo")

      // when
      val commands = execPlanBuilder.buildPipes(planContext, q)._1.asInstanceOf[ExecuteUpdateCommandsPipe].commands

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

      val execPlanBuilder = new ExecutionPlanBuilder(graph)
      val queryContext = new TransactionBoundQueryContext(graph, tx, statement)
      val labelId = queryContext.getLabelId("Person")

      // when
      val predicate = execPlanBuilder.buildPipes(planContext, q)._1.asInstanceOf[FilterPipe].predicate

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

      val execPlanBuilder = new ExecutionPlanBuilder(graph)
      val (pipe: Pipe, _) = execPlanBuilder.buildPipes(planContext, q)

      toSeq(pipe) should equal (Seq(
        classOf[EmptyResultPipe],
        classOf[ExecuteUpdateCommandsPipe],
        classOf[EagerPipe],
        classOf[NodeStartPipe],
        classOf[NullPipe]
      ))
    }
  }

  test("should not wrap a LOAD CSV pipe in an eager pipe if the query contains updates") {
    graph.inTx {
      // LOAD CSV "file:///tmp/foo.csv" AS line CREATE ()
      val q = Query
        .start(LoadCSV(withHeaders = false, new URL("file:///tmp/foo.csv"), "line"))
        .tail(Query
          .updates(CreateNode("  UNNAMED3456", Map.empty, Seq.empty))
          .returns()
        )
        .returns(AllIdentifiers())

      val execPlanBuilder = new ExecutionPlanBuilder(graph)
      val (pipe: Pipe, _) = execPlanBuilder.buildPipes(planContext, q)

      toSeq(pipe) should equal (Seq(
        classOf[EmptyResultPipe],
        classOf[ExecuteUpdateCommandsPipe],
        classOf[LoadCSVPipe],
        classOf[NullPipe]
      ))
    }
  }
}

class FakeExecPlanBuilder(gds: GraphDatabaseService, builders: Seq[PlanBuilder]) extends ExecutionPlanBuilder(gds) {
  override val phases = new Phase { def myBuilders: Seq[PlanBuilder] = builders }
}

// This is a builder that accepts everything, but changes nothing
// It's a never ending loop waiting to happen
class BadBuilder extends PlanBuilder {
  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext) = plan

  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext) = true
}

class ExplodingPipeBuilder extends PlanBuilder with MockitoSugar {
  def canWorkWith(plan: ExecutionPlanInProgress, ctx: PlanContext): Boolean =
    !plan.pipe.isInstanceOf[ExplodingPipe]

  def apply(plan: ExecutionPlanInProgress, ctx: PlanContext) = {
      val psq = mock[PartiallySolvedQuery]
      when(psq.isSolved).thenReturn(true)
      when(psq.tail).thenReturn(None)

      plan.copy(pipe = new ExplodingPipe, query = psq)
    }

  class ExplodingPipe extends Pipe {
    def internalCreateResults(state: QueryState) = throw new ExplodingException

    def symbols: SymbolTable = new SymbolTable()

    def executionPlanDescription: PlanDescription = null

    def exists(pred: Pipe => Boolean) = ???
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
