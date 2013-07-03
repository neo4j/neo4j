/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan

import scala.collection.Seq
import org.junit.Test
import org.junit.Assert._
import org.neo4j.cypher.internal.commands.{HasLabel, NodeById, Query, ReturnItem}
import org.neo4j.graphdb.{DynamicLabel, GraphDatabaseService}
import org.scalatest.Assertions
import org.neo4j.cypher.{PlanDescription, GraphDatabaseTestBase, InternalException}
import java.util.concurrent._
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.pipes.{FilterPipe, ExecuteUpdateCommandsPipe, Pipe, QueryState}
import org.neo4j.cypher.internal.commands.expressions.Identifier
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundQueryContext
import org.neo4j.cypher.internal.commands.values.TokenType.{Label, PropertyKey}
import org.neo4j.cypher.internal.mutation.DeletePropertyAction
import org.neo4j.cypher.internal.symbols.SymbolTable

class ExecutionPlanBuilderTest extends GraphDatabaseTestBase with Assertions with Timed with MockitoSugar {
  @Test def should_not_accept_returning_the_input_execution_plan() {
    val q = Query.empty
    val planContext = mock[PlanContext]

    val exception = intercept[ExecutionException](timeoutAfter(5) {
      val epi = new FakeExecPlanBuilder(graph, Seq(new BadBuilder))
      epi.build(planContext, q)
    })

    assertTrue("Execution plan builder didn't throw expected exception - was " + exception.getMessage,
      exception.getCause.isInstanceOf[InternalException])
  }

  @Test def should_close_transactions_if_pipe_throws_when_creating_iterator() {
    // given
    val tx = graph.beginTx()
    val q = Query.start(NodeById("x", 0)).returns(ReturnItem(Identifier("x"), "x"))

    val execPlanBuilder = new FakeExecPlanBuilder(graph, Seq(new ExplodingPipeBuilder))
    val queryContext = new TransactionBoundQueryContext(graph, tx, statementContext)

    // when
    intercept[ExplodingException] {
      val executionPlan = execPlanBuilder.build(planContext, q)
      executionPlan.execute(queryContext, Map())
    }

    // then
    assertNull("Expected no transactions left open", graph.getTxManager.getTransaction)

  }

  @Test def should_resolve_property_keys() {
    // given
    val tx = graph.beginTx()
    val node = graph.createNode()
    node.setProperty("foo", 12l)

    val identifier = Identifier("x")
    val q = Query
      .start(NodeById("x", node.getId))
      .updates(DeletePropertyAction(identifier, PropertyKey("foo")))
      .returns(ReturnItem(Identifier("x"), "x"))

    val execPlanBuilder = new ExecutionPlanBuilder(graph)
    val queryContext = new TransactionBoundQueryContext(graph, tx, statementContext)
    val pkId = queryContext.getPropertyKeyId("foo")

    // when
    val commands = execPlanBuilder.buildPipes(planContext, q)._1.asInstanceOf[ExecuteUpdateCommandsPipe].commands

    assertTrue("Property was not resolved", commands == Seq(DeletePropertyAction(identifier, PropertyKey("foo", pkId))))
  }

  @Test def should_resolve_label_ids() {
    // given
    val tx = graph.beginTx()
    val node = graph.createNode(DynamicLabel.label("Person"))

    val identifier = Identifier("x")
    val q = Query
      .start(NodeById("x", node.getId))
      .where(HasLabel(Identifier("x"), Label("Person")))
      .returns(ReturnItem(Identifier("x"), "x"))

    val execPlanBuilder = new ExecutionPlanBuilder(graph)
    val queryContext = new TransactionBoundQueryContext(graph, tx, statementContext)
    val labelId = queryContext.getLabelId("Person")

    // when
    val predicate = execPlanBuilder.buildPipes(planContext, q)._1.asInstanceOf[FilterPipe].predicate

    assertTrue("Label was not resolved", predicate == HasLabel(Identifier("x"), Label("Person", labelId)))
  }
}

class FakeExecPlanBuilder(gds: GraphDatabaseService, myBuilders: Seq[PlanBuilder]) extends ExecutionPlanBuilder(gds) {
  override lazy val builders = myBuilders
}

// This is a builder that accepts everything, but changes nothing
// It's a never ending loop waiting to happen
class BadBuilder extends LegacyPlanBuilder {
  def apply(plan: ExecutionPlanInProgress) = plan

  def canWorkWith(plan: ExecutionPlanInProgress) = true

  def priority = 0
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

  def priority: Int = 0

  class ExplodingPipe extends Pipe {
    def internalCreateResults(state: QueryState) = throw new ExplodingException

    def symbols: SymbolTable = new SymbolTable()

    def executionPlanDescription: PlanDescription = null
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