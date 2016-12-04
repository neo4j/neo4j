/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.profiler

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.{NestedPipeExpression, ProjectedPath}
import org.neo4j.cypher.internal.compiler.v3_2.pipes._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v3_2.planDescription._
import org.neo4j.cypher.internal.compiler.v3_2.spi.QueryContext
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

import scala.collection.immutable.::

class ProfilerTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("should report simplest case") {
    //GIVEN
    val start = SingleRowPipe()()
    val pipe = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 20)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription("single row" -> start, "foo" -> pipe)

    //WHEN
    materialize(pipe.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 20)
  }

  private def createPlanDescription(first: (String, Pipe), tail: (String, Pipe)*): InternalPlanDescription = {
    val firstDescr: InternalPlanDescription = PlanDescriptionImpl(first._2.id, first._1, NoChildren, Seq.empty, Set.empty)
    tail.foldLeft(firstDescr) {
      case (descr, (name, pipe)) => PlanDescriptionImpl(pipe.id, name, SingleChild(descr), Seq.empty, Set.empty)
    }
  }

  test("should report multiple pipes case") {
    //GIVEN
    val start = SingleRowPipe()()
    val pipe1 = ProfilerTestPipe(start, "foo", rows = 10, dbAccess = 25)
    val pipe2 = ProfilerTestPipe(pipe1, "bar", rows = 20, dbAccess = 40)
    val pipe3 = ProfilerTestPipe(pipe2, "baz", rows = 1, dbAccess = 2)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription("single row" -> start, "foo" -> pipe1, "bar" -> pipe2, "baz" -> pipe3)

    //WHEN
    materialize(pipe3.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 25)
    assertRecorded(decoratedResult, "bar", expectedRows = 20, expectedDbHits = 40)
    assertRecorded(decoratedResult, "baz", expectedRows = 1, expectedDbHits = 2)
  }

  test("should count stuff going through Apply multiple times") {
    val s1 = SingleRowPipe()()
    // GIVEN
    val lhs = ProfilerTestPipe(s1, "lhs", rows = 10, dbAccess = 10)
    val s2 = SingleRowPipe()()
    val rhs = ProfilerTestPipe(s2, "rhs", rows = 20, dbAccess = 30)
    val apply = ApplyPipe(lhs, rhs)()
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription(
      "start1" -> s1,
      "start2" -> s2,
      "lhs" -> lhs,
      "rhs" -> rhs,
      "apply" -> apply)

    // WHEN we create the results,
    materialize(apply.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    // THEN
    assertRecorded(decoratedResult, "rhs", expectedRows = 10 * 20, expectedDbHits = 10 * 30)
  }

  test("count dbhits for NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val DB_HITS = 100
    val start1 = SingleRowPipe()()
    val testPipe = ProfilerTestPipe(start1, "nested pipe", rows = 10, dbAccess = DB_HITS)
    val innerPipe = NestedPipeExpression(testPipe, projectedPath)
    val start2 = SingleRowPipe()()
    val pipeUnderInspection = ProjectionPipe(start2, Map("x" -> innerPipe))()

    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)
    val planDescription = createPlanDescription(
      "start1" -> start1,
      "start2" -> start2,
      "lhs" -> testPipe,
      "Projection" -> pipeUnderInspection)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val decoratedResult = profiler.decorate(planDescription, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = DB_HITS)
  }

  test("count dbhits for deeply nested NestedPipes") {
    // GIVEN
    val projectedPath = mock[ProjectedPath]
    val DB_HITS = 100
    val start1 = SingleRowPipe()()
    val start2 = SingleRowPipe()()
    val start3 = SingleRowPipe()()
    val profiler1 = ProfilerTestPipe(start1, "nested pipe1", rows = 10, dbAccess = DB_HITS)
    val nestedExpression = NestedPipeExpression(profiler1, projectedPath)
    val innerInnerPipe = ProjectionPipe(start2, Map("y" -> nestedExpression))()
    val profiler2 = ProfilerTestPipe(innerInnerPipe, "nested pipe2", rows = 10, dbAccess = DB_HITS)
    val pipeExpression = NestedPipeExpression(profiler2, projectedPath)
    val pipeUnderInspection = ProjectionPipe(start3, Map("x" -> pipeExpression))()

    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(pipeUnderInspection.createResults(queryState))
    val description = createPlanDescription(
      "start1" -> start1,
      "start2" -> start2,
      "start3" -> start3,
      "profiler1" -> profiler1,
      "profiler2" -> profiler2,
      "innerInner" -> innerInnerPipe,
      "Projection" -> pipeUnderInspection
    )
    val decoratedResult = profiler.decorate(description, isProfileReady = true)

    // THEN the ProjectionNewPipe has correctly recorded the dbhits
    assertRecorded(decoratedResult, "Projection", expectedRows = 1, expectedDbHits = DB_HITS * 2)
  }

    test("should not count rows multiple times when the same pipe is used multiple times") {
      val profiler = new Profiler

      val pipe1 = SingleRowPipe()()
      val iter1 = Iterator(ExecutionContext.empty, ExecutionContext.empty, ExecutionContext.empty)

      val profiled1 = profiler.decorate(pipe1, iter1)
      profiled1.toList // consume it
      profiled1.asInstanceOf[ProfilingIterator].count should equal(3)

      val pipe2 = SingleRowPipe()()
      val iter2 = Iterator(ExecutionContext.empty, ExecutionContext.empty)

      val profiled2 = profiler.decorate(pipe2, iter2)
      profiled2.toList // consume it
      profiled2.asInstanceOf[ProfilingIterator].count should equal(2)
    }

    test("should not count dbhits multiple times when the same pipe is used multiple times") {
      val profiler = new Profiler

      val pipe1 = SingleRowPipe()()
      val ctx1 = mock[QueryContext]
      val state1 = QueryStateHelper.emptyWith(ctx1, mock[ExternalCSVResource])

      val profiled1 = profiler.decorate(pipe1, state1)
      profiled1.query.createNode()
      profiled1.query.asInstanceOf[ProfilingQueryContext].count should equal(1)

      val pipe2 = SingleRowPipe()()
      val ctx2 = mock[QueryContext]
      val state2 = QueryStateHelper.emptyWith(ctx2, mock[ExternalCSVResource])


      val profiled2 = profiler.decorate(pipe2, state2)
      profiled2.query.createNode()
      profiled2.query.asInstanceOf[ProfilingQueryContext].count should equal(1)
    }

  private def assertRecorded(result: InternalPlanDescription, name: String, expectedRows: Int, expectedDbHits: Int) {
    val pipeArgs: Seq[Argument] = result.find(name).flatMap(_.arguments)
    pipeArgs shouldNot be(empty)
    pipeArgs.collect {
      case DbHits(count) => withClue("DbHits:")(count should equal(expectedDbHits))
    }

    pipeArgs.collect {
      case Rows(seenRows) => withClue("Rows:")(seenRows should equal(expectedRows))
    }
  }

  private def materialize(iterator: Iterator[_]) {
    iterator.size
  }
}

case class ProfilerTestPipe(source: Pipe, name: String, rows: Int, dbAccess: Int)  // MATCH a, ()-[r]->() WHERE id(r) = length(a-->())
                  (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  var id = new Id

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.size
    (0 until dbAccess).foreach(x => state.query.createNode())
    (0 until rows).map(x => ExecutionContext.empty).toIterator
  }

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    val other = copy(source = source)
    other.id = id
    other
  }
}
