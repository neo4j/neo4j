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
package org.neo4j.cypher.internal.compiler.v2_2.profiler

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription.Arguments.{DbHits, Rows}
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.{Argument, InternalPlanDescription}
import org.neo4j.cypher.internal.compiler.v2_2.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable

import scala.collection.immutable.::

class ProfilerTest extends CypherFunSuite {

  private implicit val monitor = mock[PipeMonitor]

  test("should report simplest case") {
    //GIVEN
    val start = SingleRowPipe()
    val pipe = new ProfilerPipe(start, "foo", rows = 10, dbAccess = 20)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe.createResults(queryState))
    val decoratedResult = profiler.decorate(pipe.planDescription, true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 20)
  }

  test("should report multiple pipes case") {
    //GIVEN
    val start = SingleRowPipe()
    val pipe1 = new ProfilerPipe(start, "foo", rows = 10, dbAccess = 25)
    val pipe2 = new ProfilerPipe(pipe1, "bar", rows = 20, dbAccess = 40)
    val pipe3 = new ProfilerPipe(pipe2, "baz", rows = 1, dbAccess = 2)
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    //WHEN
    materialize(pipe3.createResults(queryState))
    val decoratedResult = profiler.decorate(pipe3.planDescription, true)

    //THEN
    assertRecorded(decoratedResult, "foo", expectedRows = 10, expectedDbHits = 25)
    assertRecorded(decoratedResult, "bar", expectedRows = 20, expectedDbHits = 40)
    assertRecorded(decoratedResult, "baz", expectedRows = 1, expectedDbHits = 2)
  }

  test("should ignore null pipe in profile") {
    // GIVEN
    val pipes = UnionPipe(List(SingleRowPipe(), SingleRowPipe()), List())
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    // THEN it should not throw an assertion about profiling the same pipe twice.
    materialize(pipes.createResults(queryState))
  }

  test("should count stuff going through Apply multiple times") {
    // GIVEN
    val lhs = new ProfilerPipe(SingleRowPipe(), "lhs", rows = 10, dbAccess = 10)
    val rhs = new ProfilerPipe(SingleRowPipe(), "rhs", rows = 20, dbAccess = 30)
    val apply = new ApplyPipe(lhs, rhs)()
    val queryContext = mock[QueryContext]
    val profiler = new Profiler
    val queryState = QueryStateHelper.emptyWith(query = queryContext, decorator = profiler)

    // WHEN we create the results,
    materialize(apply.createResults(queryState))
    val decoratedResult = profiler.decorate(apply.planDescription, isProfileReady = true)

    // THEN
    assertRecorded(decoratedResult, "rhs", expectedRows = 10*20, expectedDbHits = 10*30)
  }

  private def assertRecorded(result: InternalPlanDescription, name: String, expectedRows: Int, expectedDbHits: Int) {
    val pipeArgs: Seq[Argument] = result.find(name).flatMap(_.arguments)

    pipeArgs.foreach {
      case DbHits(count) => count should equal(expectedDbHits)
      case _ =>
    }

    pipeArgs.collectFirst {
      case Rows(seenRows) => seenRows should equal(expectedRows)
    }
  }

  private def materialize(iterator: Iterator[_]) {
    iterator.size
  }
}

case class ProfilerPipe(source: Pipe, name: String, rows: Int, dbAccess: Int)
                  (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) {
  def planDescription: InternalPlanDescription = source.planDescription.andThen(this, name, Set())

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.size
    (0 until dbAccess).foreach(x => state.query.createNode())
    (0 until rows).map(x => ExecutionContext.empty).toIterator
  }

  def symbols: SymbolTable = SymbolTable()

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources

    copy(source = source)
  }

}
