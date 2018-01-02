/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.tracing.rewriters.RewriterStepSequencer
import org.neo4j.cypher.internal.compiler.v2_3.{Monitors, PreparedQuery, devNullLogger}
import org.neo4j.cypher.internal.frontend.v2_3.parser.CypherParser
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{Scope, SemanticTable}

class RuleExecutablePlanBuilderTest extends CypherFunSuite {
  val planContext: PlanContext = mock[PlanContext]
  val parser = new CypherParser
  val planBuilder = new LegacyExecutablePlanBuilder(mock[Monitors], RewriterStepSequencer.newValidating)

  test("should_use_distinct_pipe_for_distinct") {
    val pipe = buildExecutionPipe("MATCH n RETURN DISTINCT n")

    pipe.exists(_.isInstanceOf[DistinctPipe]) should equal(true)
  }

  test("should_use_traversal_matcher_when_possible") {

    when(planContext.getOptLabelId("Foo")).thenReturn(Some(1))

    val pipe = buildExecutionPipe("match (n:Foo)-->(x) return x")

    pipe.exists(_.isInstanceOf[TraversalMatchPipe]) should equal(true)
  }

  test("should_use_schema_index_with_load_csv") {

    when(planContext.getOptLabelId("Person")).thenReturn(Some(1))
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(1))
    when(planContext.getIndexRule("Person", "name")).thenReturn(Some(IndexDescriptor(1, 1)))
    when(planContext.getUniquenessConstraint("Person", "name")).thenReturn(None)

    val pipe = buildExecutionPipe("LOAD CSV FROM 'file:///tmp/foo.csv' AS line MATCH (p:Person { name: line[0] }) RETURN p;")
    pipe.exists { pipe =>
      pipe.isInstanceOf[NodeStartPipe] && pipe.asInstanceOf[NodeStartPipe].createSource.producerType == "SchemaIndex"
    } should equal(true)
  }

  test("should_use_schema_index_with_load_csv_2") {

    when(planContext.getOptLabelId("Person")).thenReturn(Some(1))
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(1))
    when(planContext.getIndexRule("Person", "name")).thenReturn(Some(IndexDescriptor(1, 1)))
    when(planContext.getUniquenessConstraint("Person", "name")).thenReturn(None)

    val pipe = buildExecutionPipe("LOAD CSV FROM 'file:///tmp/foo.csv' AS line MATCH (p:Person { name: \"Foo Bar Baz\" }) RETURN p;")
    pipe.exists { pipe =>
      pipe.isInstanceOf[NodeStartPipe] && pipe.asInstanceOf[NodeStartPipe].createSource.producerType == "SchemaIndex"
    } should equal(true)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create") {
    val pipe = buildExecutionPipe("MATCH (a), (b) CREATE (a)-[:TYPE]->(b)")
    assertPipeDoesNotExist(pipe, classOf[EagerPipe])
  }

  test("should not introduce an eager pipe between two node reads and a relationships create when theres is sorting between the two") {
    val pipe = buildExecutionPipe("MATCH (a), (b) WITH a, b ORDER BY id(a) CREATE (a)-[:TYPE]->(b)")
    assertPipeDoesNotExist(pipe, classOf[EagerPipe])
  }

  test("should introduce an eager pipe between a node read and a relationship + node create") {
    val pipe = buildExecutionPipe("MATCH (a) CREATE (a)-[:TYPE]->()")
    assertPipeExists(pipe, classOf[EagerPipe])
  }

  test("should introduce an eager pipe between a relationship read and a relationship create") {
    val pipe = buildExecutionPipe("MATCH (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b)")
    assertPipeExists(pipe, classOf[EagerPipe])
  }

  def assertPipeExists[T](pipe: Pipe, klass: Class[T]) {
    assert(pipe.exists(_.getClass == klass), s"Expected to contain a pipe of type $klass. Got: $pipe")
  }

  def assertPipeDoesNotExist[T](pipe: Pipe, klass: Class[T]) {
    assert(!pipe.exists(_.getClass == klass), s"Expected not to contain a pipe of type $klass. Got: $pipe")
  }

  private def buildExecutionPipe(q: String): Pipe = {
    val statement = parser.parse(q)
    val parsedQ = PreparedQuery(statement, q, Map.empty)(mock[SemanticTable], Set.empty, mock[Scope], devNullLogger)
    planBuilder.producePlan(parsedQ, planContext).pipe
  }
}
