/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_1.parser.{ParserMonitor, CypherParser}
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito._
import org.neo4j.cypher.internal.compiler.v2_1.ast
import ast.convert.StatementConverters._
import org.neo4j.kernel.api.index.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v2_1.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_1.ParsedQuery
import org.neo4j.cypher.internal.compiler.v2_1.Monitors


class LegacyPipeBuilderTest extends MockitoSugar {
  val planContext: PlanContext = mock[PlanContext]
  val parser = new CypherParser(mock[ParserMonitor])
  val planBuilder = new LegacyPipeBuilder(mock[Monitors])

  @Test def should_use_distinct_pipe_for_distinct() {
    val pipe = buildExecutionPipe("MATCH n RETURN DISTINCT n")

    assert(pipe.exists(_.isInstanceOf[DistinctPipe]), "Expected a DistinctPipe but didn't find any")
  }

  @Test def should_use_traversal_matcher_when_possible() {

    when(planContext.getOptLabelId("Foo")).thenReturn(Some(1))

    val pipe = buildExecutionPipe("match (n:Foo)-->(x) return x")

    assert(pipe.exists(_.isInstanceOf[TraversalMatchPipe]), "Expected a DistinctPipe but didn't find any")
  }

  @Test def should_use_schema_index_with_load_csv() {

    when(planContext.getOptLabelId("Person")).thenReturn(Some(1))
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(1))
    when(planContext.getIndexRule("Person", "name")).thenReturn(Some(new IndexDescriptor(1, 1)))
    when(planContext.getUniquenessConstraint("Person", "name")).thenReturn(None)

    val pipe = buildExecutionPipe("LOAD CSV FROM 'file:///tmp/foo.csv' AS line MATCH (p:Person { name: line[0] }) RETURN p;")
    assert(pipe.exists { pipe =>
      pipe.isInstanceOf[NodeStartPipe] && pipe.asInstanceOf[NodeStartPipe].createSource.producerType == "SchemaIndex"
    })
  }

  @Test def should_use_schema_index_with_load_csv_2() {

    when(planContext.getOptLabelId("Person")).thenReturn(Some(1))
    when(planContext.getOptPropertyKeyId("name")).thenReturn(Some(1))
    when(planContext.getIndexRule("Person", "name")).thenReturn(Some(new IndexDescriptor(1, 1)))
    when(planContext.getUniquenessConstraint("Person", "name")).thenReturn(None)

    val pipe = buildExecutionPipe("LOAD CSV FROM 'file:///tmp/foo.csv' AS line MATCH (p:Person { name: \"Foo Bar Baz\" }) RETURN p;")
    assert(pipe.exists { pipe =>
      pipe.isInstanceOf[NodeStartPipe] && pipe.asInstanceOf[NodeStartPipe].createSource.producerType == "SchemaIndex"
    })
  }

  private def buildExecutionPipe(q: String): Pipe = {
    val statement = parser.parse(q)
    val parsedQ = ParsedQuery(statement, statement.asQuery, mock[SemanticTable], q)
    planBuilder.producePlan(parsedQ, planContext).pipe
  }
}
