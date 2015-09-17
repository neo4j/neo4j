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
package org.neo4j.cypher.docgen.tooling.tests

import org.neo4j.cypher.ExecutionEngine
import org.neo4j.cypher.docgen.tooling._
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.test.TestGraphDatabaseFactory

class QueryResultContentBuilderTest extends CypherFunSuite {
  test("should handle no content being required back from the query") {
    val result = runQuery("match (n) return n", NoContent)

    result should equal(NoContent)
  }

  test("should handle query with result table output and empty results") {
    val result = runQuery("match (n) return n", QueryResultTable)

    result should equal(QueryResult(Seq("n"), Seq.empty, footer = "0 rows"))
  }

  test("should handle query with result table output and non-empty results") {
    val result = runQuery("match (x) return x", QueryResultTable, init = "CREATE ()").asInstanceOf[QueryResult]

    result.columns should equal(Seq("x"))
    result.footer should equal("1 row")
    result.rows should have size 1
  }

  test("should handle simple query with result table output") {
    val result = runQuery("match (n) return n", Paragraph("hello world") ~ QueryResultTable)

    result should equal(Paragraph("hello world") ~ QueryResult(Seq("n"), Seq.empty, footer = "0 rows"))
  }

  test("updating query should report changes") {
    val result = runQuery("create ()-[:T]->()", Paragraph("start") ~ QueryResultTable ~ Paragraph("end"))

    result should equal(Paragraph("start") ~ QueryResult(Seq(), Seq.empty, footer = "0 rows\nNodes created: 2\nRelationships created: 1\n") ~ Paragraph("end"))
  }

  def runQuery(query: String, content: Content, init: String = ""): Content = {
    val db = new TestGraphDatabaseFactory().newImpermanentDatabase()
    if (init != "") db.execute(init)
    val engine = new ExecutionEngine(db)
    val builder = QueryResultContentBuilder
    val queryResult = RewindableExecutionResult(engine.execute(query))

    builder.apply(queryResult, content, db)
  }
}
