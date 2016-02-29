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
package org.neo4j.cypher.docgen.tooling.tests

import org.neo4j.cypher.ExecutionEngineHelper
import org.neo4j.cypher.docgen.tooling._
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService
import org.neo4j.test.TestGraphDatabaseFactory

class QueryResultContentBuilderTest extends CypherFunSuite with GraphIcing with ExecutionEngineHelper {

  val graph = new GraphDatabaseCypherService(new TestGraphDatabaseFactory().newImpermanentDatabase())
  val eengine = new ExecutionEngine(graph)

  test("should handle query with result table output and empty results") {
    val result = runQuery("match (n) return n")

    result should equal(QueryResultTable(Seq("n"), Seq.empty, footer = "0 rows"))
  }

  test("should handle query with result table output and non-empty results") {
    val result = runQuery("match (x) return x", init = "CREATE ()").asInstanceOf[QueryResultTable]

    result.columns should equal(Seq("x"))
    result.footer should equal("1 row")
    result.rows should have size 1
  }

  def runQuery(query: String, init: String = ""): Content = {
    if (init != "") graph.execute(init)
    val builder = new QueryResultContentBuilder(x => x.toString)
    val queryResult = execute(query)
    builder.apply(queryResult)
  }
}
