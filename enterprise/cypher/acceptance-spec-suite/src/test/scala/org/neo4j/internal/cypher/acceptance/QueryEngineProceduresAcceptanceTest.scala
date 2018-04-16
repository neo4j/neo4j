/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher._
import org.neo4j.logging.AssertableLogProvider

class QueryEngineProceduresAcceptanceTest extends ExecutionEngineFunSuite {

  test("Clearing the query caches should work with empty caches") {
    val query = "CALL dbms.clearQueryCaches()"
    val result = graph.execute(query)

    result.next().toString should equal ("{value=Query cache already empty.}")
    result.hasNext should be (false)
  }

  test("Clearing the query caches should work properly") {
    val q1 = "MATCH (n) RETURN n.prop"
    val q2 = "MATCH (n) WHERE n.prop = 3 RETURN n"
    val q3 = "MATCH (n) WHERE n.prop = 4 RETURN n"

    graph.execute(q1)
    graph.execute(q2)
    graph.execute(q3)
    graph.execute(q1)

    val query = "CALL dbms.clearQueryCaches()"
    val result = graph.execute(query)

    result.next().toString should equal ("{value=Query caches successfully cleared of 3 queries.}")
    result.hasNext should be (false)
  }

  test("Multiple calls to clearing the cache should work") {
    val q1 = "MATCH (n) RETURN n.prop"

    graph.execute(q1)

    val query = "CALL dbms.clearQueryCaches()"
    val result = graph.execute(query)

    result.next().toString should equal ("{value=Query caches successfully cleared of 1 queries.}")
    result.hasNext should be (false)

    val result2 = graph.execute(query)
    result2.next().toString should equal ("{value=Query cache already empty.}")
    result2.hasNext should be (false)

  }

  test("Test that cache clearing procedure writes to log") {
    val logProvider = new AssertableLogProvider()
    val graphDatabaseService = graphDatabaseFactory().setUserLogProvider(logProvider).newImpermanentDatabase()

    graphDatabaseService.execute("MATCH (n) RETURN n.prop")

    val query = "CALL dbms.clearQueryCaches()"
    graphDatabaseService.execute(query)

    logProvider.assertContainsLogCallContaining("Called dbms.clearQueryCaches(): Query caches successfully cleared of 1 queries.")
    graphDatabaseService.shutdown()

  }
}
