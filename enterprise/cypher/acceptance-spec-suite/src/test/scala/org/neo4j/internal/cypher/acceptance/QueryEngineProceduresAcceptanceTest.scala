/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
