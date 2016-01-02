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

import org.mockito.Mockito._
import org.neo4j.cypher.CypherException
import org.neo4j.cypher.docgen.tooling.RestartableDatabase
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite
import org.neo4j.test.TestGraphDatabaseFactory

class RestartableDatabaseTest extends CypherFunSuite {
  test("just creating a restartable database should not create any temp-dbs") {
    // given
    val databaseFactory = mock[TestGraphDatabaseFactory]

    // when
    new RestartableDatabase(Seq.empty, databaseFactory)

    // then
    verify(databaseFactory, never()).newImpermanentDatabase()
  }

  test("running two read queries should only need one database") {
    // given
    val databaseFactory = spy(new TestGraphDatabaseFactory())
    val db = new RestartableDatabase(Seq.empty, databaseFactory)

    // when
    db.execute("MATCH (n) RETURN n")
    db.nowIsASafePointToRestartDatabase()
    db.execute("MATCH (n) RETURN n")

    // then
    verify(databaseFactory, times(1)).newImpermanentDatabase()

    db.shutdown()
  }

  test("running two write queries should need two databases") {
    // given
    val databaseFactory = spy(new TestGraphDatabaseFactory())
    val db = new RestartableDatabase(Seq.empty, databaseFactory)

    // when
    db.execute("CREATE ()")
    db.nowIsASafePointToRestartDatabase()
    db.execute("CREATE ()")

    // then
    verify(databaseFactory, times(2)).newImpermanentDatabase()

    db.shutdown()
  }

  test("running two queries that throw exception should need two databases") {
    // given
    val databaseFactory = spy(new TestGraphDatabaseFactory())
    val db = new RestartableDatabase(Seq.empty, databaseFactory)

    // when
    intercept[CypherException](db.execute("THIS SHOULD FAIL"))
    db.nowIsASafePointToRestartDatabase()
    intercept[CypherException](db.execute("THIS SHOULD FAIL"))

    // then
    verify(databaseFactory, times(2)).newImpermanentDatabase()

    db.shutdown()
  }
}
