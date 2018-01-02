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
package org.neo4j.cypher

class ExecutionResultStatisticsTest extends ExecutionEngineFunSuite with EnterpriseGraphDatabaseTestSupport {

  test("correct statistics for added node property existence constraint") {
    val result = execute("create constraint on (n:Person) assert exists(n.name)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 1)
    assert(stats.existenceConstraintsRemoved === 0)
  }

  test("correct statistics for node property existence constraint added twice") {
    execute("create constraint on (n:Person) assert exists(n.name)")
    val result = execute("create constraint on (n:Person) assert exists(n.name)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 0)
    assert(stats.existenceConstraintsRemoved === 0)
  }

  test("correct statistics for dropped node property existence constraint") {
    execute("create constraint on (n:Person) assert exists(n.name)")
    val result = execute("drop constraint on (n:Person) assert exists(n.name)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 0)
    assert(stats.existenceConstraintsRemoved === 1)
  }

  test("correct statistics for added relationship property existence constraint") {
    val result = execute("create constraint on ()-[r:KNOWS]-() assert exists(r.since)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 1)
    assert(stats.existenceConstraintsRemoved === 0)
  }

  test("correct statistics for relationship property existence constraint added twice") {
    execute("create constraint on ()-[r:KNOWS]-() assert exists(r.since)")
    val result = execute("create constraint on ()-[r:KNOWS]-() assert exists(r.since)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 0)
    assert(stats.existenceConstraintsRemoved === 0)
  }

  test("correct statistics for dropped relationship property existence constraint") {
    execute("create constraint on ()-[r:KNOWS]-() assert exists(r.since)")
    val result = execute("drop constraint on ()-[r:KNOWS]-() assert exists(r.since)")
    val stats = result.queryStatistics()

    assert(stats.existenceConstraintsAdded === 0)
    assert(stats.existenceConstraintsRemoved === 1)
  }
}
