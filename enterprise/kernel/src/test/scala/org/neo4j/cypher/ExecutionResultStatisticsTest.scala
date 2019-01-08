/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
