/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.result.ConcurrentTransactionsProfile

class QueryStatisticsTest extends CypherFunSuite {

  private def profile =
    new ConcurrentTransactionsProfile(10L, 8.5, 9L, 8.0, 1L, 2.0, 16L, 1024L, 1024L, 1024L, 1024L, 3L)

  test("concurrent transaction getters default to zero without a profile") {
    val stats = QueryStatistics()
    stats.concurrentTransactionsPeakConcurrency should equal(0L)
    stats.concurrentTransactionsAvgConcurrency should equal(0.0)
    stats.concurrentTransactionsPeakQueuedBatches should equal(0L)
    stats.concurrentTransactionsAvgQueuedBatches should equal(0.0)
    stats.concurrentTransactionsConcurrencyLimit should equal(0L)
    stats.concurrentTransactionsRetries should equal(0L)
  }

  test("concurrent transaction getters read from the attached profile") {
    val stats = QueryStatistics().withConcurrentTransactions(profile)
    stats.concurrentTransactions should contain(profile)
    stats.concurrentTransactionsPeakConcurrency should equal(10L)
    stats.concurrentTransactionsAvgConcurrency should equal(8.5)
    stats.concurrentTransactionsPeakQueuedBatches should equal(9L)
    stats.concurrentTransactionsAvgQueuedBatches should equal(8.0)
    stats.concurrentTransactionsConcurrencyLimit should equal(16L)
    stats.concurrentTransactionsRetries should equal(3L)
  }

  test("the concurrent transactions profile is diagnostic metadata and does not affect equality") {
    // assertStats and other comparisons compare statistics by value; the recorded concurrency must not
    // make two otherwise-identical statistics unequal.
    QueryStatistics(nodesCreated =
      1
    ).withConcurrentTransactions(profile) should equal(QueryStatistics(nodesCreated = 1))
    QueryStatistics(nodesCreated = 1).withConcurrentTransactions(profile).hashCode() should equal(
      QueryStatistics(nodesCreated = 1).hashCode()
    )
  }

  test("withConcurrentTransactions does not mutate the original statistics") {
    val original = QueryStatistics(nodesCreated = 1)
    original.withConcurrentTransactions(profile)
    original.concurrentTransactions should be(None)
  }

  test("adding statistics preserves an attached profile from either operand") {
    val withProfile = QueryStatistics(nodesCreated = 1).withConcurrentTransactions(profile)
    val plain = QueryStatistics(nodesCreated = 2)

    (withProfile + plain).concurrentTransactions should contain(profile)
    (plain + withProfile).concurrentTransactions should contain(profile)
    (plain + plain).concurrentTransactions should be(None)
  }

  test("adding statistics never merges two attached profiles, the left operand wins") {
    val otherProfile =
      new ConcurrentTransactionsProfile(2L, 1.5, 1L, 0.5, 1L, 1.0, 4L, 8L, 8L, 8L, 8L, 0L)
    val left = QueryStatistics().withConcurrentTransactions(profile)
    val right = QueryStatistics().withConcurrentTransactions(otherProfile)

    (left + right).concurrentTransactions should contain(profile)
  }

  test("subtracting statistics preserves an attached profile from either operand") {
    val withProfile = QueryStatistics(nodesCreated = 3).withConcurrentTransactions(profile)
    val plain = QueryStatistics(nodesCreated = 1)

    (withProfile - plain).concurrentTransactions should contain(profile)
    (withProfile - plain).nodesCreated should equal(2)
  }
}
