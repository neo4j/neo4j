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
package org.neo4j.cypher

import org.neo4j.cypher.internal.helpers.TxCounts
import org.neo4j.graphdb.Node

class PeriodicCommitAcceptanceTest extends ExecutionEngineFunSuite with TxCountsTrackingTestSupport {

  test("should reject periodic commit when not updating") {
    evaluating {
      executeScalar("USING PERIODIC COMMIT 200 MATCH (n) RETURN count(n)")
    } should produce[SyntaxException]
  }

  test("should produce data from periodic commit") {
    val result = execute("USING PERIODIC COMMIT 200 CREATE (n {id: 42}) RETURN n.id")

    result.toList should equal(List(Map("n.id" -> 42)))
    result.columns should equal(List("n.id"))
  }

  test("should support simple periodic commit with aligned batch size commits") {
    // given
    val queryText =
      "USING PERIODIC COMMIT 2 " +
      "CREATE () " +
      "CREATE () " +
      "WITH * MATCH (n) RETURN count(n) AS updates"

    // when
    val (expectedUpdates, txCounts) = executeScalarAndTrackTxCounts[Number](queryText)

    // then
    expectedUpdates should equal(2)

    // and then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("should support simple periodic commit with unaligned batch size") {
    // given
    val queryText =
      "USING PERIODIC COMMIT 3 " +
      "CREATE () " +
      "CREATE () " +
      "CREATE () " +
      "CREATE () " +
      "WITH * MATCH (n) RETURN count(n) AS updates"

    // when
    val (expectedUpdates, txCounts) = executeScalarAndTrackTxCounts[Number](queryText)

    // then
    expectedUpdates should equal(4)

    // and then
    txCounts should equal(TxCounts(commits = 2))
  }

  test("should support periodic commit with aligned batch size") {
    /*
      10 x nodes         => 30 updates
      4  y nodes         => 12 updates
      40 r relationships => 40 updates
     */

    // given
    val queryText =
        "USING PERIODIC COMMIT 41 " +
        "FOREACH (x IN range(0, 9) | " +
        "  MERGE (n:X {x: x}) " +
        "  FOREACH (y IN range(0, 3) | " +
        "    MERGE (m:Y {y: y}) " +
        "    MERGE (n:X)-[:R]->(m:Y)) " +
        ") " +
        "WITH * MATCH (:X)-[r:R]->(:Y) RETURN 30 + 12 + count(r) AS updates"

    // when
    val (expectedUpdates, txCounts) = executeScalarAndTrackTxCounts[Number](queryText)

    // then
    expectedUpdates should equal(82)

    // and then
    txCounts should equal(TxCounts(commits = 3))
  }

  test("should support periodic commit with unaligned batch size") {
    /*
      10 x nodes         => 30 updates
      4  y nodes         => 12 updates
      40 r relationships => 40 updates
     */

    // given
    val queryText =
      "USING PERIODIC COMMIT 20 " +
        "FOREACH (x IN range(0, 9) | " +
        "  MERGE (n:X {x: x}) " +
        "  FOREACH (y IN range(0, 3) | " +
        "    MERGE (m:Y {y: y}) " +
        "    MERGE (n:X)-[:R]->(m:Y)) " +
        ") " +
        "WITH * MATCH (:X)-[r:R]->(:Y) RETURN 30 + 12 + count(r) AS updates"

    // when
    val (expectedUpdates, txCounts) = executeScalarAndTrackTxCounts[Number](queryText)

    // then
    expectedUpdates should equal(82)

    // and then
    txCounts should equal(TxCounts(commits = 5))
  }

  test("should abort first tx when failing on first batch during periodic commit") {
    // given
    val queryText = "USING PERIODIC COMMIT 256 FOREACH (x IN range(0, 1023) | CREATE ({x: 1/0}))"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[ArithmeticException](executeScalar[Number](queryText)))

    // then
    txCounts should equal(TxCounts(rollbacks = 1))
  }

  test("should commit first tx and abort second tx when failing on second batch during periodic commit") {
    // given
    // creating 256 means 512 updates, indeed 1) create node and set the label
    val queryText = "USING PERIODIC COMMIT 256 FOREACH (x IN range(0, 1023) | CREATE ({x: 1/(300-x)}))"

    // when
    val (_, txCounts) = prepareAndTrackTxCounts(intercept[ArithmeticException](executeScalar[Number](queryText)))

    // then
    txCounts should equal(TxCounts(commits = 2, rollbacks = 1))
  }

  test("should support periodic commit hint without explicit size") {
    executeScalar[Node]("USING PERIODIC COMMIT CREATE (n) RETURN n")
  }

  test("should support periodic commit hint with explicit size") {
    executeScalar[Node]("USING PERIODIC COMMIT 400 CREATE (n) RETURN n")
  }

  test("should reject periodic commit hint with negative size") {
    evaluating {
      executeScalar[Node]("USING PERIODIC COMMIT -1 CREATE (n) RETURN n")
    } should produce[SyntaxException]
  }

  test("should fail if periodic commit is executed in an open transaction") {
    // given
    evaluating {
      graph.inTx {
        execute("USING PERIODIC COMMIT CREATE ()")
      }
    } should produce[PeriodicCommitInOpenTransactionException]
  }
}
