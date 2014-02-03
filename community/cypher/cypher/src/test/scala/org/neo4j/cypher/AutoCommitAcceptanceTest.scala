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

import org.scalatest.Matchers
import org.junit.{Ignore, Test}
import org.neo4j.cypher.internal.helpers.{TxCounts, GraphIcing}
import org.neo4j.graphdb.Node

class AutoCommitAcceptanceTest extends ExecutionEngineJUnitSuite with Matchers {

  @Test
  def should_reject_autocommit_when_not_updating() {
    evaluating {
      executeScalar("USING AUTOCOMMIT 200 MATCH (n) RETURN count(n)")
    } should produce[SyntaxException]
  }

  @Test
  def should_support_simple_autocommit_with_aligned_batch_size_commits() {
    // given
    val queryText =
      "USING AUTOCOMMIT 2 " +
      "CREATE () " +
      "CREATE () " +
      "WITH * MATCH (n) RETURN count(n) AS updates"

    // prepare
    executeScalar[Number](queryText)
    deleteAllEntities()

    // when
    val initialTxCounts = graph.txCounts
    val expectedUpdates = executeScalar[Number](queryText)

    // then
    expectedUpdates should be(2)

    // and then
    graph.txCounts-initialTxCounts should be(TxCounts(commits = 2))
  }

  @Test
  def should_support_simple_autocommit_with_unaligned_batch_size() {
    // given
    val queryText =
      "USING AUTOCOMMIT 3 " +
      "CREATE () " +
      "CREATE () " +
      "CREATE () " +
      "CREATE () " +
      "WITH * MATCH (n) RETURN count(n) AS updates"

    // prepare
    executeScalar[Number](queryText)
    deleteAllEntities()

    // when
    val initialTxCounts = graph.txCounts
    val expectedUpdates = executeScalar[Number](queryText)

    // then
    expectedUpdates should be(4)

    // and then
    graph.txCounts-initialTxCounts should be(TxCounts(commits = 2))
  }

  @Test
  def should_support_autocommit_with_aligned_batch_size() {
    /*
      10 x nodes         => 30 updates
      4  y nodes         => 12 updates
      40 r relationships => 40 updates
     */

    // given
    val queryText =
        "USING AUTOCOMMIT 41 " +
        "FOREACH (x IN range(0, 9) | " +
        "  MERGE (n:X {x: x}) " +
        "  FOREACH (y IN range(0, 3) | " +
        "    MERGE (m:Y {y: y}) " +
        "    MERGE (n:X)-[:R]->(m:Y)) " +
        ") " +
        "WITH * MATCH (:X)-[r:R]->(:Y) RETURN 30 + 12 + count(r) AS updates"

    // prepare
    executeScalar[Number](queryText)
    deleteAllEntities()

    // when
    val initialTxCounts = graph.txCounts
    val expectedUpdates = executeScalar[Number](queryText)

    // then
    expectedUpdates should be(82)

    // and then
    graph.txCounts-initialTxCounts should be(TxCounts(commits = 3))
  }

  @Test
  def should_support_autocommit_with_unaligned_batch_size() {
    /*
      10 x nodes         => 30 updates
      4  y nodes         => 12 updates
      40 r relationships => 40 updates
     */

    // given
    val queryText =
      "USING AUTOCOMMIT 20 " +
        "FOREACH (x IN range(0, 9) | " +
        "  MERGE (n:X {x: x}) " +
        "  FOREACH (y IN range(0, 3) | " +
        "    MERGE (m:Y {y: y}) " +
        "    MERGE (n:X)-[:R]->(m:Y)) " +
        ") " +
        "WITH * MATCH (:X)-[r:R]->(:Y) RETURN 30 + 12 + count(r) AS updates"

    // prepare
    executeScalar[Number](queryText)
    deleteAllEntities()

    // when
    val initialTxCounts = graph.txCounts
    val expectedUpdates = executeScalar[Number](queryText)

    // then
    expectedUpdates should be(82)

    // and then
    graph.txCounts-initialTxCounts should be(TxCounts(commits = 5))
  }

  @Test
  def should_abort_first_tx_when_failing_on_first_batch_during_autocommit() {
    // given
    val queryText = "USING AUTOCOMMIT 256 FOREACH (x IN range(0, 1023) | CREATE ({x: 1/0}))"

    // prepare
    intercept[ArithmeticException](executeScalar[Number](queryText))
    deleteAllEntities()

    // when
    val initialTxCounts = graph.txCounts
    intercept[ArithmeticException](executeScalar[Number](queryText))

    // then
    graph.txCounts-initialTxCounts should be(TxCounts(rollbacks = 1))
  }

  @Test
  def should_commit_first_tx_and_abort_second_tx_when_failing_on_second_batch_during_autocommit() {
    // given
    // creating 256 means 512 updates, indeed 1) create node and set the label
    val queryText = "USING AUTOCOMMIT 256 FOREACH (x IN range(0, 1023) | CREATE ({x: 1/(300-x)}))"

    // prepare
    intercept[ArithmeticException](executeScalar[Number](queryText))
    deleteAllEntities()

    // when
    val initialTxCounts = graph.txCounts
    intercept[ArithmeticException](executeScalar[Number](queryText))

    // then
    graph.txCounts-initialTxCounts should be(TxCounts(commits = 2, rollbacks = 1))
  }

  @Test
  def should_support_autocommit_hint_without_explicit_size() {
    executeScalar[Node]("USING AUTOCOMMIT CREATE (n) RETURN n")
  }

  @Test
  def should_support_autocommit_hint_with_explicit_size() {
    executeScalar[Node]("USING AUTOCOMMIT 400 CREATE (n) RETURN n")
  }

  @Test
  def should_reject_autocommit_hint_with_negative_size() {
    evaluating {
      executeScalar[Node]("USING AUTOCOMMIT -1 CREATE (n) RETURN n")
    } should produce[SyntaxException]
  }

  @Test
  def should_fail_if_autocommit_is_executed_in_an_open_transaction() {
    // given
    evaluating {
      graph.inTx {
        execute("USING AUTOCOMMIT CREATE ()")
      }
    } should produce[AutoCommitInOpenTransactionException]
  }
}
