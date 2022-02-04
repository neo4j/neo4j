/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME
import org.neo4j.configuration.GraphDatabaseSettings.auth_enabled
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.exceptions.SyntaxException
import org.neo4j.graphdb.Result
import org.neo4j.graphdb.TransientTransactionFailureException
import org.neo4j.graphdb.config.Setting
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.kernel.api.KernelTransaction.Type
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.server.security.auth.SecurityTestUtils
import org.neo4j.test.DoubleLatch
import org.neo4j.test.NamedFunction
import org.neo4j.test.extension.Threading
import org.neo4j.values.storable.DurationValue
import org.scalatest.concurrent.Eventually.eventually

import java.util

class CommunityTransactionCommandAcceptanceTest extends ExecutionEngineFunSuite with GraphDatabaseTestSupport {
  private val username = "foo"
  private val username2 = "bar"
  private val password = "secret"

  private val threading: Threading = new Threading()

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    threading.before()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    threading.after()
  }

  override def databaseConfig(): Map[Setting[_], Object] = super.databaseConfig() ++ Map(auth_enabled -> java.lang.Boolean.TRUE)

  // SHOW TRANSACTIONS (don't test exact id as it might change)

  test("Should show current transaction") {
    eventually {
      // WHEN
      val result = execute("SHOW TRANSACTIONS").toList

      // THEN
      result should have size 1
      assertCorrectDefaultMap(result.head, "neo4j-transaction-", "", "SHOW TRANSACTIONS")
    }
  }

  test("Should show all transactions") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    val res = execute("SHOW TRANSACTIONS").toList
    val result = res.filterNot(m => m("database").asInstanceOf[String].equals(SYSTEM_DATABASE_NAME)) // remove random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectDefaultMap(sortedRes.head, "neo4j-transaction-", username, unwindQuery)
    assertCorrectDefaultMap(sortedRes(1), "neo4j-transaction-", "", "SHOW TRANSACTIONS")
  }

  test("Should show system transactions") {
    // GIVEN
    createUser()
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch, SYSTEM_DATABASE_NAME)
    tx.execute(username, password, threading, "SHOW DATABASES")
    latch.startAndWaitForAllToStart()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    val res = execute("SHOW TRANSACTIONS").toList
    val result = res.filter(m => !m("database").asInstanceOf[String].equals(SYSTEM_DATABASE_NAME) || m("username").asInstanceOf[String].equals(username)) // remove possible random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectDefaultMap(sortedRes.head, "neo4j-transaction-", "", "SHOW TRANSACTIONS")
    assertCorrectDefaultMap(sortedRes(1), "system-transaction-", username, "SHOW DATABASES", database = SYSTEM_DATABASE_NAME)
  }

  test("Should only show given transactions") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val result = execute(s"SHOW TRANSACTIONS '$unwindId'").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    assertCorrectDefaultMap(result.head, unwindId, username, unwindQuery)
  }

  test("Should only show given transactions once") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)
    val result = execute(s"SHOW TRANSACTIONS '$user2Id', '$user1Id', '$user2Id'").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("username").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectDefaultMap(sortedRes.head, user2Id, username2, user2Query)
    assertCorrectDefaultMap(sortedRes(1), user1Id, username, user1Query)
  }

  test("Should return nothing when showing non-existing transaction") {
    // WHEN
    val result = execute("SHOW TRANSACTIONS 'noDb-transaction-123'").toList

    // THEN
    result should be(empty)
  }

  test("Should show current transaction on system database") {
    eventually {
      // WHEN
      selectDatabase(SYSTEM_DATABASE_NAME)
      val result = execute("SHOW TRANSACTIONS").toList

      // THEN
      result should have size 1
      assertCorrectDefaultMap(result.head, "system-transaction-", "", "SHOW TRANSACTIONS", database = SYSTEM_DATABASE_NAME)
    }
  }

  test("Should show all transactions when executing on system database") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    selectDatabase(SYSTEM_DATABASE_NAME)
    val res = execute("SHOW TRANSACTIONS").toList
    val result = res.filter(m => !m("database").asInstanceOf[String].equals(SYSTEM_DATABASE_NAME) || m("username").asInstanceOf[String].equals("")) // remove possible random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectDefaultMap(sortedRes.head, "neo4j-transaction-", username, unwindQuery)
    assertCorrectDefaultMap(sortedRes(1), "system-transaction-", "", "SHOW TRANSACTIONS", database = SYSTEM_DATABASE_NAME)
  }

  test("Should only show given transactions when executing on system database") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)
    selectDatabase(SYSTEM_DATABASE_NAME)
    val result = execute(s"SHOW TRANSACTIONS '$user2Id', '$user1Id'").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("username").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectDefaultMap(sortedRes.head, user2Id, username2, user2Query)
    assertCorrectDefaultMap(sortedRes(1), user1Id, username, user1Query)
  }

  test("Should show given transactions with string parameter") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val result = execute("SHOW TRANSACTIONS $id", Map("id" -> unwindId)).toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    assertCorrectDefaultMap(result.head, unwindId, username, unwindQuery)
  }

  test("Should show given transactions with list parameter") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val result = execute("SHOW TRANSACTIONS $id", Map("id" -> List(unwindId))).toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    assertCorrectDefaultMap(result.head, unwindId, username, unwindQuery)
  }

  test("Should always show all transactions in community") {
    // GIVEN
    val showUser = "baz"
    createUser(showUser)
    val latch = new DoubleLatch(4)
    val (user1Query, user2Query1) = setupTwoUsersAndOneTransactionEach(latch)
    val user2Query2 = "SHOW DATABASES"
    val tx = ThreadedTransaction(latch, SYSTEM_DATABASE_NAME)
    tx.execute(username2, password, threading, user2Query2)
    latch.startAndWaitForAllToStart()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    val res = executeAs(showUser, password, "SHOW TRANSACTIONS").toList
    val result = res.filter(m => !m("database").asInstanceOf[String].equals(SYSTEM_DATABASE_NAME) || m("username").asInstanceOf[String].equals(username2)) // remove possible random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 4
    val sortedRes = result.sortBy(m => m("currentQuery").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectDefaultMap(sortedRes.head, "neo4j-transaction-", username2, user2Query1)
    assertCorrectDefaultMap(sortedRes(1), "system-transaction-", username2, user2Query2, database = SYSTEM_DATABASE_NAME)
    assertCorrectDefaultMap(sortedRes(2), "neo4j-transaction-", showUser, "SHOW TRANSACTIONS")
    assertCorrectDefaultMap(sortedRes(3), "neo4j-transaction-", username, user1Query)
  }

  // yield/where/return tests

  test("Should show transactions with WHERE") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (_, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val result = execute(s"SHOW TRANSACTIONS WHERE username = '$username2'").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    assertCorrectDefaultMap(result.head, "neo4j-transaction-", username2, user2Query)
  }

  test("Should show given transactions with WHERE") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)
    val result = execute(s"SHOW TRANSACTIONS '$user2Id', '$user1Id' WHERE username = '$username2'").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    assertCorrectDefaultMap(result.head, "neo4j-transaction-", username2, user2Query)
  }

  test("Should show given transactions filtered with WHERE on transactionId") {
    // GIVEN
    val latch = new DoubleLatch(4)
    val (user1Query, user2Query1) = setupTwoUsersAndOneTransactionEach(latch)
    val user2Query2 = "SHOW DATABASES"
    val tx = ThreadedTransaction(latch, SYSTEM_DATABASE_NAME)
    tx.execute(username2, password, threading, user2Query2)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query1)
    val result = execute(s"SHOW TRANSACTIONS '$user1Id', '$user2Id' WHERE transactionId = '$user2Id' OR transactionId STARTS WITH 'system-transaction-'").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    assertCorrectDefaultMap(result.head, user2Id, username2, user2Query1)
  }

  test("should fail to show transactions with WHERE on verbose column") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("SHOW TRANSACTIONS WHERE isEmpty(parameters)")
    }

    // THEN
    exception.getMessage should startWith("Variable `parameters` not defined")
  }

  test("Should show current transaction with YIELD *") {
    eventually {
      // WHEN
      val result = execute("SHOW TRANSACTIONS YIELD *").toList

      // THEN
      result should have size 1
      assertCorrectFullMap(result.head, "neo4j-transaction-", "", "SHOW TRANSACTIONS YIELD *")
    }
  }

  test("Should show all transactions with YIELD *") {
    // GIVEN
    createUser()
    val userQuery = "SHOW DATABASES"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch, SYSTEM_DATABASE_NAME)
    tx.execute(username, password, threading, userQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    selectDatabase(DEFAULT_DATABASE_NAME)
    val res = execute("SHOW TRANSACTIONS YIELD *").toList
    val result = res.filter(m => !m("database").asInstanceOf[String].equals(SYSTEM_DATABASE_NAME) || m("username").asInstanceOf[String].equals(username)) // remove possible random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectFullMap(sortedRes.head, "neo4j-transaction-", "", "SHOW TRANSACTIONS YIELD *")
    assertCorrectFullMap(sortedRes(1), "system-transaction-", username, userQuery, database = SYSTEM_DATABASE_NAME, planner = "administration", runtime = "system")
  }

  test("Should show given transactions with YIELD *") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)
    val result = execute(s"SHOW TRANSACTIONS '$user2Id', '$user1Id', '$user1Id' YIELD *").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("username").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectFullMap(sortedRes.head, user2Id, username2, user2Query)
    assertCorrectFullMap(sortedRes(1), user1Id, username, user1Query)
  }

  test("Should show all transactions with specific YIELD") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val showIdNumber = unwindId.split("-")(2).toInt + 2
    val showId = s"neo4j-transaction-$showIdNumber"

    // WHEN
    val showQuery = "SHOW TRANSACTIONS YIELD transactionId, currentQuery, runtime"
    val res = execute(showQuery).toList
    val result = res.filterNot(m => m("transactionId").asInstanceOf[String].startsWith("system")) // remove random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    sortedRes should be(List(
      Map("transactionId" -> unwindId, "currentQuery" -> unwindQuery, "runtime" -> "interpreted"),
      Map("transactionId" -> showId, "currentQuery" -> showQuery, "runtime" -> "interpreted")
    ))
  }

  test("Should show given transactions with specific YIELD") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)
    val result = execute(s"SHOW TRANSACTIONS '$user2Id', '$user1Id', '$user1Id' YIELD transactionId, currentQuery, runtime").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    sortedRes should be(List(
      Map("transactionId" -> user1Id, "currentQuery" -> user1Query, "runtime" -> "interpreted"),
      Map("transactionId" -> user2Id, "currentQuery" -> user2Query, "runtime" -> "interpreted")
    ).sortBy(m => m("transactionId")))
  }

  test("Should show transactions with YIELD and ORDER BY ASC") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val showIdNumber = unwindId.split("-")(2).toInt + 2
    val showId = s"neo4j-transaction-$showIdNumber"

    // WHEN
    val res = execute("SHOW TRANSACTIONS YIELD transactionId, runtime ORDER BY transactionId ASC").toList
    val result = res.filterNot(m => m("transactionId").asInstanceOf[String].startsWith("system")) // remove random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should be(List(
      Map("transactionId" -> unwindId, "runtime" -> "interpreted"),
      Map("transactionId" -> showId, "runtime" -> "interpreted")
    ))
  }

  test("Should show transactions with YIELD and ORDER BY DESC") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val showIdNumber = unwindId.split("-")(2).toInt + 2
    val showId = s"neo4j-transaction-$showIdNumber"

    // WHEN
    val res = execute("SHOW TRANSACTIONS YIELD transactionId, runtime ORDER BY transactionId DESC").toList
    val result = res.filterNot(m => m("transactionId").asInstanceOf[String].startsWith("system")) // remove random system transactions from parallel tests/set-up
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should be(List(
      Map("transactionId" -> showId, "runtime" -> "interpreted"),
      Map("transactionId" -> unwindId, "runtime" -> "interpreted")
    ))
  }

  test("Should show transactions with YIELD * and WHERE") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()

    // WHEN
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)
    val result = execute("SHOW TRANSACTIONS YIELD * WHERE username <> ''").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 2
    val sortedRes = result.sortBy(m => m("username").asInstanceOf[String]) // To get stable order to assert correct result
    assertCorrectFullMap(sortedRes.head, user2Id, username2, user2Query)
    assertCorrectFullMap(sortedRes(1), user1Id, username, user1Query)
  }

  test("Should show transactions with specific YIELD and WHERE") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)

    // WHEN
    val result = execute("SHOW TRANSACTIONS YIELD transactionId, currentQuery, runtime, username WHERE runtime = 'interpreted' AND username <> ''").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    sortedRes should be(List(
      Map("transactionId" -> user1Id, "currentQuery" -> user1Query, "username" -> username, "runtime" -> "interpreted"),
      Map("transactionId" -> user2Id, "currentQuery" -> user2Query, "username" -> username2, "runtime" -> "interpreted")
    ).sortBy(m => m("transactionId")))
  }

  test("Should show transactions with YIELD, WHERE and RETURN") {
    // GIVEN
    val latch = new DoubleLatch(3)
    val (user1Query, user2Query) = setupTwoUsersAndOneTransactionEach(latch)
    latch.startAndWaitForAllToStart()
    val user1Id = getTransactionIdExecutingQuery(user1Query)
    val user2Id = getTransactionIdExecutingQuery(user2Query)

    // WHEN
    val result = execute("SHOW TRANSACTIONS YIELD transactionId, currentQuery, runtime, username WHERE runtime = 'interpreted' AND username <> '' RETURN transactionId, currentQuery").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
    sortedRes should be(List(
      Map("transactionId" -> user1Id, "currentQuery" -> user1Query),
      Map("transactionId" -> user2Id, "currentQuery" -> user2Query)
    ).sortBy(m => m("transactionId")))
  }

  test("Should show transactions with full yield") {
    // GIVEN
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val matchQuery = "MATCH (n) RETURN n"
    createUser()
    createUser(username2)
    val latch = new DoubleLatch(3)
    val tx1 = ThreadedTransaction(latch)
    tx1.execute(username, password, threading, unwindQuery)
    val tx2 = ThreadedTransaction(latch)
    tx2.execute(username2, password, threading, matchQuery)
    latch.startAndWaitForAllToStart()

    // WHEN
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val matchId = getTransactionIdExecutingQuery(matchQuery)
    val result = execute("SHOW TRANSACTIONS YIELD transactionId AS txId, runtime, username ORDER BY txId SKIP 1 LIMIT 5 WHERE runtime = 'interpreted' AND username <> '' RETURN txId").toList
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 1
    result.head("txId") should (be(unwindId) or be(matchId)) // we don't know which query will get skipped
  }

  test("Should show transactions with multiple ORDER BY") {
    def assertCorrectMap(resultMap: Map[String, AnyRef], transactionId: String, database: String) = {
      resultMap("transactionId").asInstanceOf[String] should startWith(transactionId)
      resultMap("database") should be(database)
    }

    // GIVEN
    createUser()
    createUser(username2)

    val latch = new DoubleLatch(3)
    val tx1 = ThreadedTransaction(latch)
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    tx1.execute(username, password, threading, unwindQuery)
    val tx2 = ThreadedTransaction(latch, SYSTEM_DATABASE_NAME)
    tx2.execute(username2, password, threading, "SHOW DATABASES")
    latch.startAndWaitForAllToStart()
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val showIdNumber = unwindId.split("-")(2).toInt + 2
    val showId = s"neo4j-transaction-$showIdNumber"

    // WHEN (WHERE to remove random system transactions from parallel tests/set-up)
    selectDatabase(DEFAULT_DATABASE_NAME)
    val result = execute(s"SHOW TRANSACTIONS YIELD * ORDER BY transactionId DESC WHERE database <> '$SYSTEM_DATABASE_NAME' OR username = '$username2' RETURN transactionId, database ORDER BY database ASC")
    val resultList = result.toList
    val planDescr = result.executionPlanDescription()
    latch.finishAndWaitForAllToFinish()

    // THEN
    result should have size 3
    planDescr should includeSomewhere.aPlan("Sort").containingArgument("transactionId DESC")
    planDescr should includeSomewhere.aPlan("Sort").containingArgument("database ASC")
    assertCorrectMap(resultList.head, showId, DEFAULT_DATABASE_NAME)
    assertCorrectMap(resultList(1), unwindId, DEFAULT_DATABASE_NAME)
    assertCorrectMap(resultList(2), "system-transaction-", SYSTEM_DATABASE_NAME)
  }

  test("Should show transactions with aggregation") {
    // GIVEN
    createUser()
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()
    val unwindId = getTransactionIdExecutingQuery(unwindQuery)
    val showIdNumber = unwindId.split("-")(2).toInt + 2
    val showId = s"neo4j-transaction-$showIdNumber"

    // WHEN (WHERE to remove random system transactions from parallel tests/set-up)
    val result = execute(s"SHOW TRANSACTIONS YIELD * ORDER BY transactionId WHERE database <> '$SYSTEM_DATABASE_NAME' RETURN collect(transactionId) AS txIds")
    latch.finishAndWaitForAllToFinish()

    // THEN
    result.toList should be(List(Map("txIds" -> List(unwindId, showId))))
  }

  test("Should show transactions with double aggregation") {
    // GIVEN
    createUser()
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, "UNWIND [1,2,3] AS x RETURN x")
    latch.startAndWaitForAllToStart()

    // WHEN: the query is rewritten to include WITH (splitting the aggregation)
    // (WHERE to remove random system transactions from parallel tests/set-up)
    val result = execute(s"SHOW TRANSACTIONS YIELD * ORDER BY transactionId WHERE database <> '$SYSTEM_DATABASE_NAME' RETURN size(collect(transactionId)) AS numTx")
    latch.finishAndWaitForAllToFinish()

    // THEN
    result.toList should be(List(Map("numTx" -> 2)))
  }

  test("Should show transactions with double aggregation when executing on system database") {
    // GIVEN
    createUser()
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, "UNWIND [1,2,3] AS x RETURN x")
    latch.startAndWaitForAllToStart()

    // WHEN: the query is rewritten to include WITH (splitting the aggregation)
    // (WHERE to remove random system transactions from parallel tests/set-up)
    selectDatabase(SYSTEM_DATABASE_NAME)
    val result = execute(s"SHOW TRANSACTIONS YIELD * ORDER BY transactionId WHERE database <> '$SYSTEM_DATABASE_NAME' OR username = '' RETURN size(collect(transactionId)) AS numTx")
    latch.finishAndWaitForAllToFinish()

    // THEN
    result.toList should be(List(Map("numTx" -> 2)))
  }

  // TERMINATE TRANSACTIONS

  test("Should terminate transaction") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2, true)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      val result = execute(s"TERMINATE TRANSACTION '$unwindTransactionId'").toList

      // THEN
      result should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should terminate system transaction") {
    def getTransactionId: String = {
      val result = execute(
        s"""SHOW TRANSACTIONS
           |WHERE database = '$SYSTEM_DATABASE_NAME' AND username = '$username'
           |""".stripMargin)

      if (result.isEmpty) throw new RuntimeException(s"No queries found for user '$username' on database '$SYSTEM_DATABASE_NAME'")

      result.columnAs[String]("transactionId").next
    }

    // GIVEN
    createUser()
    val latch = new DoubleLatch(2, true)
    val tx = ThreadedTransaction(latch, SYSTEM_DATABASE_NAME)
    tx.execute(username, password, threading, s"CREATE USER $username2 SET PASSWORD '$password'")
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      selectDatabase(DEFAULT_DATABASE_NAME)
      val systemTransactionId = getTransactionId
      val result = execute(s"TERMINATE TRANSACTION '$systemTransactionId'").toList

      // THEN
      result should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> systemTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should fail when terminating the current transaction") {
    // The first transaction gets id 'neo4j-transaction-3' so that should be the terminate command in this case

    // WHEN
    val exception = the[TransientTransactionFailureException] thrownBy {
      execute("TERMINATE TRANSACTIONS 'neo4j-transaction-3'").toList
    }

    // THEN
    exception.getMessage should startWith("Unable to complete transaction.: Explicitly terminated by the user.")
  }

  test("Should only terminate given transactions once") {
    // GIVEN
    createUser()
    createUser(username2)
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val matchQuery = "MATCH (n) RETURN n"
    val latch = new DoubleLatch(3)
    val tx1 = ThreadedTransaction(latch)
    tx1.execute(username, password, threading, unwindQuery)
    val tx2 = ThreadedTransaction(latch)
    tx2.execute(username2, password, threading, matchQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      val matchTransactionId = getTransactionIdExecutingQuery(matchQuery)
      val result = execute(s"TERMINATE TRANSACTIONS '$matchTransactionId', '$unwindTransactionId', '$matchTransactionId'").toList

      // THEN
      val sortedRes = result.sortBy(m => m("transactionId").asInstanceOf[String]) // To get stable order to assert correct result
      sortedRes should be(List(
        Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username),
        Map("message" -> "Transaction terminated.", "transactionId" -> matchTransactionId, "username" -> username2)
      ).sortBy(m => m("transactionId")))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Try to terminate already terminated transaction") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2, true)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      val resultFirstTerminate = execute(s"TERMINATE TRANSACTION '$unwindTransactionId'").toList
      resultFirstTerminate should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))

      // THEN (same behaviour as the procedure)
      val resultSecondTerminate = execute(s"TERMINATE TRANSACTION '$unwindTransactionId'").toList
      resultSecondTerminate should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Try to terminate non-existing transaction") {
    // WHEN
    val result = execute("TERMINATE TRANSACTION 'none-transaction-0'").toList

    // THEN
    result should be(List(Map("message" -> "Transaction not found.", "transactionId" -> "none-transaction-0", "username" -> null)))
  }

  test("Should fail to terminate transaction when missing id") {
    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute("TERMINATE TRANSACTION")
    }

    // THEN
    exception.getMessage should startWith("Missing transaction id to terminate, the transaction id can be found using `SHOW TRANSACTIONS`")
  }

  test("Should terminate transaction when executing on system database") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2, true)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      selectDatabase(SYSTEM_DATABASE_NAME)
      val result = execute(s"TERMINATE TRANSACTION '$unwindTransactionId'").toList

      // THEN
      result should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should terminate transactions with string parameter") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      val result = execute("TERMINATE TRANSACTION $id", Map("id" -> unwindTransactionId)).toList

      // THEN
      result should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should terminate transactions with list parameter") {
    // GIVEN
    createUser()
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      val result = execute("TERMINATE TRANSACTION $id", Map("id" -> List(unwindTransactionId))).toList

      // THEN
      result should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  test("Should always terminate transaction in community") {
    // GIVEN
    createUser()
    createUser(username2)
    val unwindQuery = "UNWIND [1,2,3] AS x RETURN x"
    val latch = new DoubleLatch(2, true)
    val tx = ThreadedTransaction(latch)
    tx.execute(username, password, threading, unwindQuery)
    latch.startAndWaitForAllToStart()

    try {
      // WHEN
      val unwindTransactionId = getTransactionIdExecutingQuery(unwindQuery)
      val result = executeAs(username2, password, s"TERMINATE TRANSACTION '$unwindTransactionId'").toList

      // THEN
      result should be(List(Map("message" -> "Transaction terminated.", "transactionId" -> unwindTransactionId, "username" -> username)))
    } finally {
      latch.finishAndWaitForAllToFinish()
    }
  }

  // Help methods

  private def assertCorrectDefaultMap(resultMap: Map[String, AnyRef],
                                      transactionId: String,
                                      username: String,
                                      query: String,
                                      database: String = DEFAULT_DATABASE_NAME) = {
    resultMap("database") should be(database)
    resultMap("transactionId").asInstanceOf[String] should startWith(transactionId) // not stable on system database, differs among things between running the test on its own and the whole class
    resultMap("currentQueryId").asInstanceOf[String] should startWith("query-") // not stable, differs among things between running the test on its own and the whole class
    resultMap("username") should be(username)
    resultMap("currentQuery") should be(query)
    // Default values:
    resultMap("status") should be("Running")
    resultMap("allocatedBytes") should be(0L)
    resultMap("connectionId") should be("")
    resultMap("clientAddress") should be("")
    // Don't check exact values:
    resultMap("startTime").isInstanceOf[String] should be(true) // This is a timestamp
    resultMap("elapsedTime").isInstanceOf[DurationValue] should be(true)
  }

  private def assertCorrectFullMap(resultMap: Map[String, AnyRef],
                                   transactionId: String,
                                   username: String,
                                   query: String,
                                   database: String = DEFAULT_DATABASE_NAME,
                                   planner: String = "idp",
                                   runtime: String = "interpreted") = {
    assertCorrectDefaultMap(resultMap, transactionId, username, query, database)
    resultMap("planner") should be(planner)
    resultMap("runtime") should be(runtime)
    // Default values:
    resultMap("outerTransactionId") should be("")
    resultMap("parameters") should be(Map())
    resultMap("indexes") should be(List())
    resultMap("protocol") should be("embedded")
    resultMap("metaData") should be(Map())
    resultMap("requestUri") should be("")
    resultMap("statusDetails") should be("")
    resultMap("resourceInformation") should be(Map())
    resultMap("allocatedDirectBytes") should be(0L)
    resultMap("initializationStackTrace") should be("")
    // Don't check exact values:
    resultMap("estimatedUsedHeapMemory").isInstanceOf[Long] should be(true)
    resultMap("activeLockCount").isInstanceOf[Long] should be(true)
    resultMap("pageHits").isInstanceOf[Long] should be(true)
    resultMap("pageFaults").isInstanceOf[Long] should be(true)
    resultMap("cpuTime").isInstanceOf[DurationValue] should be(true)
    resultMap("waitTime").isInstanceOf[DurationValue] should be(true)
    resultMap("idleTime").isInstanceOf[DurationValue] should be(true)
  }

  /* Sets up one query for _username_ on neo4j.
   * Sets up one query for _username2_ on neo4j.
   *
   * Returns the two queries.
   */
  private def setupTwoUsersAndOneTransactionEach(latch: DoubleLatch) = {
    createUser()
    createUser(username2)

    val user1Query = "UNWIND [1,2,3] AS x RETURN x"
    val tx1 = ThreadedTransaction(latch)
    tx1.execute(username, password, threading, user1Query)

    val user2Query = "MATCH (n) RETURN n"
    val tx2 = ThreadedTransaction(latch)
    tx2.execute(username2, password, threading, user2Query)

    (user1Query, user2Query)
  }

  private def getTransactionIdExecutingQuery(query: String): String = {
    val result = execute(
      s"""SHOW TRANSACTIONS
         |WHERE currentQuery = '$query'
         |""".stripMargin)

    if (result.isEmpty) throw new RuntimeException(s"Expected query not found: $query")

    result.columnAs[String]("transactionId").next // There should be exactly one match for the given query
  }

  private def login(username: String, password: String): LoginContext = {
    val authManager = graph.getDependencyResolver.resolveDependency(classOf[AuthManager])
    authManager.login(SecurityTestUtils.authToken(username, password), EMBEDDED_CONNECTION)
  }

  private def createUser(username: String = username, password: String = password): Unit = {
    selectDatabase(SYSTEM_DATABASE_NAME)
    execute(s"CREATE USER $username SET PASSWORD '$password' CHANGE NOT REQUIRED")
    selectDatabase(DEFAULT_DATABASE_NAME)
  }

  private def executeAs(username: String, password: String, queryText: String): RewindableExecutionResult = {
    val tx = graph.beginTransaction(Type.EXPLICIT, login(username, password))
    try {
      val result = execute(queryText, Map.empty[String, Any], tx)
      tx.commit()
      result
    } finally {
      tx.close()
    }
  }

  case class ThreadedTransaction(latch: DoubleLatch, database: String = DEFAULT_DATABASE_NAME) {
    private val graphService = new GraphDatabaseCypherService(managementService.database(database))

    def execute(username: String, password: String, threading: Threading, query: String): Unit = {
      val startTransaction = new NamedFunction[LoginContext, Throwable]("threaded-transaction-" + util.Arrays.hashCode(query.toCharArray)) {
        override def apply(subject: LoginContext): Throwable = try {
          val tx = graphService.beginTransaction(Type.EXPLICIT, subject)

          try {
            var result: Result = null
            try {
              result = tx.execute(query)
              latch.startAndWaitForAllToStart()
            } finally {
              latch.start()
              latch.finishAndWaitForAllToFinish()
            }
            if (result != null) {
              result.accept((_: Result.ResultRow) => true)
              result.close()
            }
            tx.commit()
            null
          } catch {
            case t: Throwable => t
          } finally {
            if (tx != null) tx.close()
          }
        }
      }
      val subject = login(username, password)
      threading.execute(startTransaction, subject)
    }
  }
}
