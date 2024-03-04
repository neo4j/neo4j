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
package org.neo4j.cypher

import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.runtime.CreateTempFileTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.testing.api.CypherExecutorException
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService.TestApiKind
import org.neo4j.dbms.api.DatabaseManagementService
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.test.TestDatabaseManagementServiceBuilder

import java.time.Duration

class TransactionalQueryErrorBoltAcceptanceTest extends TransactionalQueryErrorAcceptanceTestBase
    with FeatureDatabaseManagementService.TestUsingBolt

class TransactionalQueryErrorEmbeddedAcceptanceTest extends TransactionalQueryErrorAcceptanceTestBase
    with FeatureDatabaseManagementService.TestUsingEmbedded

class TransactionalQueryErrorHttpAcceptanceTest extends TransactionalQueryErrorAcceptanceTestBase
    with FeatureDatabaseManagementService.TestUsingHttp

abstract class TransactionalQueryErrorAcceptanceTestBase
    extends CypherFunSuite
    with FeatureDatabaseManagementService.TestBase
    with CreateTempFileTestSupport {

  // This is an absolute mess...

  test("disallows CALL IN TRANSACTIONS in explicit transaction") {
    def code = executeInExplicitTx("CALL { CREATE () } IN TRANSACTIONS")

    testApiKind match {

      case TestApiKind.Embedded =>
        the[CypherExecutorException]
          .thrownBy(code)
          .getMessage.should(include(
            "can only be executed in an implicit transaction, but tried to execute in an explicit transaction."
          ))

      case _ =>
        expectError(
          Status.Transaction.ForbiddenDueToTransactionType,
          "can only be executed in an implicit transaction, but tried to execute in an explicit transaction."
        )(code)
    }
  }

  test("allows EXPLAIN CALL IN TRANSACTIONS in explicit transaction") {
    expectNoError(executeInExplicitTx("EXPLAIN CALL { CREATE () } IN TRANSACTIONS"))
  }

  test("allows CALL IN TRANSACTIONS in implicit transaction") {
    testApiKind match {
      case _ =>
        expectNoError(executeInImplicitTx("CALL { CREATE () } IN TRANSACTIONS"))
    }

  }

  def executeInExplicitTx(statement: String): StatementResult = {
    val tx = dbms.begin()
    try {
      tx.execute(statement)
    } finally {
      tx.rollback()
    }
  }

  def executeInImplicitTx(statement: String): StatementResult =
    dbms.execute(statement, Map.empty, identity)

  def expectNoError(code: => Any): Unit =
    noException.shouldBe(thrownBy(code))

  def expectError(status: Status, messageSubstring: String)(code: => Any): Unit = {
    val ex = the[CypherExecutorException]
      .thrownBy(code)

    ex.status.shouldEqual(status)
    ex.getMessage.should(include(messageSubstring))
  }

  def expectExplicitTxError(code: => Any): Unit =
    expectError(
      Status.Statement.SemanticError,
      "can only be executed in an implicit transaction, but tried to execute in an explicit transaction."
    )(code)

  override def createBackingDbms(config: Config): DatabaseManagementService =
    new TestDatabaseManagementServiceBuilder().impermanent.setConfig(config).build()

  override def baseConfig: Config.Builder = super.baseConfig
    .set(GraphDatabaseSettings.transaction_timeout, Duration.ofMinutes(10))

}
