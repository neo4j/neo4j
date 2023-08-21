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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.neo4j.configuration.helpers.DatabaseNameValidator.MAXIMUM_DATABASE_NAME_LENGTH
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.impl.api.TransactionRegistry

import scala.jdk.CollectionConverters.SetHasAsJava

class TransactionCommandHelperTest extends ShowCommandTestBase {

  test("`isSelfOrAllows` should return true for users own transactions") {
    // Given
    val username = "me"
    val user = mock[AuthSubject]
    when(user.hasUsername(username)).thenReturn(true)
    when(securityContext.subject()).thenReturn(user)

    val action = mock[AdminActionOnResource]
    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.EXPLICIT_DENY)

    // When
    val result = TransactionCommandHelper.isSelfOrAllows(username, action, securityContext)

    // Then
    result should be(true)
  }

  test("`isSelfOrAllows` should return true for allowed transactions") {
    // Given
    val username = "other"
    val user = mock[AuthSubject]
    when(user.hasUsername(username)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    val action = mock[AdminActionOnResource]
    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.EXPLICIT_GRANT)

    // When
    val result = TransactionCommandHelper.isSelfOrAllows(username, action, securityContext)

    // Then
    result should be(true)
  }

  test("`isSelfOrAllows` should return false for not granted transactions") {
    // Given
    val username = "other"
    val user = mock[AuthSubject]
    when(user.hasUsername(username)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    val action = mock[AdminActionOnResource]
    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.NOT_GRANTED)

    // When
    val result = TransactionCommandHelper.isSelfOrAllows(username, action, securityContext)

    // Then
    result should be(false)
  }

  test("`isSelfOrAllows` should return false for denied transactions") {
    // Given
    val username = "other"
    val user = mock[AuthSubject]
    when(user.hasUsername(username)).thenReturn(false)
    when(securityContext.subject()).thenReturn(user)

    val action = mock[AdminActionOnResource]
    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.EXPLICIT_DENY)

    // When
    val result = TransactionCommandHelper.isSelfOrAllows(username, action, securityContext)

    // Then
    result should be(false)
  }

  test("`getExecutingTransactions` should return empty set when no dependencies are found") {
    // Given
    val dbContext = mock[DatabaseContext]
    when(dbContext.dependencies).thenReturn(null)

    // When
    val result = TransactionCommandHelper.getExecutingTransactions(dbContext)

    // Then
    result should have size 0
  }

  test("`getExecutingTransactions` should return executing transactions") {
    // Given
    val dbContext = mock[DatabaseContext]
    val txRegistry = mock[TransactionRegistry]
    setupDbDependencies(dbContext, txRegistry)

    val txHandle1 = mock[KernelTransactionHandle]
    val txHandle2 = mock[KernelTransactionHandle]
    when(txRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)

    // When
    val result = TransactionCommandHelper.getExecutingTransactions(dbContext)

    // Then
    result should be(Set(txHandle1, txHandle2))
  }

  // Test transaction ids

  private val expectedTxIdFormat = "(expected format: <databasename>-transaction-<id>)"

  test("prints transaction ids") {
    TransactionId("neo4j", 12L).toString shouldBe "neo4j-transaction-12"
  }

  test("prints transaction ids with normalized database name") {
    TransactionId("NEO4J", 12L).toString shouldBe "neo4j-transaction-12"
  }

  test("does not print negative transaction ids") {
    the[InvalidArgumentsException] thrownBy {
      TransactionId("neo4j", -15L)
    } should have message s"Negative ids are not supported $expectedTxIdFormat"
  }

  test("parses transaction ids") {
    TransactionId.parse("neo4j-transaction-14") shouldBe TransactionId("neo4j", 14L)
  }

  test("parses transaction ids with double separator") {
    TransactionId.parse("neo4j-transaction-transaction-14") shouldBe TransactionId("neo4j-transaction", 14L)
  }

  test("does not parse negative transaction ids") {
    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("neo4j-transaction--12")
    } should have message s"Negative ids are not supported $expectedTxIdFormat"
  }

  test("does not parse wrong separator") {
    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("neo4j-transactioo-12")
    } should have message s"Could not parse id $expectedTxIdFormat"

    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("neo4j-12")
    } should have message s"Could not parse id $expectedTxIdFormat"
  }

  test("does not parse random text") {
    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("blarglbarf")
    } should have message s"Could not parse id $expectedTxIdFormat"
  }

  test("does not parse trailing random text") {
    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("neo4j-transaction-12  ")
    } should have message s"Could not parse id $expectedTxIdFormat"
  }

  test("does not parse empty text") {
    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("")
    } should have message s"Could not parse id $expectedTxIdFormat"
  }

  test("validate and normalise database name when parsing") {
    TransactionId.parse("NEO4J-transaction-14") shouldBe TransactionId("neo4j", 14L)

    val e = the[InvalidArgumentsException] thrownBy TransactionId.parse(
      "a".repeat(MAXIMUM_DATABASE_NAME_LENGTH + 1) + "-transaction-14"
    )
    e.getMessage should include(" must have a length between ")

    the[InvalidArgumentsException] thrownBy {
      TransactionId.parse("-transaction-12")
    } should have message "The provided database name is empty."
  }

  // Test query ids

  test("prints query ids") {
    QueryId(12L).toString shouldBe "query-12"
  }

  test("does not print negative query ids") {
    the[InvalidArgumentsException] thrownBy {
      QueryId(-15L)
    } should have message "Negative ids are not supported (expected format: query-<id>)"
  }
}
