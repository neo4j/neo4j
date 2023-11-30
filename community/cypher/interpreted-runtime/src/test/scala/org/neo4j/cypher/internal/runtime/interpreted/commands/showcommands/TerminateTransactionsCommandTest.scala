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
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ListLiteral
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_TRANSACTION
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.DatabaseIdRepository
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.logging.InternalLog
import org.neo4j.logging.InternalLogProvider
import org.neo4j.logging.NullLogProvider
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Values

import java.util.Optional
import java.util.UUID

import scala.jdk.CollectionConverters.SetHasAsJava

class TerminateTransactionsCommandTest extends ShowCommandTestBase {

  // Terminate transaction currently have no non-default columns
  private val columns =
    TerminateTransactionsClause(Left(List.empty), List.empty, yieldAll = false, None)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Defaults:
    setupBaseSecurityContext()
    when(securityContext.impersonating).thenReturn(false)

    when(ctx.logProvider).thenReturn(NullLogProvider.getInstance())

    val dbCtxProvider = mock[DatabaseContextProvider[DatabaseContext]]
    val dbRegistry = mock[DatabaseIdRepository]
    val userDbContext = mock[DatabaseContext]
    val systemDbContext = mock[DatabaseContext]

    setupDbDependencies(userDbContext, userTxRegistry)
    setupDbDependencies(systemDbContext, systemTxRegistry)

    when(ctx.getDatabaseContextProvider).thenReturn(dbCtxProvider)
    when(dbCtxProvider.databaseIdRepository).thenReturn(dbRegistry)
    when(dbCtxProvider.getDatabaseContext(any(): NamedDatabaseId)).thenAnswer(invocation =>
      invocation.getArgument[NamedDatabaseId](0) match {
        case n if n != null && n.name().equalsIgnoreCase(userDbName)   => Optional.of(userDbContext)
        case n if n != null && n.name().equalsIgnoreCase(systemDbName) => Optional.of(systemDbContext)
        case _                                                         => Optional.empty()
      }
    )

    val userDbNamedId = DatabaseIdFactory.from(userDbName, UUID.randomUUID())
    when(dbRegistry.getByName(any(): NormalizedDatabaseName)).thenAnswer(invocation =>
      invocation.getArgument[NormalizedDatabaseName](0) match {
        case n if n != null && n.name().equalsIgnoreCase(userDbName) => Optional.of(userDbNamedId)
        case n if n != null && n.name().equalsIgnoreCase(systemDbName) =>
          Optional.of(NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID)
        case _ => Optional.empty()
      }
    )
  }

  private def setupTxHandles() = {
    val userAuthSubject = mock[AuthSubject]
    when(userAuthSubject.executingUser()).thenReturn(username)

    val txHandle1 = mock[KernelTransactionHandle]
    when(txHandle1.subject()).thenReturn(userAuthSubject)
    when(txHandle1.getTransactionSequenceNumber).thenReturn(0L)
    when(txHandle1.isClosing).thenReturn(false)

    val txHandle2 = mock[KernelTransactionHandle]
    when(txHandle2.subject()).thenReturn(AuthSubject.AUTH_DISABLED)
    when(txHandle2.getTransactionSequenceNumber).thenReturn(1L)
    when(txHandle2.isClosing).thenReturn(false)

    val txHandle3 = mock[KernelTransactionHandle]
    when(txHandle3.subject()).thenReturn(userAuthSubject)
    when(txHandle3.getTransactionSequenceNumber).thenReturn(0L)
    when(txHandle3.isClosing).thenReturn(false)

    (txHandle1, txHandle2, txHandle3)
  }

  // Only checks the given parameters
  private def checkResult(
    resultMap: Map[String, AnyValue],
    txId: Option[String],
    message: Option[String],
    username: Option[String] = None
  ): Unit = {
    txId.foreach(expected =>
      resultMap(TerminateTransactionsClause.transactionIdColumn) should be(Values.stringValue(expected))
    )
    username.foreach(maybeExpected => {
      val expected = if (maybeExpected == null) Values.NO_VALUE else Values.stringValue(maybeExpected)
      resultMap(TerminateTransactionsClause.usernameColumn) should be(expected)
    })
    message.foreach(expected =>
      resultMap(TerminateTransactionsClause.messageColumn) should be(Values.stringValue(expected))
    )
  }

  // Tests

  test("terminate transaction should give back correct values") {
    // Set-up which transactions is executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1, tx2, tx3)), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 3
    val sortedResult = result.sortBy(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    checkResult(
      sortedResult.head,
      txId = tx1,
      username = username,
      message = "Transaction terminated."
    )
    checkResult(
      sortedResult(1),
      txId = tx2,
      username = AuthSubject.AUTH_DISABLED.executingUser(),
      message = "Transaction terminated."
    )
    checkResult(
      sortedResult(2),
      txId = tx3,
      username = username,
      message = "Transaction terminated."
    )

    // Verify markedForTermination calls
    verify(txHandle1, times(1)).markForTermination(Status.Transaction.Terminated)
    verify(txHandle2, times(1)).markForTermination(Status.Transaction.Terminated)
    verify(txHandle3, times(1)).markForTermination(Status.Transaction.Terminated)
  }

  test("terminate transaction should not return the transactions sorted (but grouped by database)") {
    // Set-up which transactions is executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When: given transactions not ordered by id
    val terminateTx = TerminateTransactionsCommand(Left(List(tx2, tx3, tx1)), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then: will collect the transactions by database
    result should have size 3
    val txIds = result.map(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    txIds should be(List(tx2, tx1, tx3))
  }

  test("terminate transaction should get exception on empty transaction list") {
    // Set-up no executing transactions:
    when(userTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // Given
    val emptyList = Left(List.empty)
    val emptyExpression = Right(ListLiteral())

    // Then
    the[InvalidSemanticsException] thrownBy {
      TerminateTransactionsCommand(emptyList, columns, List.empty).originalNameRows(queryState, initialCypherRow)
    } should have message
      "Missing transaction id to terminate, the transaction id can be found using `SHOW TRANSACTIONS`."

    // Then
    the[InvalidSemanticsException] thrownBy {
      TerminateTransactionsCommand(emptyExpression, columns, List.empty).originalNameRows(queryState, initialCypherRow)
    } should have message
      "Missing transaction id to terminate, the transaction id can be found using `SHOW TRANSACTIONS`."
  }

  test("terminate transaction should give back correct result for non-existing transaction") {
    // Set-up no executing transactions:
    when(userTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List("unknown-transaction-1")), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = "unknown-transaction-1",
      username = Some(null),
      message = "Transaction not found."
    )
  }

  test("terminate transaction should give back correct result for already closed transaction") {
    // Set-up which transactions is executing:
    val (txHandle1, _, _) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // Given
    when(txHandle1.isClosing).thenReturn(true)

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1)), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx1,
      username = username,
      message = "Unable to terminate closing transactions."
    )

    // Verify markedForTermination calls
    verify(txHandle1, times(0)).markForTermination(any())
  }

  test("terminate transaction should only terminate given transactions") {
    // Set-up which transactions is executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1)), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx1,
      message = "Transaction terminated."
    )

    // Verify markedForTermination calls
    verify(txHandle1, times(1)).markForTermination(Status.Transaction.Terminated)
    verify(txHandle2, times(0)).markForTermination(any())
    verify(txHandle3, times(0)).markForTermination(any())
  }

  test("terminate transaction should only terminate allowed transactions") {
    // Set-up which transactions is executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // Given
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(
          TERMINATE_TRANSACTION,
          new AdminActionOnResource.DatabaseScope(userDbName),
          new UserSegment(username)
        )) =>
        PermissionState.NOT_GRANTED
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(
          TERMINATE_TRANSACTION,
          new AdminActionOnResource.DatabaseScope(systemDbName),
          new UserSegment(username)
        )) =>
        PermissionState.EXPLICIT_DENY
      case _ => PermissionState.EXPLICIT_GRANT
    })

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1, tx2, tx3)), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 3
    val sortedResult = result.sortBy(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    checkResult(
      sortedResult.head,
      txId = tx1,
      message = "Transaction not found."
    )
    checkResult(
      sortedResult(1),
      txId = tx2,
      message = "Transaction terminated."
    )
    checkResult(
      sortedResult(2),
      txId = tx3,
      message = "Transaction not found."
    )

    // Verify markedForTermination calls
    verify(txHandle1, times(0)).markForTermination(any())
    verify(txHandle2, times(1)).markForTermination(Status.Transaction.Terminated)
    verify(txHandle3, times(0)).markForTermination(any())
  }

  test("terminate transaction should always terminate users own transactions") {
    // Set-up which transactions is executing:
    val (txHandle1, _, _) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // Given
    val userAuthSubject = mock[AuthSubject]
    when(userAuthSubject.hasUsername(username)).thenReturn(true)
    when(securityContext.subject()).thenReturn(userAuthSubject)

    when(securityContext.allowsAdminAction(any())).thenReturn(PermissionState.EXPLICIT_DENY)

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1)), columns, List.empty)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx1,
      message = "Transaction terminated."
    )

    // Verify markedForTermination calls
    verify(txHandle1, times(1)).markForTermination(Status.Transaction.Terminated)
  }

  test("terminate transaction should log attempting to terminate and successfully terminated") {
    // Set-up which transactions is executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // Given
    val user = "terminatingUser"
    val userAuthSubject = mock[AuthSubject]
    when(userAuthSubject.executingUser()).thenReturn(user)
    when(securityContext.subject()).thenReturn(userAuthSubject)
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(
          TERMINATE_TRANSACTION,
          new AdminActionOnResource.DatabaseScope(systemDbName),
          new UserSegment(username)
        )) =>
        PermissionState.EXPLICIT_DENY
      case _ => PermissionState.EXPLICIT_GRANT
    })

    val logProvider = mock[InternalLogProvider]
    val log = mock[InternalLog]
    when(ctx.logProvider).thenReturn(logProvider)
    when(logProvider.getLog(any(): Class[_])).thenReturn(log)

    var logLines: List[String] = List.empty
    when(log.info(any(): String, any(): Any, any(): Any)).thenAnswer(invocation => {
      val logLine = String.format(
        invocation.getArgument[String](0),
        invocation.getArgument[Any](1),
        invocation.getArgument[Any](2)
      )

      logLines = logLines :+ logLine
    })

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1, tx3)), columns, List.empty)
    terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    logLines should be(List(
      s"User $user trying to terminate transactions: [$tx1, $tx3].",
      s"User $user terminated transaction $tx1."
    ))
  }

  test("terminate transaction should log attempting to terminate and successfully terminated when impersonating") {
    // Set-up which transactions is executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // Given
    val user = "terminatingUser"
    val impersonator = "impersonatingUser"
    val userAuthSubject = mock[AuthSubject]
    when(userAuthSubject.executingUser()).thenReturn(user)
    when(userAuthSubject.authenticatedUser()).thenReturn(impersonator)
    when(securityContext.subject()).thenReturn(userAuthSubject)
    when(securityContext.impersonating).thenReturn(true)
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(
          TERMINATE_TRANSACTION,
          new AdminActionOnResource.DatabaseScope(systemDbName),
          new UserSegment(username)
        )) =>
        PermissionState.EXPLICIT_DENY
      case _ => PermissionState.EXPLICIT_GRANT
    })

    val logProvider = mock[InternalLogProvider]
    val log = mock[InternalLog]
    when(ctx.logProvider).thenReturn(logProvider)
    when(logProvider.getLog(any(): Class[_])).thenReturn(log)

    var logLines: List[String] = List.empty
    when(log.info(any(): String, any(): Any, any(): Any)).thenAnswer(invocation => {
      val logLine = String.format(
        invocation.getArgument[String](0),
        invocation.getArgument[Any](1),
        invocation.getArgument[Any](2)
      )

      logLines = logLines :+ logLine
    })

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx3, tx2)), columns, List.empty)
    terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    logLines should be(List(
      s"User $impersonator:$user trying to terminate transactions: [$tx3, $tx2].",
      s"User $impersonator:$user terminated transaction $tx2."
    ))
  }

  test("terminate transaction should rename columns renamed in YIELD") {
    // Set-up which transactions is executing:
    val (txHandle1, _, _) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // Given: YIELD transactionId AS txId, username
    val yieldColumns: List[CommandResultItem] = List(
      CommandResultItem(
        TerminateTransactionsClause.transactionIdColumn,
        Variable("txId")(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        TerminateTransactionsClause.usernameColumn,
        Variable(TerminateTransactionsClause.usernameColumn)(InputPosition.NONE)
      )(InputPosition.NONE)
    )

    // When
    val terminateTx = TerminateTransactionsCommand(Left(List(tx1)), columns, yieldColumns)
    val result = terminateTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    result should be(List(Map(
      "txId" -> Values.stringValue(tx1),
      TerminateTransactionsClause.usernameColumn -> Values.stringValue(username)
    )))
  }

}
