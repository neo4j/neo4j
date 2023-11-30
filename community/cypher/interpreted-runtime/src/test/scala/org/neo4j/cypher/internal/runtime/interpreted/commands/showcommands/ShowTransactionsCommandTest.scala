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
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.TerminationMark
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.query.ExecutingQuery
import org.neo4j.kernel.api.query.QuerySnapshot
import org.neo4j.kernel.database.AbstractDatabase
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic
import org.neo4j.kernel.impl.api.TransactionRegistry
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.lock.ActiveLock
import org.neo4j.lock.LockType
import org.neo4j.lock.ResourceType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.StringValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.net.InetSocketAddress
import java.time.Duration
import java.util
import java.util.Collections

import scala.jdk.CollectionConverters.MapHasAsJava
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava

class ShowTransactionsCommandTest extends ShowCommandTestBase {

  private val defaultColumns =
    ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = false)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  private val allColumns =
    ShowTransactionsClause(Left(List.empty), None, List.empty, yieldAll = true)(InputPosition.NONE)
      .unfilteredColumns
      .columns

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Defaults:
    setupBaseSecurityContext()
    when(ctx.getConfig).thenReturn(Config.defaults())

    val dbCtxProvider = mock[DatabaseContextProvider[DatabaseContext]]
    when(ctx.getDatabaseContextProvider).thenReturn(dbCtxProvider)

    val databaseMap: util.NavigableMap[NamedDatabaseId, DatabaseContext] = new util.TreeMap()
    val userDbNamedId = DatabaseIdFactory.from(userDbName, util.UUID.randomUUID())
    setupDatabase(userDbName, userDbNamedId, userTxRegistry, databaseMap, started = true)
    setupDatabase(systemDbName, NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID, systemTxRegistry, databaseMap, started = true)
    when(dbCtxProvider.registeredDatabases).thenReturn(databaseMap)
  }

  private def setupDatabase(
    dbName: String,
    dbNamedId: NamedDatabaseId,
    txRegistry: TransactionRegistry,
    databaseMap: util.NavigableMap[NamedDatabaseId, DatabaseContext],
    started: Boolean
  ): Unit = {
    val dbContext = mock[DatabaseContext]
    setupDbDependencies(dbContext, txRegistry)
    databaseMap.put(dbNamedId, dbContext)

    val db = mock[AbstractDatabase]
    when(dbContext.database()).thenReturn(db)
    when(db.isStarted).thenReturn(started)
    when(db.getNamedDatabaseId).thenReturn(dbNamedId)

    val dbFacade = mock[GraphDatabaseAPI]
    when(dbContext.databaseFacade()).thenReturn(dbFacade)
    when(dbFacade.databaseName()).thenReturn(dbName)
  }

  private def setupTxHandles() = {
    val userAuthSubject = mock[AuthSubject]
    when(userAuthSubject.executingUser()).thenReturn(username)

    // Transaction 1

    val txHandle1 = mock[KernelTransactionHandle]
    when(txHandle1.subject()).thenReturn(userAuthSubject)
    when(txHandle1.getTransactionSequenceNumber).thenReturn(0L)
    when(txHandle1.getUserTransactionName).thenReturn(tx1)
    when(txHandle1.isClosing).thenReturn(false)
    when(txHandle1.transactionStatistic()).thenReturn(TransactionExecutionStatistic.NOT_AVAILABLE)
    when(txHandle1.clientInfo()).thenReturn(util.Optional.of(ClientConnectionInfo.EMBEDDED_CONNECTION))
    when(txHandle1.startTime()).thenReturn(500L)
    when(txHandle1.getMetaData).thenReturn(util.Collections.emptyMap())
    when(txHandle1.getStatusDetails).thenReturn("")
    when(txHandle1.activeLocks()).thenAnswer(_ => util.Collections.emptyList())
    when(txHandle1.transactionInitialisationTrace()).thenReturn(new TransactionInitializationTrace())
    when(txHandle1.terminationMark).thenReturn(
      util.Optional.of(new TerminationMark(Status.Transaction.TransactionTimedOut, 600L))
    )
    when(txHandle1.executingQuery()).thenReturn(util.Optional.empty())

    // Transaction 2

    val txHandle2 = mock[KernelTransactionHandle]
    when(txHandle2.subject()).thenReturn(AuthSubject.AUTH_DISABLED)
    when(txHandle2.getTransactionSequenceNumber).thenReturn(1L)
    when(txHandle2.getUserTransactionName).thenReturn(tx2)
    when(txHandle2.isClosing).thenReturn(false)
    val txStatistics2 = mock[TransactionExecutionStatistic]
    when(txHandle2.transactionStatistic()).thenReturn(txStatistics2)
    when(txStatistics2.getElapsedTimeMillis).thenReturn(1000L)
    when(txStatistics2.getCpuTimeMillis).thenReturn(90L)
    when(txStatistics2.getWaitTimeMillis).thenReturn(3L)
    when(txStatistics2.getIdleTimeMillis).thenReturn(6L)
    when(txStatistics2.getNativeAllocatedBytes).thenReturn(55L)
    when(txStatistics2.getEstimatedUsedHeapMemory).thenReturn(10L)
    when(txStatistics2.getPageHits).thenReturn(7L)
    when(txStatistics2.getPageFaults).thenReturn(2L)

    when(txHandle2.clientInfo()).thenReturn(util.Optional.empty())
    when(txHandle2.startTime()).thenReturn(1500L)
    when(txHandle2.getMetaData).thenReturn(util.Collections.emptyMap())
    when(txHandle2.getStatusDetails).thenReturn("")
    when(txHandle2.activeLocks()).thenAnswer(_ =>
      List[ActiveLock](new ActiveLock(ResourceType.LABEL, LockType.SHARED, 1L, 0L)).asJava
    )
    when(txHandle2.transactionInitialisationTrace()).thenReturn(TransactionInitializationTrace.NONE)
    when(txHandle2.terminationMark).thenReturn(util.Optional.empty())

    val query2 = mock[ExecutingQuery]
    when(txHandle2.executingQuery()).thenReturn(util.Optional.of(query2))
    val querySnapshot2 = mock[QuerySnapshot]
    when(query2.snapshot()).thenReturn(querySnapshot2)
    when(querySnapshot2.internalQueryId).thenReturn(2L)
    when(querySnapshot2.obfuscatedQueryText).thenReturn(
      util.Optional.of("MATCH (n:IndexedLabel {indexedProperty:3}) RETURN n")
    )
    when(querySnapshot2.resourceInformation()).thenReturn(util.Collections.emptyMap())
    when(querySnapshot2.transactionId()).thenReturn(2L)
    when(querySnapshot2.parentTransactionId()).thenReturn(-1L)
    when(querySnapshot2.obfuscatedQueryParameters()).thenReturn(util.Optional.empty())
    when(querySnapshot2.planner()).thenReturn("COST")
    when(querySnapshot2.runtime()).thenReturn("PIPELINED")
    when(querySnapshot2.indexes()).thenReturn(List(Map(
      "identifier" -> "n",
      "label" -> "IndexedLabel",
      "indexType" -> "SCHEMA INDEX",
      "propertyKey" -> "indexedProperty",
      "entityType" -> "NODE",
      "labelId" -> "0"
    ).asJava).asJava)
    when(querySnapshot2.startTimestampMillis()).thenReturn(1502L)
    when(querySnapshot2.status()).thenReturn("running")
    when(querySnapshot2.activeLockCount()).thenReturn(1L)
    when(querySnapshot2.waitingLocks()).thenReturn(List[ActiveLock](
      new ActiveLock(ResourceType.INDEX_ENTRY, LockType.EXCLUSIVE, 0L, 1L)
    ).asJava)
    when(querySnapshot2.elapsedTimeMicros()).thenReturn(90L)
    when(querySnapshot2.cpuTimeMicros()).thenReturn(util.OptionalLong.of(83L))
    when(querySnapshot2.waitTimeMicros()).thenReturn(2L)
    when(querySnapshot2.idleTimeMicros()).thenReturn(util.OptionalLong.of(5L))
    when(querySnapshot2.allocatedBytes()).thenReturn(50L)
    when(querySnapshot2.pageHits()).thenReturn(5L)
    when(querySnapshot2.pageFaults()).thenReturn(1L)

    // Transaction 3

    val txHandle3 = mock[KernelTransactionHandle]
    when(txHandle3.subject()).thenReturn(userAuthSubject)
    when(txHandle3.getTransactionSequenceNumber).thenReturn(0L)
    when(txHandle3.getUserTransactionName).thenReturn(tx3)
    when(txHandle3.isClosing).thenReturn(false)
    val txStatistics3 = mock[TransactionExecutionStatistic]
    when(txHandle3.transactionStatistic()).thenReturn(txStatistics3)
    when(txStatistics3.getElapsedTimeMillis).thenReturn(300L)
    when(txStatistics3.getCpuTimeMillis).thenReturn(0L)
    when(txStatistics3.getWaitTimeMillis).thenReturn(0L)
    when(txStatistics3.getIdleTimeMillis).thenReturn(0L)
    when(txStatistics3.getNativeAllocatedBytes).thenReturn(0L)
    when(txStatistics3.getEstimatedUsedHeapMemory).thenReturn(0L)
    when(txStatistics3.getPageHits).thenReturn(0L)
    when(txStatistics3.getPageFaults).thenReturn(0L)

    when(txHandle3.clientInfo()).thenReturn(util.Optional.of(new BoltConnectionInfo(
      "testConnection",
      "test",
      new InetSocketAddress("127.0.0.1", 56789),
      new InetSocketAddress("127.0.0.1", 7687),
      Collections.emptyMap()
    )))
    when(txHandle3.startTime()).thenReturn(42L)
    when(txHandle3.getMetaData).thenReturn(Map[String, AnyRef]("key" -> "value").asJava)
    when(txHandle3.getStatusDetails).thenReturn("I'm a status detail string")
    when(txHandle3.activeLocks()).thenAnswer(_ =>
      List[ActiveLock](
        new ActiveLock(ResourceType.INDEX_ENTRY, LockType.EXCLUSIVE, 0L, 1L),
        new ActiveLock(ResourceType.NODE, LockType.SHARED, 0L, 2L)
      ).asJava
    )
    when(txHandle3.transactionInitialisationTrace()).thenReturn(TransactionInitializationTrace.NONE)
    when(txHandle3.terminationMark).thenReturn(util.Optional.empty())

    val query3 = mock[ExecutingQuery]
    when(txHandle3.executingQuery()).thenReturn(util.Optional.of(query3))
    val querySnapshot3 = mock[QuerySnapshot]
    when(query3.snapshot()).thenReturn(querySnapshot3)
    when(querySnapshot3.internalQueryId).thenReturn(3L)
    when(querySnapshot3.obfuscatedQueryText).thenReturn(util.Optional.of("CREATE ROLE $name"))
    when(querySnapshot3.resourceInformation()).thenReturn(Map[String, AnyRef]("key" -> "value").asJava)
    when(querySnapshot3.transactionId()).thenReturn(0L)
    when(querySnapshot3.obfuscatedQueryParameters()).thenReturn(
      util.Optional.of(VirtualValues.map(Array("name"), Array(Values.stringValue("Foo"))))
    )
    when(querySnapshot3.planner()).thenReturn("ADMINISTRATION")
    when(querySnapshot3.runtime()).thenReturn("SYSTEM")
    when(querySnapshot3.indexes()).thenReturn(util.Collections.emptyList())
    when(querySnapshot3.startTimestampMillis()).thenReturn(42L)
    when(querySnapshot3.status()).thenReturn("running")
    when(querySnapshot3.activeLockCount()).thenReturn(2L)
    when(querySnapshot3.elapsedTimeMicros()).thenReturn(2000L)
    when(querySnapshot3.cpuTimeMicros()).thenReturn(util.OptionalLong.empty())
    when(querySnapshot3.waitTimeMicros()).thenReturn(0L)
    when(querySnapshot3.idleTimeMicros()).thenReturn(util.OptionalLong.empty())
    when(querySnapshot3.allocatedBytes()).thenReturn(0L)
    when(querySnapshot3.pageHits()).thenReturn(0L)
    when(querySnapshot3.pageFaults()).thenReturn(0L)

    (txHandle1, txHandle2, txHandle3)
  }

  // Only checks the given parameters
  private def checkResult(
    resultMap: Map[String, AnyValue],
    txId: Option[String],
    database: Option[String] = None,
    queryId: Option[String] = None,
    connectionId: Option[String] = None,
    clientAddress: Option[String] = None,
    username: Option[String] = None,
    currentQuery: Option[String] = None,
    startTime: Option[String] = None,
    status: Option[String] = None,
    elapsedTime: Option[AnyValue] = None,
    outerTxId: Option[String] = None,
    metaData: Option[AnyValue] = None,
    params: Option[AnyValue] = None,
    planner: Option[String] = None,
    runtime: Option[String] = None,
    indexes: Option[List[AnyValue]] = None,
    queryStartTime: Option[String] = None,
    protocol: Option[String] = None,
    requestUri: Option[String] = None,
    queryStatus: Option[String] = None,
    statusDetails: Option[String] = None,
    resourceInfo: Option[AnyValue] = None,
    activeLockCount: Option[Long] = None,
    queryActiveLockCount: Option[AnyValue] = None,
    cpuTime: Option[AnyValue] = None,
    waitTime: Option[AnyValue] = None,
    idleTime: Option[AnyValue] = None,
    queryElapsedTime: Option[AnyValue] = None,
    queryCpuTime: Option[AnyValue] = None,
    queryWaitTime: Option[AnyValue] = None,
    queryIdleTime: Option[AnyValue] = None,
    queryAllocatedBytes: Option[AnyValue] = None,
    allocatedDirectBytes: Option[AnyValue] = None,
    estimatedUsedHeapMemory: Option[AnyValue] = None,
    pageHits: Option[Long] = None,
    pageFaults: Option[Long] = None,
    queryPageHits: Option[AnyValue] = None,
    queryPageFaults: Option[AnyValue] = None,
    startOfInitStackTrace: Option[String] = None
  ): Unit = {
    withClue("transactionId:") {
      txId.foreach(expected =>
        resultMap(ShowTransactionsClause.transactionIdColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("database:") {
      database.foreach(expected =>
        resultMap(ShowTransactionsClause.databaseColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("currentQueryId:") {
      queryId.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryIdColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("connectionId:") {
      connectionId.foreach(expected =>
        resultMap(ShowTransactionsClause.connectionIdColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("clientAddress:") {
      clientAddress.foreach(expected =>
        resultMap(ShowTransactionsClause.clientAddressColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("username:") {
      username.foreach(expected =>
        resultMap(ShowTransactionsClause.usernameColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("currentQuery:") {
      currentQuery.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("startTime:") {
      startTime.foreach(expected =>
        resultMap(ShowTransactionsClause.startTimeColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("status:") {
      status.foreach(expected => resultMap(ShowTransactionsClause.statusColumn) should be(Values.stringValue(expected)))
    }
    withClue("elapsedTime:") {
      elapsedTime.foreach(expected => resultMap(ShowTransactionsClause.elapsedTimeColumn) should be(expected))
    }
    withClue("outerTransactionId:") {
      outerTxId.foreach(expected =>
        resultMap(ShowTransactionsClause.outerTransactionIdColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("metaData:") {
      metaData.foreach(expected => resultMap(ShowTransactionsClause.metaDataColumn) should be(expected))
    }
    withClue("parameters:") {
      params.foreach(expected => resultMap(ShowTransactionsClause.parametersColumn) should be(expected))
    }
    withClue("planner:") {
      planner.foreach(expected =>
        resultMap(ShowTransactionsClause.plannerColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("runtime:") {
      runtime.foreach(expected =>
        resultMap(ShowTransactionsClause.runtimeColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("indexes:") {
      indexes.foreach(expected =>
        resultMap(ShowTransactionsClause.indexesColumn) should be(VirtualValues.list(expected: _*))
      )
    }
    withClue("currentQueryStartTime:") {
      queryStartTime.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryStartTimeColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("protocol:") {
      protocol.foreach(expected =>
        resultMap(ShowTransactionsClause.protocolColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("requestUri:") {
      requestUri.foreach(expected =>
        if (expected == null) resultMap(ShowTransactionsClause.requestUriColumn) should be(Values.NO_VALUE)
        else resultMap(ShowTransactionsClause.requestUriColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("currentQueryStatus:") {
      queryStatus.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryStatusColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("statusDetails:") {
      statusDetails.foreach(expected =>
        resultMap(ShowTransactionsClause.statusDetailsColumn) should be(Values.stringValue(expected))
      )
    }
    withClue("resourceInformation:") {
      resourceInfo.foreach(expected => resultMap(ShowTransactionsClause.resourceInformationColumn) should be(expected))
    }
    withClue("activeLockCount:") {
      activeLockCount.foreach(expected =>
        resultMap(ShowTransactionsClause.activeLockCountColumn) should be(Values.longValue(expected))
      )
    }
    withClue("currentQueryActiveLockCount:") {
      queryActiveLockCount.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryActiveLockCountColumn) should be(expected)
      )
    }
    withClue("cpuTime:") {
      cpuTime.foreach(expected => resultMap(ShowTransactionsClause.cpuTimeColumn) should be(expected))
    }
    withClue("waitTime:") {
      waitTime.foreach(expected => resultMap(ShowTransactionsClause.waitTimeColumn) should be(expected))
    }
    withClue("idleTime:") {
      idleTime.foreach(expected => resultMap(ShowTransactionsClause.idleTimeColumn) should be(expected))
    }
    withClue("currentQueryElapsedTime:") {
      queryElapsedTime.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryElapsedTimeColumn) should be(expected)
      )
    }
    withClue("currentQueryCpuTime:") {
      queryCpuTime.foreach(expected => resultMap(ShowTransactionsClause.currentQueryCpuTimeColumn) should be(expected))
    }
    withClue("currentQueryWaitTime:") {
      queryWaitTime.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryWaitTimeColumn) should be(expected)
      )
    }
    withClue("currentQueryIdleTime:") {
      queryIdleTime.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryIdleTimeColumn) should be(expected)
      )
    }
    withClue("currentQueryAllocatedBytes:") {
      queryAllocatedBytes.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryAllocatedBytesColumn) should be(expected)
      )
    }
    withClue("allocatedDirectBytes:") {
      allocatedDirectBytes.foreach(expected =>
        resultMap(ShowTransactionsClause.allocatedDirectBytesColumn) should be(expected)
      )
    }
    withClue("estimatedUsedHeapMemory:") {
      estimatedUsedHeapMemory.foreach(expected =>
        resultMap(ShowTransactionsClause.estimatedUsedHeapMemoryColumn) should be(expected)
      )
    }
    withClue("pageHits:") {
      pageHits.foreach(expected =>
        resultMap(ShowTransactionsClause.pageHitsColumn) should be(Values.longValue(expected))
      )
    }
    withClue("pageFaults:") {
      pageFaults.foreach(expected =>
        resultMap(ShowTransactionsClause.pageFaultsColumn) should be(Values.longValue(expected))
      )
    }
    withClue("currentQueryPageHits:") {
      queryPageHits.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryPageHitsColumn) should be(expected)
      )
    }
    withClue("currentQueryPageFaults:") {
      queryPageFaults.foreach(expected =>
        resultMap(ShowTransactionsClause.currentQueryPageFaultsColumn) should be(expected)
      )
    }
    withClue("initializationStackTrace:") {
      startOfInitStackTrace.foreach(expected => {
        if (expected.isEmpty) {
          // If Some("") -> check it is the empty string
          resultMap(ShowTransactionsClause.initializationStackTraceColumn) should be(Values.stringValue(""))
        } else {
          // If Some("string value") -> check that stacktrace starts with given value
          // this to avoid it breaking due to line number changes in stacktrace
          resultMap(ShowTransactionsClause.initializationStackTraceColumn)
            .asInstanceOf[StringValue].stringValue() should startWith(expected.get)
        }
      })
    }
  }

  // Tests

  test("show transactions should give back correct default values") {
    // Set-up which transactions are executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When
    val showTx = ShowTransactionsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 3
    val sortedResult = result.sortBy(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    checkResult(
      sortedResult.head,
      txId = tx1,
      database = userDbName,
      queryId = "",
      connectionId = "",
      clientAddress = "",
      username = username,
      currentQuery = "",
      startTime = "1970-01-01T00:00:00.5Z",
      status = "Terminated with reason: Status.Code[Neo.ClientError.Transaction.TransactionTimedOut]",
      elapsedTime = Values.durationValue(Duration.ofMillis(-1L))
    )
    checkResult(
      sortedResult(1),
      txId = tx2,
      database = userDbName,
      queryId = "query-2",
      connectionId = "",
      clientAddress = "",
      username = AuthSubject.AUTH_DISABLED.executingUser(),
      currentQuery = "MATCH (n:IndexedLabel {indexedProperty:3}) RETURN n",
      startTime = "1970-01-01T00:00:01.5Z",
      status = s"Blocked by: [$tx3]",
      elapsedTime = Values.durationValue(Duration.ofMillis(1000L))
    )
    checkResult(
      sortedResult(2),
      txId = tx3,
      database = systemDbName,
      queryId = "query-3",
      connectionId = "testConnection",
      clientAddress = "127.0.0.1:56789",
      username = username,
      currentQuery = "CREATE ROLE $name",
      startTime = "1970-01-01T00:00:00.042Z",
      status = "Running",
      elapsedTime = Values.durationValue(Duration.ofMillis(300L))
    )
    // confirm no verbose columns:
    result.foreach(res => {
      res.keys.toList should contain noElementsOf List(
        ShowTransactionsClause.outerTransactionIdColumn,
        ShowTransactionsClause.metaDataColumn,
        ShowTransactionsClause.parametersColumn,
        ShowTransactionsClause.plannerColumn,
        ShowTransactionsClause.runtimeColumn,
        ShowTransactionsClause.indexesColumn,
        ShowTransactionsClause.currentQueryStartTimeColumn,
        ShowTransactionsClause.protocolColumn,
        ShowTransactionsClause.requestUriColumn,
        ShowTransactionsClause.currentQueryStatusColumn,
        ShowTransactionsClause.statusDetailsColumn,
        ShowTransactionsClause.resourceInformationColumn,
        ShowTransactionsClause.activeLockCountColumn,
        ShowTransactionsClause.currentQueryActiveLockCountColumn,
        ShowTransactionsClause.cpuTimeColumn,
        ShowTransactionsClause.waitTimeColumn,
        ShowTransactionsClause.idleTimeColumn,
        ShowTransactionsClause.currentQueryElapsedTimeColumn,
        ShowTransactionsClause.currentQueryCpuTimeColumn,
        ShowTransactionsClause.currentQueryWaitTimeColumn,
        ShowTransactionsClause.currentQueryIdleTimeColumn,
        ShowTransactionsClause.currentQueryAllocatedBytesColumn,
        ShowTransactionsClause.allocatedDirectBytesColumn,
        ShowTransactionsClause.estimatedUsedHeapMemoryColumn,
        ShowTransactionsClause.pageHitsColumn,
        ShowTransactionsClause.pageFaultsColumn,
        ShowTransactionsClause.currentQueryPageHitsColumn,
        ShowTransactionsClause.currentQueryPageFaultsColumn,
        ShowTransactionsClause.initializationStackTraceColumn
      )
    })
  }

  test("show transactions should give back correct full values") {
    // Set-up which transactions are executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When
    val showTx = ShowTransactionsCommand(Left(List.empty), allColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 3
    val sortedResult = result.sortBy(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    checkResult(
      sortedResult.head,
      txId = tx1,
      database = userDbName,
      queryId = "",
      connectionId = "",
      clientAddress = "",
      username = username,
      currentQuery = "",
      startTime = "1970-01-01T00:00:00.5Z",
      status = "Terminated with reason: Status.Code[Neo.ClientError.Transaction.TransactionTimedOut]",
      elapsedTime = Values.durationValue(Duration.ofMillis(-1L)),
      outerTxId = "",
      metaData = VirtualValues.EMPTY_MAP,
      params = VirtualValues.EMPTY_MAP,
      planner = "",
      runtime = "",
      indexes = List.empty[AnyValue],
      queryStartTime = "",
      protocol = "embedded",
      requestUri = Some(null),
      queryStatus = "",
      statusDetails = "",
      resourceInfo = VirtualValues.EMPTY_MAP,
      activeLockCount = 0L,
      queryActiveLockCount = Values.NO_VALUE,
      cpuTime = Values.NO_VALUE,
      waitTime = Values.durationValue(Duration.ofMillis(-1L)),
      idleTime = Values.NO_VALUE,
      queryElapsedTime = Values.NO_VALUE,
      queryCpuTime = Values.NO_VALUE,
      queryWaitTime = Values.NO_VALUE,
      queryIdleTime = Values.NO_VALUE,
      queryAllocatedBytes = Values.NO_VALUE,
      allocatedDirectBytes = Values.NO_VALUE,
      estimatedUsedHeapMemory = Values.NO_VALUE,
      pageHits = 0L,
      pageFaults = 0L,
      queryPageHits = Values.NO_VALUE,
      queryPageFaults = Values.NO_VALUE,
      startOfInitStackTrace = "java.lang.Throwable: Transaction initialization stacktrace."
    )
    checkResult(
      sortedResult(1),
      txId = tx2,
      database = userDbName,
      queryId = "query-2",
      connectionId = "",
      clientAddress = "",
      username = AuthSubject.AUTH_DISABLED.executingUser(),
      currentQuery = "MATCH (n:IndexedLabel {indexedProperty:3}) RETURN n",
      startTime = "1970-01-01T00:00:01.5Z",
      status = s"Blocked by: [$tx3]",
      elapsedTime = Values.durationValue(Duration.ofMillis(1000L)),
      outerTxId = s"$userDbName-transaction-2",
      metaData = VirtualValues.EMPTY_MAP,
      params = VirtualValues.EMPTY_MAP,
      planner = "COST",
      runtime = "PIPELINED",
      indexes = List[AnyValue](VirtualValues.map(
        Array("indexType", "entityType", "identifier", "labelId", "label", "propertyKey"),
        Array(
          Values.stringValue("SCHEMA INDEX"),
          Values.stringValue("NODE"),
          Values.stringValue("n"),
          Values.stringValue("0"),
          Values.stringValue("IndexedLabel"),
          Values.stringValue("indexedProperty")
        )
      )),
      queryStartTime = "1970-01-01T00:00:01.502Z",
      protocol = "",
      requestUri = Some(null),
      queryStatus = "running",
      statusDetails = "",
      resourceInfo = VirtualValues.EMPTY_MAP,
      activeLockCount = 1L,
      queryActiveLockCount = Values.longValue(1L),
      cpuTime = Values.durationValue(Duration.ofMillis(90L)),
      waitTime = Values.durationValue(Duration.ofMillis(3L)),
      idleTime = Values.durationValue(Duration.ofMillis(6L)),
      queryElapsedTime = Values.durationValue(Duration.ofMillis(0L)),
      queryCpuTime = Values.durationValue(Duration.ofMillis(0L)),
      queryWaitTime = Values.durationValue(Duration.ofMillis(0L)),
      queryIdleTime = Values.durationValue(Duration.ofMillis(0L)),
      queryAllocatedBytes = Values.longValue(50L),
      allocatedDirectBytes = Values.longValue(55L),
      estimatedUsedHeapMemory = Values.longValue(10L),
      pageHits = 7L,
      pageFaults = 2L,
      queryPageHits = Values.longValue(5L),
      queryPageFaults = Values.longValue(1L),
      startOfInitStackTrace = ""
    )
    checkResult(
      sortedResult(2),
      txId = tx3,
      database = systemDbName,
      queryId = "query-3",
      connectionId = "testConnection",
      clientAddress = "127.0.0.1:56789",
      username = username,
      currentQuery = "CREATE ROLE $name",
      startTime = "1970-01-01T00:00:00.042Z",
      status = "Running",
      elapsedTime = Values.durationValue(Duration.ofMillis(300L)),
      outerTxId = "",
      metaData = VirtualValues.map(Array("key"), Array(Values.stringValue("value"))),
      params = VirtualValues.map(Array("name"), Array(Values.stringValue("Foo"))),
      planner = "ADMINISTRATION",
      runtime = "SYSTEM",
      indexes = List.empty[AnyValue],
      queryStartTime = "1970-01-01T00:00:00.042Z",
      protocol = "bolt",
      requestUri = "127.0.0.1:7687",
      queryStatus = "running",
      statusDetails = "I'm a status detail string",
      resourceInfo = VirtualValues.map(Array("key"), Array(Values.stringValue("value"))),
      activeLockCount = 2L,
      queryActiveLockCount = Values.longValue(2L),
      cpuTime = Values.durationValue(Duration.ofMillis(0L)),
      waitTime = Values.durationValue(Duration.ofMillis(0L)),
      idleTime = Values.durationValue(Duration.ofMillis(0L)),
      queryElapsedTime = Values.durationValue(Duration.ofMillis(2L)),
      queryCpuTime = Values.NO_VALUE,
      queryWaitTime = Values.durationValue(Duration.ofMillis(0L)),
      queryIdleTime = Values.NO_VALUE,
      queryAllocatedBytes = Values.longValue(0L),
      allocatedDirectBytes = Values.longValue(0L),
      estimatedUsedHeapMemory = Values.longValue(0L),
      pageHits = 0L,
      pageFaults = 0L,
      queryPageHits = Values.longValue(0L),
      queryPageFaults = Values.longValue(0L),
      startOfInitStackTrace = ""
    )
  }

  test("show transactions should only return given transactions") {
    // Set-up which transactions are executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When
    val showTx = ShowTransactionsCommand(Left(List(tx1)), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx1
    )
  }

  test("show transactions should not return the transactions sorted (but grouped by database) - show all") {
    // Set-up which transactions is executing (not ordered on txId):
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle2, txHandle1).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When: given transactions not ordered by id
    val showTx = ShowTransactionsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then: will collect the transactions by database
    // for this test sorted on database id, but that's not guaranteed code-wise
    // as we just get values from a map and doesn't sort them
    result should have size 3
    val txIds = result.map(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    txIds should (be(List(tx3, tx2, tx1)) or be(List(tx3, tx1, tx2)))
  }

  test("show transactions should not return the transactions sorted (but grouped by database) - show listed") {
    // Set-up which transactions are executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // When: given transactions not ordered by id
    val showTx = ShowTransactionsCommand(Left(List(tx2, tx3, tx1)), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then: will collect the transactions by database
    // for this test sorted on database id, but that's not guaranteed code-wise
    // as we just get values from a map and doesn't sort them
    result should have size 3
    val txIds = result.map(m => m("transactionId").asInstanceOf[StringValue].stringValue())
    txIds should (be(List(tx3, tx2, tx1)) or be(List(tx3, tx1, tx2)))
  }

  test("show transactions should return nothing for non-existing transaction") {
    // Set-up no executing transactions:
    when(userTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // When
    val showTx =
      ShowTransactionsCommand(Left(List("unknown-transaction-1")), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 0
  }

  test("show transactions should give back correct result for already closed transaction") {
    // Set-up which transactions are executing:
    val (txHandle1, _, _) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // Given
    when(txHandle1.isClosing).thenReturn(true)

    // When
    val showTx = ShowTransactionsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx1,
      status = "Terminated with reason: Status.Code[Neo.ClientError.Transaction.TransactionTimedOut]"
    )
  }

  test("show transactions should return nothing for stopped databases") {
    // Set-up which transactions are executing
    // (on the stopped database just to show they will not be returned):
    val (txHandle1, txHandle2, _) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)

    // Given: stopped database
    val dbCtxProvider = mock[DatabaseContextProvider[DatabaseContext]]
    when(ctx.getDatabaseContextProvider).thenReturn(dbCtxProvider)

    val databaseMap: util.NavigableMap[NamedDatabaseId, DatabaseContext] = new util.TreeMap()
    val userDbNamedId = DatabaseIdFactory.from(userDbName, util.UUID.randomUUID())
    setupDatabase(userDbName, userDbNamedId, userTxRegistry, databaseMap, started = false)
    when(dbCtxProvider.registeredDatabases).thenReturn(databaseMap)

    // When
    val showTx = ShowTransactionsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 0
  }

  test("show transactions should only show allowed transactions") {
    // Set-up which transactions are executing:
    val (txHandle1, txHandle2, txHandle3) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1, txHandle2).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set(txHandle3).asJava)

    // Given
    when(securityContext.allowsAdminAction(any())).thenAnswer(_.getArgument[AdminActionOnResource](0) match {
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(
          SHOW_TRANSACTION,
          new AdminActionOnResource.DatabaseScope(userDbName),
          new UserSegment(username)
        )) =>
        PermissionState.NOT_GRANTED
      case a: AdminActionOnResource
        if a.matches(new AdminActionOnResource(
          SHOW_TRANSACTION,
          new AdminActionOnResource.DatabaseScope(systemDbName),
          new UserSegment(username)
        )) =>
        PermissionState.EXPLICIT_DENY
      case _ => PermissionState.EXPLICIT_GRANT
    })

    // When
    val showTx = ShowTransactionsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx2
    )
  }

  test("show transactions should always show users own transactions") {
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
    val showTx = ShowTransactionsCommand(Left(List.empty), defaultColumns, List.empty)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    checkResult(
      result.head,
      txId = tx1
    )
  }

  test("show transactions should rename columns renamed in YIELD") {
    // Set-up which transactions are executing:
    val (txHandle1, _, _) = setupTxHandles()
    when(userTxRegistry.executingTransactions).thenReturn(Set(txHandle1).asJava)
    when(systemTxRegistry.executingTransactions).thenReturn(Set.empty[KernelTransactionHandle].asJava)

    // Given: YIELD transactionId AS txId, username AS user, currentQuery, status
    val yieldColumns: List[CommandResultItem] = List(
      CommandResultItem(
        ShowTransactionsClause.transactionIdColumn,
        Variable("txId")(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowTransactionsClause.usernameColumn,
        Variable("user")(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowTransactionsClause.currentQueryColumn,
        Variable(ShowTransactionsClause.currentQueryColumn)(InputPosition.NONE)
      )(InputPosition.NONE),
      CommandResultItem(
        ShowTransactionsClause.statusColumn,
        Variable(ShowTransactionsClause.statusColumn)(InputPosition.NONE)
      )(InputPosition.NONE)
    )

    // When
    val showTx = ShowTransactionsCommand(Left(List.empty), allColumns, yieldColumns)
    val result = showTx.originalNameRows(queryState, initialCypherRow).toList

    // Then
    result should have size 1
    result should be(List(Map(
      "txId" -> Values.stringValue(tx1),
      "user" -> Values.stringValue(username),
      ShowTransactionsClause.currentQueryColumn -> Values.stringValue(""),
      ShowTransactionsClause.statusColumn -> Values.stringValue(
        "Terminated with reason: Status.Code[Neo.ClientError.Transaction.TransactionTimedOut]"
      )
    )))
  }
}
