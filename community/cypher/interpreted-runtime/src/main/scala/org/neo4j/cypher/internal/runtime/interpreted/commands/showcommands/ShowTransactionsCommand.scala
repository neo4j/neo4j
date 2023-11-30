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

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.StringUtils.EMPTY
import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.activeLockCountColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.allocatedDirectBytesColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.clientAddressColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.connectionIdColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.cpuTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryActiveLockCountColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryAllocatedBytesColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryCpuTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryElapsedTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryIdColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryIdleTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryPageFaultsColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryPageHitsColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryStartTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryStatusColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.currentQueryWaitTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.databaseColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.elapsedTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.estimatedUsedHeapMemoryColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.idleTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.indexesColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.initializationStackTraceColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.metaDataColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.outerTransactionIdColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.pageFaultsColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.pageHitsColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.parametersColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.plannerColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.protocolColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.requestUriColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.resourceInformationColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.runtimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.startTimeColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.statusColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.statusDetailsColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.transactionIdColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.usernameColumn
import org.neo4j.cypher.internal.ast.ShowTransactionsClause.waitTimeColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.helpers.TransactionDependenciesResolver
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.query.QuerySnapshot
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.lang
import java.time.Duration
import java.time.ZoneId
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util
import java.util.Optional
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

// SHOW TRANSACTION[S] [transaction-id[,...]] [WHERE clause|YIELD clause]
case class ShowTransactionsCommand(
  givenIds: Either[List[String], Expression],
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem]
) extends Command(defaultColumns, yieldColumns) {

  private val needQueryColumns =
    requestedColumnsNames.contains(currentQueryColumn) ||
      requestedColumnsNames.contains(currentQueryIdColumn) ||
      requestedColumnsNames.contains(outerTransactionIdColumn) ||
      requestedColumnsNames.contains(parametersColumn) ||
      requestedColumnsNames.contains(plannerColumn) ||
      requestedColumnsNames.contains(runtimeColumn) ||
      requestedColumnsNames.contains(indexesColumn) ||
      requestedColumnsNames.contains(currentQueryStartTimeColumn) ||
      requestedColumnsNames.contains(currentQueryStatusColumn) ||
      requestedColumnsNames.contains(currentQueryActiveLockCountColumn) ||
      requestedColumnsNames.contains(currentQueryElapsedTimeColumn) ||
      requestedColumnsNames.contains(currentQueryCpuTimeColumn) ||
      requestedColumnsNames.contains(currentQueryWaitTimeColumn) ||
      requestedColumnsNames.contains(currentQueryIdleTimeColumn) ||
      requestedColumnsNames.contains(currentQueryAllocatedBytesColumn) ||
      requestedColumnsNames.contains(currentQueryPageHitsColumn) ||
      requestedColumnsNames.contains(currentQueryPageFaultsColumn)

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    val ids = Command.extractNames(givenIds, state, baseRow)
    val ctx = state.query
    val securityContext = ctx.transactionalContext.securityContext

    val allowedTransactions = ctx.getDatabaseContextProvider.registeredDatabases.values.asScala.toList
      .filter(_.database.isStarted)
      .flatMap(databaseContext => {
        val dbName = databaseContext.databaseFacade.databaseName
        val dbScope = new AdminActionOnResource.DatabaseScope(databaseContext.database.getNamedDatabaseId.name)
        val allTransactions = TransactionCommandHelper.getExecutingTransactions(databaseContext)
        allTransactions.filter(tx => {
          val username = tx.subject.executingUser()
          val action = new AdminActionOnResource(SHOW_TRANSACTION, dbScope, new UserSegment(username))
          TransactionCommandHelper.isSelfOrAllows(username, action, securityContext)
        }).map(tx => {
          val querySnapshot: util.Optional[QuerySnapshot] = tx.executingQuery.map(_.snapshot())
          (tx, querySnapshot, dbName)
        })
      })

    val askedForTransactions =
      if (ids.nonEmpty) allowedTransactions.filter {
        case (transaction: KernelTransactionHandle, _, dbName: String) =>
          val txId = TransactionId(dbName, transaction.getTransactionSequenceNumber).toString
          ids.contains(txId)
      }
      else allowedTransactions

    val handleQuerySnapshotsMap = new util.HashMap[KernelTransactionHandle, util.Optional[QuerySnapshot]]
    askedForTransactions.foreach {
      case (transaction: KernelTransactionHandle, querySnapshot: util.Optional[QuerySnapshot], _) =>
        handleQuerySnapshotsMap.put(transaction, querySnapshot)
    }
    val transactionDependenciesResolver = new TransactionDependenciesResolver(handleQuerySnapshotsMap)

    val zoneId = getConfiguredTimeZone(ctx)
    val rows = askedForTransactions.map {
      case (transaction: KernelTransactionHandle, querySnapshot: util.Optional[QuerySnapshot], dbName: String) =>
        // These don't really have a default/fallback and is used in multiple columns
        // so let's keep them as is regardless of if they are actually needed or not
        val txId = TransactionId(dbName, transaction.getTransactionSequenceNumber).toString
        val statistic = transaction.transactionStatistic
        val clientInfo = transaction.clientInfo

        val (
          currentQueryId,
          currentQuery,
          outerTransactionId,
          parameters,
          planner,
          runtime,
          indexes,
          queryStartTime,
          queryStatus,
          queryActiveLockCount,
          queryElapsedTime,
          queryCpuTime,
          queryWaitTime,
          queryIdleTime,
          queryAllocatedBytes,
          queryPageHits,
          queryPageFaults
        ) = getQueryColumns(querySnapshot, txId, dbName, zoneId)

        val (status, statusDetails) =
          if (requestedColumnsNames.contains(statusColumn) || requestedColumnsNames.contains(statusDetailsColumn)) {
            getStatus(transaction, transactionDependenciesResolver)
          } else ("", "")

        requestedColumnsNames.flatMap {
          // Name of the database the transaction belongs to
          case `databaseColumn` => Some(databaseColumn -> Values.stringValue(dbName))
          // The id of the transaction
          case `transactionIdColumn` => Some(transactionIdColumn -> Values.stringValue(txId))
          // The id of the currently executing query
          case `currentQueryIdColumn` => Some(currentQueryIdColumn -> Values.stringValue(currentQueryId))
          // The id of the connection the transaction belongs to
          case `connectionIdColumn` =>
            Some(connectionIdColumn -> Values.stringValue(clientInfo.map[String](_.connectionId).orElse(EMPTY)))
          // The client address
          case `clientAddressColumn` =>
            Some(clientAddressColumn -> Values.stringValue(clientInfo.map[String](_.clientAddress).orElse(EMPTY)))
          // The name of the user running the transaction
          case `usernameColumn` => Some(usernameColumn -> Values.stringValue(transaction.subject.executingUser()))
          // The currently executing query
          case `currentQueryColumn` => Some(currentQueryColumn -> Values.stringValue(currentQuery))
          // The start time of the transaction
          case `startTimeColumn` =>
            Some(startTimeColumn -> Values.stringValue(formatTimeString(transaction.startTime(), zoneId)))
          // The status of the transaction (terminated, blocked, closing or running)
          case `statusColumn` => Some(statusColumn -> Values.stringValue(status))
          // The time elapsed
          case `elapsedTimeColumn` =>
            Some(elapsedTimeColumn -> getDurationOrNullFromMillis(statistic.getElapsedTimeMillis))
          // Id of outer transaction if it exists
          case `outerTransactionIdColumn` => Some(outerTransactionIdColumn -> Values.stringValue(outerTransactionId))
          // Metadata for the transaction
          case `metaDataColumn` => Some(metaDataColumn -> getMapValue(transaction.getMetaData))
          // Parameters for the currently executing query
          case `parametersColumn` => Some(parametersColumn -> parameters)
          // Planner for the currently executing query
          case `plannerColumn` => Some(plannerColumn -> Values.stringValue(planner))
          // Runtime for the currently executing query
          case `runtimeColumn` => Some(runtimeColumn -> Values.stringValue(runtime))
          // Indexes used by the currently executing query
          case `indexesColumn` => Some(indexesColumn -> indexes)
          // The start time of the currently executing query
          case `currentQueryStartTimeColumn` =>
            Some(currentQueryStartTimeColumn -> Values.stringValue(queryStartTime))
          // Protocol for the transaction
          case `protocolColumn` =>
            Some(protocolColumn -> Values.stringValue(clientInfo.map[String](_.protocol).orElse(EMPTY)))
          // Request URI for the transaction
          case `requestUriColumn` =>
            Some(requestUriColumn -> Values.stringOrNoValue(clientInfo.map[String](_.requestURI).orElse(null)))
          // The status of the currently executing query (parsing, planning, planned, running, waiting)
          case `currentQueryStatusColumn` => Some(currentQueryStatusColumn -> Values.stringValue(queryStatus))
          // Any string a dedicated kernel API will write to track the transaction progress
          case `statusDetailsColumn` => Some(statusDetailsColumn -> Values.stringValue(statusDetails))
          // Resource information for the transaction
          case `resourceInformationColumn` => Some(resourceInformationColumn -> getMapValue(
              querySnapshot.map[util.Map[String, AnyRef]](_.resourceInformation()).orElse(util.Collections.emptyMap())
            ))
          // Number of active locks held by the transaction
          case `activeLockCountColumn` =>
            Some(activeLockCountColumn -> Values.longValue(transaction.activeLocks.size()))
          // Number of active locks held by the currently executing query
          case `currentQueryActiveLockCountColumn` => Some(currentQueryActiveLockCountColumn -> queryActiveLockCount)
          // The CPU time
          case `cpuTimeColumn` => Some(cpuTimeColumn -> getDurationOrNullFromMillis(statistic.getCpuTimeMillis))
          // The wait time
          case `waitTimeColumn` =>
            Some(waitTimeColumn -> Values.durationValue(Duration.ofMillis(statistic.getWaitTimeMillis)))
          // The idle time
          case `idleTimeColumn` => Some(idleTimeColumn -> getDurationOrNullFromMillis(statistic.getIdleTimeMillis))
          // The time elapsed of the currently executing query
          case `currentQueryElapsedTimeColumn` => Some(currentQueryElapsedTimeColumn -> queryElapsedTime)
          // The CPU time of the currently executing query
          case `currentQueryCpuTimeColumn` => Some(currentQueryCpuTimeColumn -> queryCpuTime)
          // The wait time of the currently executing query
          case `currentQueryWaitTimeColumn` => Some(currentQueryWaitTimeColumn -> queryWaitTime)
          // The idle time of the currently executing query
          case `currentQueryIdleTimeColumn` => Some(currentQueryIdleTimeColumn -> queryIdleTime)
          // The bytes allocated by the currently executing query
          case `currentQueryAllocatedBytesColumn` => Some(currentQueryAllocatedBytesColumn -> queryAllocatedBytes)
          // The direct bytes allocated by the transaction
          case `allocatedDirectBytesColumn` =>
            Some(allocatedDirectBytesColumn -> getLongOrNull(statistic.getNativeAllocatedBytes))
          // The estimation of heap memory used by the transaction
          case `estimatedUsedHeapMemoryColumn` =>
            Some(estimatedUsedHeapMemoryColumn -> getLongOrNull(statistic.getEstimatedUsedHeapMemory))
          // The page hits
          case `pageHitsColumn` => Some(pageHitsColumn -> Values.longValue(statistic.getPageHits))
          // The page faults
          case `pageFaultsColumn` => Some(pageFaultsColumn -> Values.longValue(statistic.getPageFaults))
          // The page hits of the currently executing query
          case `currentQueryPageHitsColumn` => Some(currentQueryPageHitsColumn -> queryPageHits)
          // The page faults of the currently executing query
          case `currentQueryPageFaultsColumn` => Some(currentQueryPageFaultsColumn -> queryPageFaults)
          // The initialization stacktrace
          case `initializationStackTraceColumn` =>
            Some(
              initializationStackTraceColumn -> Values.stringValue(transaction.transactionInitialisationTrace.getTrace)
            )
          case unknown =>
            // This match should cover all existing columns but we get scala warnings
            // on non-exhaustive match due to it being string values
            throw new IllegalStateException(s"Missing case for column: $unknown")
        }.toMap[String, AnyValue]
    }

    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows)
    ClosingIterator.apply(updatedRows.iterator)
  }

  private def getQueryColumns(querySnapshot: Optional[QuerySnapshot], txId: String, dbName: String, zoneId: ZoneId) =
    if (needQueryColumns && querySnapshot.isPresent) {
      val query = querySnapshot.get

      val currentQueryId =
        if (requestedColumnsNames.contains(currentQueryIdColumn)) QueryId(query.internalQueryId).toString
        else EMPTY
      val currentQuery =
        if (requestedColumnsNames.contains(currentQueryColumn)) query.obfuscatedQueryText.orElse(EMPTY)
        else EMPTY
      val outerTransactionId =
        if (requestedColumnsNames.contains(outerTransactionIdColumn)) {
          val parentDbName = query.parentDbName()
          val parentTransactionId = query.parentTransactionId()
          val querySessionDbName = if (parentDbName != null) parentDbName else dbName
          val querySessionTransactionId =
            if (parentTransactionId > -1L) parentTransactionId else query.transactionId()
          val querySessionTransaction = TransactionId(querySessionDbName, querySessionTransactionId).toString
          if (querySessionTransaction == txId) EMPTY else querySessionTransaction
        } else EMPTY
      val parameters =
        if (requestedColumnsNames.contains(parametersColumn))
          query.obfuscatedQueryParameters().orElse(MapValue.EMPTY)
        else MapValue.EMPTY
      val planner = if (requestedColumnsNames.contains(plannerColumn)) {
        val maybePlanner = query.planner
        if (maybePlanner == null) EMPTY else maybePlanner
      } else EMPTY
      val runtime = if (requestedColumnsNames.contains(runtimeColumn)) {
        val maybeRuntime = query.runtime
        if (maybeRuntime == null) EMPTY else maybeRuntime
      } else EMPTY
      val indexes = if (requestedColumnsNames.contains(indexesColumn))
        VirtualValues.list(query.indexes.asScala.toList.map(m => {
          val scalaMap = m.asScala
          val keys = scalaMap.keys.toArray
          val vals: Array[AnyValue] = scalaMap.values.map(Values.stringValue).toArray
          VirtualValues.map(keys, vals)
        }): _*)
      else VirtualValues.EMPTY_LIST
      val queryStartTime = if (requestedColumnsNames.contains(currentQueryStartTimeColumn))
        formatTimeString(query.startTimestampMillis, zoneId)
      else EMPTY
      val queryStatus = if (requestedColumnsNames.contains(currentQueryStatusColumn)) query.status else EMPTY
      val queryActiveLockCount = if (requestedColumnsNames.contains(currentQueryActiveLockCountColumn))
        Values.longValue(query.activeLockCount)
      else Values.NO_VALUE
      val queryElapsedTime = if (requestedColumnsNames.contains(currentQueryElapsedTimeColumn))
        getDurationOrNullFromMicro(query.elapsedTimeMicros)
      else Values.NO_VALUE
      val queryCpuTime = if (requestedColumnsNames.contains(currentQueryCpuTimeColumn)) {
        val optionalCpuTime = query.cpuTimeMicros
        if (optionalCpuTime.isPresent) getDurationOrNullFromMicro(optionalCpuTime.getAsLong)
        else Values.NO_VALUE
      } else Values.NO_VALUE
      val queryWaitTime = if (requestedColumnsNames.contains(currentQueryWaitTimeColumn))
        Values.durationValue(Duration.ofMillis(TimeUnit.MICROSECONDS.toMillis(query.waitTimeMicros)))
      else Values.NO_VALUE
      val queryIdleTime = if (requestedColumnsNames.contains(currentQueryIdleTimeColumn)) {
        val optionalIdleTime = query.idleTimeMicros
        if (optionalIdleTime.isPresent) getDurationOrNullFromMicro(optionalIdleTime.getAsLong)
        else Values.NO_VALUE
      } else Values.NO_VALUE
      val queryAllocatedBytes = if (requestedColumnsNames.contains(currentQueryAllocatedBytesColumn)) {
        val queryBytes = query.allocatedBytes
        if (queryBytes == HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED) Values.NO_VALUE
        else getLongOrNull(queryBytes)
      } else Values.NO_VALUE
      val queryPageHits = if (requestedColumnsNames.contains(currentQueryPageHitsColumn))
        Values.longValue(query.pageHits)
      else Values.NO_VALUE
      val queryPageFaults = if (requestedColumnsNames.contains(currentQueryPageFaultsColumn))
        Values.longValue(query.pageFaults)
      else Values.NO_VALUE

      (
        currentQueryId,
        currentQuery,
        outerTransactionId,
        parameters,
        planner,
        runtime,
        indexes,
        queryStartTime,
        queryStatus,
        queryActiveLockCount,
        queryElapsedTime,
        queryCpuTime,
        queryWaitTime,
        queryIdleTime,
        queryAllocatedBytes,
        queryPageHits,
        queryPageFaults
      )
    } else (
      EMPTY,
      EMPTY,
      EMPTY,
      MapValue.EMPTY,
      EMPTY,
      EMPTY,
      VirtualValues.EMPTY_LIST,
      EMPTY,
      EMPTY,
      Values.NO_VALUE,
      Values.NO_VALUE,
      Values.NO_VALUE,
      Values.NO_VALUE,
      Values.NO_VALUE,
      Values.NO_VALUE,
      Values.NO_VALUE,
      Values.NO_VALUE
    )

  private def getLongOrNull(long: lang.Long) = long match {
    case l: lang.Long => Values.longValue(l)
    case _            => Values.NO_VALUE
  }

  private def getDurationOrNullFromMillis(long: lang.Long) = long match {
    case l: lang.Long => Values.durationValue(Duration.ofMillis(l))
    case _            => Values.NO_VALUE
  }

  private def getDurationOrNullFromMicro(long: lang.Long) = long match {
    case l: lang.Long => Values.durationValue(Duration.ofMillis(TimeUnit.MICROSECONDS.toMillis(l)))
    case _            => Values.NO_VALUE
  }

  private def getMapValue(m: util.Map[String, AnyRef]) = {
    val scalaMap = m.asScala
    val keys = scalaMap.keys.toArray
    val vals: Array[AnyValue] = scalaMap.values.map(ValueUtils.of).toArray
    VirtualValues.map(keys, vals)
  }

  private def formatTimeString(startTime: Long, zoneId: ZoneId) =
    formatTime(startTime, zoneId).format(ISO_OFFSET_DATE_TIME)

  private def getStatus(
    handle: KernelTransactionHandle,
    transactionDependenciesResolver: TransactionDependenciesResolver
  ): (String, String) =
    handle.terminationMark.map[(String, String)](info =>
      (String.format(TransactionId.TERMINATED_STATE, info.getReason.code), handle.getStatusDetails)
    )
      .orElseGet(() => getExecutingStatus(handle, transactionDependenciesResolver))

  private def resolveStatusDetails(handle: KernelTransactionHandle, defaultDetails: String): String = {
    var handleDetails = handle.getStatusDetails
    if (StringUtils.isEmpty(handleDetails)) {
      handleDetails = defaultDetails
    }
    handleDetails
  }

  private def getExecutingStatus(
    handle: KernelTransactionHandle,
    transactionDependenciesResolver: TransactionDependenciesResolver
  ): (String, String) =
    if (transactionDependenciesResolver.isBlocked(handle))
      (
        TransactionId.BLOCKED_STATE + transactionDependenciesResolver.describeBlockingTransactions(handle),
        handle.getStatusDetails
      )
    else if (handle.isCommitting) {
      (TransactionId.CLOSING_STATE, resolveStatusDetails(handle, TransactionId.COMMITTING_STATE))
    } else if (handle.isRollingback) {
      (TransactionId.CLOSING_STATE, resolveStatusDetails(handle, TransactionId.ROLLING_BACK_STATE))
    } else {
      (TransactionId.RUNNING_STATE, handle.getStatusDetails)
    }

}
