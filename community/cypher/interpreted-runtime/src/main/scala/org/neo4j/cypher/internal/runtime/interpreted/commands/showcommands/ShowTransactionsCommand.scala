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
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.CollectionHasAsScala
import scala.jdk.CollectionConverters.MapHasAsScala

// SHOW TRANSACTION[S] [transaction-id[,...]] [WHERE clause|YIELD clause]
case class ShowTransactionsCommand(
  givenIds: Either[List[String], Expression],
  verbose: Boolean,
  defaultColumns: List[ShowColumn],
  yieldColumns: List[CommandResultItem]
) extends TransactionCommand(defaultColumns, yieldColumns) {

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
        def getLongOrNull(long: lang.Long) = long match {
          case l: lang.Long => Values.longValue(l)
          case _            => Values.NO_VALUE
        }

        def getDurationOrNullFromMillis(long: lang.Long) = long match {
          case l: lang.Long => Values.durationValue(Duration.ofMillis(l))
          case _            => Values.NO_VALUE
        }

        def getDurationOrNullFromMicro(long: lang.Long) = long match {
          case l: lang.Long => Values.durationValue(Duration.ofMillis(TimeUnit.MICROSECONDS.toMillis(l)))
          case _            => Values.NO_VALUE
        }

        val statistic = transaction.transactionStatistic
        val clientInfo = transaction.clientInfo

        val txId = TransactionId(dbName, transaction.getTransactionSequenceNumber).toString
        val username = transaction.subject.executingUser()
        val startTime = formatTimeString(transaction.startTime(), zoneId)
        val elapsedTime = getDurationOrNullFromMillis(statistic.getElapsedTimeMillis)
        val (currentQueryId, currentQuery) =
          if (querySnapshot.isPresent) {
            val snapshot = querySnapshot.get
            val currentQueryId = QueryId(snapshot.internalQueryId).toString
            val currentQuery = snapshot.obfuscatedQueryText.orElse(EMPTY)
            (currentQueryId, currentQuery)
          } else (EMPTY, EMPTY)
        val connectionId = clientInfo.map[String](_.connectionId).orElse(EMPTY)
        val clientAddress = clientInfo.map[String](_.clientAddress).orElse(EMPTY)
        val (status, statusDetails) = getStatus(transaction, transactionDependenciesResolver)

        val briefResult = Map(
          // Name of the database the transaction belongs to
          "database" -> Values.stringValue(dbName),
          // The id of the transaction
          "transactionId" -> Values.stringValue(txId),
          // The id of the currently executing query
          "currentQueryId" -> Values.stringValue(currentQueryId),
          // The id of the connection the transaction belongs to
          "connectionId" -> Values.stringValue(connectionId),
          // The client address
          "clientAddress" -> Values.stringValue(clientAddress),
          // The name of the user running the transaction
          "username" -> Values.stringValue(username),
          // The currently executing query
          "currentQuery" -> Values.stringValue(currentQuery),
          // The start time of the transaction
          "startTime" -> Values.stringValue(startTime),
          // The status of the transaction (terminated, blocked, closing or running)
          "status" -> Values.stringValue(status),
          // The time elapsed
          "elapsedTime" -> elapsedTime
        )
        if (verbose) {
          def getMapValue(m: util.Map[String, AnyRef]) = {
            val scalaMap = m.asScala
            val keys = scalaMap.keys.toArray
            val vals: Array[AnyValue] = scalaMap.values.map(ValueUtils.of).toArray
            VirtualValues.map(keys, vals)
          }

          val (
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
          ) =
            if (querySnapshot.isPresent) {
              val query = querySnapshot.get
              val parentDbName = query.parentDbName();
              val parentTransactionId = query.parentTransactionId();
              val querySessionDbName = if (parentDbName != null) parentDbName else dbName
              val querySessionTransactionId =
                if (parentTransactionId > -1L) parentTransactionId else query.transactionId()
              val querySessionTransaction = TransactionId(querySessionDbName, querySessionTransactionId).toString
              val outerTransactionId = if (querySessionTransaction == txId) EMPTY else querySessionTransaction
              val parameters = query.obfuscatedQueryParameters().orElse(MapValue.EMPTY)
              val maybePlanner = query.planner
              val planner = if (maybePlanner == null) EMPTY else maybePlanner
              val maybeRuntime = query.runtime
              val runtime = if (maybeRuntime == null) EMPTY else maybeRuntime
              val indexes = VirtualValues.list(query.indexes.asScala.toList.map(m => {
                val scalaMap = m.asScala
                val keys = scalaMap.keys.toArray
                val vals: Array[AnyValue] = scalaMap.values.map(Values.stringValue).toArray
                VirtualValues.map(keys, vals)
              }): _*)

              val queryStartTime = formatTimeString(query.startTimestampMillis, zoneId)
              val queryStatus = query.status
              val queryActiveLockCount = Values.longValue(query.activeLockCount)
              val queryElapsedTime = getDurationOrNullFromMicro(query.elapsedTimeMicros)
              val optionalCpuTime = query.cpuTimeMicros
              val queryCpuTime =
                if (optionalCpuTime.isPresent) getDurationOrNullFromMicro(optionalCpuTime.getAsLong)
                else Values.NO_VALUE
              val queryWaitTime =
                Values.durationValue(Duration.ofMillis(TimeUnit.MICROSECONDS.toMillis(query.waitTimeMicros)))
              val optionalIdleTime = query.idleTimeMicros
              val queryIdleTime =
                if (optionalIdleTime.isPresent) getDurationOrNullFromMicro(optionalIdleTime.getAsLong)
                else Values.NO_VALUE
              val queryBytes = query.allocatedBytes
              val queryAllocatedBytes =
                if (queryBytes == HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED) Values.NO_VALUE
                else getLongOrNull(queryBytes)
              val queryPageHits = Values.longValue(query.pageHits)
              val queryPageFaults = Values.longValue(query.pageFaults)

              (
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
          val metaData = getMapValue(transaction.getMetaData)
          val protocol = clientInfo.map[String](_.protocol).orElse(EMPTY)
          val requestUri = clientInfo.map[String](_.requestURI).orElse(null)
          val resourceInformation = getMapValue(
            querySnapshot.map[util.Map[String, AnyRef]](_.resourceInformation()).orElse(util.Collections.emptyMap())
          )
          val activeLockCount = transaction.activeLocks.size()
          val cpuTime = getDurationOrNullFromMillis(statistic.getCpuTimeMillis)
          val waitTime = Values.durationValue(Duration.ofMillis(statistic.getWaitTimeMillis))
          val idleTime = getDurationOrNullFromMillis(statistic.getIdleTimeMillis)
          val allocatedDirectBytes = getLongOrNull(statistic.getNativeAllocatedBytes)
          val estimatedUsedHeapMemory = getLongOrNull(statistic.getEstimatedUsedHeapMemory)
          val pageHits = statistic.getPageHits
          val pageFaults = statistic.getPageFaults
          val initializationStackTrace = transaction.transactionInitialisationTrace.getTrace

          briefResult ++ Map(
            // Id of outer transaction if it exists
            "outerTransactionId" -> Values.stringValue(outerTransactionId),
            // Metadata for the transaction
            "metaData" -> metaData,
            // Parameters for the currently executing query
            "parameters" -> parameters,
            // Planner for the currently executing query
            "planner" -> Values.stringValue(planner),
            // Runtime for the currently executing query
            "runtime" -> Values.stringValue(runtime),
            // Indexes used by the currently executing query
            "indexes" -> indexes,
            // The start time of the currently executing query
            "currentQueryStartTime" -> Values.stringValue(queryStartTime),
            // Protocol for the transaction
            "protocol" -> Values.stringValue(protocol),
            // Request URI for the transaction
            "requestUri" -> Values.stringOrNoValue(requestUri),
            // The status of the currently executing query (parsing, planning, planned, running, waiting)
            "currentQueryStatus" -> Values.stringValue(queryStatus),
            // Any string a dedicated kernel API will write to track the transaction progress
            "statusDetails" -> Values.stringValue(statusDetails),
            // Resource information for the transaction
            "resourceInformation" -> resourceInformation,
            // Number of active locks held by the transaction
            "activeLockCount" -> Values.longValue(activeLockCount),
            // Number of active locks held by the currently executing query
            "currentQueryActiveLockCount" -> queryActiveLockCount,
            // The CPU time
            "cpuTime" -> cpuTime,
            // The wait time
            "waitTime" -> waitTime,
            // The idle time
            "idleTime" -> idleTime,
            // The time elapsed of the currently executing query
            "currentQueryElapsedTime" -> queryElapsedTime,
            // The CPU time of the currently executing query
            "currentQueryCpuTime" -> queryCpuTime,
            // The wait time of the currently executing query
            "currentQueryWaitTime" -> queryWaitTime,
            // The idle time of the currently executing query
            "currentQueryIdleTime" -> queryIdleTime,
            // The bytes allocated by the currently executing query
            "currentQueryAllocatedBytes" -> queryAllocatedBytes,
            // The direct bytes allocated by the transaction
            "allocatedDirectBytes" -> allocatedDirectBytes,
            // The estimation of heap memory used by the transaction
            "estimatedUsedHeapMemory" -> estimatedUsedHeapMemory,
            // The page hits
            "pageHits" -> Values.longValue(pageHits),
            // The page faults
            "pageFaults" -> Values.longValue(pageFaults),
            // The page hits of the currently executing query
            "currentQueryPageHits" -> queryPageHits,
            // The page faults of the currently executing query
            "currentQueryPageFaults" -> queryPageFaults,
            // The initialization stacktrace
            "initializationStackTrace" -> Values.stringValue(initializationStackTrace)
          )
        } else {
          briefResult
        }
    }

    val updatedRows = updateRowsWithPotentiallyRenamedColumns(rows)
    ClosingIterator.apply(updatedRows.iterator)
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
