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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.apache.commons.lang3.StringUtils.EMPTY
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.internal.kernel.api.helpers.TransactionDependenciesResolver
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction.SHOW_TRANSACTION
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.query.QuerySnapshot
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME
import java.util
import scala.collection.JavaConverters.collectionAsScalaIterableConverter
import scala.collection.JavaConverters.mapAsScalaMapConverter

// SHOW TRANSACTION[S] [transaction-id[,...]] [WHERE clause|YIELD clause]
case class ShowTransactionsCommand(givenIds: Either[List[String], Expression], verbose: Boolean, defaultColumns: List[ShowColumn]) extends Command(defaultColumns) {

  override def originalNameRows(state: QueryState): ClosingIterator[Map[String, AnyValue]] = {
    val ids = TransactionCommandHelper.extractIds(givenIds, state.params)
    val ctx = state.query
    val securityContext = ctx.transactionalContext.securityContext

    val allowedTransactions = ctx.getDatabaseManager.registeredDatabases.values.asScala.toList
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

    val askedForTransactions = if (ids.nonEmpty) allowedTransactions.filter {
      case (transaction: KernelTransactionHandle, _, dbName: String) =>
        val txId = TransactionId(dbName, transaction.getUserTransactionId).toString
        ids.contains(txId)
    } else allowedTransactions

    val handleQuerySnapshotsMap = new util.HashMap[KernelTransactionHandle, util.Optional[QuerySnapshot]]
    askedForTransactions.foreach {
      case (transaction: KernelTransactionHandle, querySnapshot: util.Optional[QuerySnapshot], _) =>
        handleQuerySnapshotsMap.put( transaction, querySnapshot )
    }
    val transactionDependenciesResolver = new TransactionDependenciesResolver(handleQuerySnapshotsMap)

    val zoneId = getConfiguredTimeZone(ctx)
    val rows = askedForTransactions.map {
      case (transaction: KernelTransactionHandle, querySnapshot: util.Optional[QuerySnapshot], dbName: String) =>
        val statistic = transaction.transactionStatistic
        val clientInfo = transaction.clientInfo

        val txId = TransactionId(dbName, transaction.getUserTransactionId).toString
        val username = transaction.subject.executingUser()
        val startTime = formatTime( transaction.startTime(), zoneId )
        val elapsedTimeMillis = Duration.ofMillis(statistic.getElapsedTimeMillis)
        val allocatedBytes = statistic.getHeapAllocatedBytes
        val (currentQueryId, currentQuery) = if (querySnapshot.isPresent) {
          val snapshot = querySnapshot.get
          val currentQueryId = QueryId(snapshot.internalQueryId).toString
          val currentQuery = snapshot.obfuscatedQueryText.orElse(null)
          (currentQueryId, currentQuery)
        } else (EMPTY, EMPTY)
        val connectionId = clientInfo.map[String](_.connectionId).orElse(EMPTY)
        val clientAddress = clientInfo.map[String](_.clientAddress).orElse(EMPTY)
        val status = getStatus(transaction, transactionDependenciesResolver)

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
          "currentQuery" -> Values.stringOrNoValue(currentQuery),
          // The start time of the transaction
          "startTime" -> Values.stringValue(startTime),
          // The status of the transaction (terminated, blocked, closing or running)
          "status" -> Values.stringValue(status),
          // The time elapsed
          "elapsedTime" -> Values.durationValue(elapsedTimeMillis),
          // The bytes allocated by the transaction
          "allocatedBytes" -> Values.longValue(allocatedBytes)
        )
        if (verbose) {
          def getMapValue(m: util.Map[String, AnyRef]) = {
            val scalaMap = m.asScala
            val keys = scalaMap.keys.toArray
            val vals: Array[AnyValue] = scalaMap.values.map(Values.of).toArray
            VirtualValues.map(keys, vals)
          }

          val (outerTransactionId, parameters, planner, runtime, indexes) = if ( querySnapshot.isPresent ) {
            val query = querySnapshot.get
            val queryTransactionId = TransactionId(dbName, query.transactionId).toString
            val outerTransactionId = if (queryTransactionId == txId) EMPTY else queryTransactionId
            val parameters = query.obfuscatedQueryParameters().orElse( MapValue.EMPTY )
            val planner = query.planner
            val runtime = query.runtime
            val indexes = VirtualValues.list(query.indexes.asScala.toList.map(m => {
              val scalaMap = m.asScala
              val keys = scalaMap.keys.toArray
              val vals: Array[AnyValue] = scalaMap.values.map(Values.stringValue).toArray
              VirtualValues.map(keys, vals)
            }): _*)
            (outerTransactionId, parameters, planner, runtime, indexes)
          } else (EMPTY, MapValue.EMPTY, EMPTY, EMPTY, VirtualValues.EMPTY_LIST)
          val metaData = getMapValue(transaction.getMetaData)
          val protocol = clientInfo.map[String](_.protocol).orElse(EMPTY)
          val requestUri = clientInfo.map[String](_.requestURI).orElse(EMPTY)
          val statusDetails = transaction.getStatusDetails
          val resourceInformation = getMapValue(querySnapshot.map[util.Map[String, AnyRef]](_.resourceInformation()).orElse(util.Collections.emptyMap()))
          val activeLockCount = transaction.activeLocks.count
          val cpuTimeMillis = Duration.ofMillis(statistic.getCpuTimeMillis)
          val waitTimeMillis = Duration.ofMillis(statistic.getWaitTimeMillis)
          val idleTimeMillis = Duration.ofMillis(statistic.getIdleTimeMillis)
          val allocatedDirectBytes = statistic.getNativeAllocatedBytes
          val estimatedUsedHeapMemory = statistic.getEstimatedUsedHeapMemory
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
            // Protocol for the transaction
            "protocol" -> Values.stringValue(protocol),
            // Request URI for the transaction
            "requestUri" -> Values.stringValue(requestUri),
            // Any string a dedicated kernel API will write to track the transaction progress
            "statusDetails" -> Values.stringValue(statusDetails),
            // Resource information for the transaction
            "resourceInformation" -> resourceInformation,
            // Number of active locks held by the transaction
            "activeLockCount" -> Values.longValue(activeLockCount),
            // The CPU time
            "cpuTime" -> Values.durationValue(cpuTimeMillis),
            // The wait time
            "waitTime" -> Values.durationValue(waitTimeMillis),
            // The idle time
            "idleTime" -> Values.durationValue(idleTimeMillis),
            // The direct bytes allocated by the transaction
            "allocatedDirectBytes" -> Values.longValue(allocatedDirectBytes),
            // The estimation of heap memory used by the transaction
            "estimatedUsedHeapMemory" -> Values.longValue(estimatedUsedHeapMemory),
            // The page hits
            "pageHits" -> Values.longValue(pageHits),
            // The page faults
            "pageFaults" -> Values.longValue(pageFaults),
            // The initialization stacktrace
            "initializationStackTrace" -> Values.stringValue(initializationStackTrace),
          )
        } else {
          briefResult
        }
    }
    ClosingIterator.apply(rows.iterator)
  }

  private def getConfiguredTimeZone(ctx: QueryContext): ZoneId =
    ctx.getConfig.get(GraphDatabaseSettings.db_timezone).getZoneId

  private def formatTime(startTime: Long, zoneId: ZoneId) =
    OffsetDateTime
      .ofInstant(Instant.ofEpochMilli(startTime), zoneId)
      .format(ISO_OFFSET_DATE_TIME)

  private def getStatus(handle: KernelTransactionHandle, transactionDependenciesResolver: TransactionDependenciesResolver): String =
    handle.terminationReason.map[String](reason => String.format(TransactionId.TERMINATED_STATE, reason.code))
      .orElseGet(() => getExecutingStatus(handle, transactionDependenciesResolver))

  private def getExecutingStatus(handle: KernelTransactionHandle, transactionDependenciesResolver: TransactionDependenciesResolver): String =
    if (transactionDependenciesResolver.isBlocked(handle)) TransactionId.BLOCKED_STATE + transactionDependenciesResolver.describeBlockingTransactions(handle)
    else if (handle.isClosing) TransactionId.CLOSING_STATE
    else TransactionId.RUNNING_STATE

}
