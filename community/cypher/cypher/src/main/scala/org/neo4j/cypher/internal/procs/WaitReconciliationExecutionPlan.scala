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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SystemCommandRuntimeName
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DATABASE_UUID_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.NAMESPACE_PROPERTY
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.TARGETS
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

import java.util

import scala.jdk.CollectionConverters.SetHasAsJava

/**
 * This plan calls the internal procedure internal.dbms.admin.wait which waits for a transaction to replicate across a cluster.
 * It is used to implement the WAIT clause on admin commands, e.g. CREATE DATABASE foo WAIT
 */
case class WaitReconciliationExecutionPlan(
  name: String,
  normalExecutionEngine: ExecutionEngine,
  systemParams: MapValue,
  queryHandler: QueryHandler,
  databaseNameParamKey: String,
  databaseNamespaceParamKey: String,
  timeoutInSeconds: Long,
  source: ExecutionPlan,
  parameterConverter: (Transaction, MapValue) => MapValue = (_, p) => p,
  parameterValidator: (Transaction, MapValue) => (MapValue, Set[InternalNotification]) = (_, p) => (p, Set.empty)
) extends AdministrationChainedExecutionPlan(Some(source)) {

  private val txIdParam = "__internal_transactionId"

  override def onSkip(
    ctx: SystemUpdateCountingQueryContext,
    subscriber: QuerySubscriber,
    runtimeNotifications: Set[InternalNotification]
  ): RuntimeResult = {
    SingleRowRuntimeResult(
      Array("address", "state", "message", "success"),
      Array(
        Values.stringValue(""),
        Values.stringValue("CaughtUp"),
        Values.stringValue("No operation needed"),
        Values.booleanValue(true)
      ),
      subscriber,
      runtimeNotifications
    )
  }

  override def runSpecific(
    ctx: SystemUpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    ignore: InputDataStream,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {

    val query =
      s"""OPTIONAL MATCH (d:$DATABASE_NAME {$DATABASE_NAME_PROPERTY: $$`$databaseNameParamKey`, $NAMESPACE_PROPERTY: $$`$databaseNamespaceParamKey`})-[:$TARGETS]-(db:$DATABASE)
         |WITH coalesce(db.$DATABASE_UUID_PROPERTY,$$`__internal_databaseUuid`) as uuid, coalesce(db.$DATABASE_NAME_PROPERTY,$$`__internal_deletedDatabaseName`) as name
         |CALL internal.dbms.admin.wait($$`$txIdParam`, uuid, name, $timeoutInSeconds)
         |YIELD address, state, message, success RETURN address, state, message, success""".stripMargin
    val tc: TransactionalContext = ctx.kernelTransactionalContext

    var revertAccessModeChange: KernelTransaction.Revertable = null
    try {
      val tx = tc.transaction()
      val (updatedParams, notifications) = {
        parameterValidator(
          tx,
          parameterConverter(
            tx,
            safeMergeParameters(systemParams, params, ctx.contextVars)
          )
        )
      }

      // We can't wait for a transaction from the same transaction so commit the existing transaction
      // and start a new one like PERIODIC COMMIT does
      val oldTxId = tc.commitAndRestartTx()

      val securityContext = tc.securityContext()
      val fullAccess = securityContext.withMode(AccessMode.Static.FULL)
      revertAccessModeChange = tc.kernelTransaction().overrideWith(fullAccess)

      // Need to wait for the old transaction
      val txParams = VirtualValues.map(Array(txIdParam), Array(Values.longValue(oldTxId)))
      val paramsWithTxId = updatedParams.updatedWith(txParams)

      val systemSubscriber = new SystemCommandQuerySubscriber(ctx, subscriber, queryHandler, params)
      val execution = normalExecutionEngine.executeSubquery(
        query,
        paramsWithTxId,
        tc,
        isOutermostQuery = false,
        executionMode == ProfileMode,
        prePopulateResults,
        systemSubscriber
      ).asInstanceOf[InternalExecutionResult]
      systemSubscriber.assertNotFailed()

      SystemCommandRuntimeResult(
        ctx,
        new SystemCommandExecutionResult(execution),
        systemSubscriber,
        fullAccess,
        tc.kernelTransaction(),
        previousNotifications ++ notifications
      )
    } finally {
      if (revertAccessModeChange != null) revertAccessModeChange.close()
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil
}

case class SingleRowRuntimeResult(
  cols: Array[String],
  row: Array[Value],
  subscriber: QuerySubscriber,
  runtimeNotifications: Set[InternalNotification]
) extends RuntimeResult {
  import org.neo4j.cypher.internal.runtime.QueryStatistics

  private var cs = ConsumptionState.NOT_STARTED
  subscriber.onResult(cols.length)

  override def fieldNames(): Array[String] = cols
  override def queryStatistics(): QueryStatistics = QueryStatistics()
  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED
  override def consumptionState: RuntimeResult.ConsumptionState = cs
  override def close(): Unit = {}
  override def queryProfile(): QueryProfile = QueryProfile.NONE

  override def request(numberOfRecords: Long): Unit = if (numberOfRecords > 0 && cs != ConsumptionState.EXHAUSTED) {
    subscriber.onRecord()
    row.zipWithIndex.foreach { case (v, i) => subscriber.onField(i, v) }
    subscriber.onRecordCompleted()
    subscriber.onResultCompleted(queryStatistics())
    cs = ConsumptionState.EXHAUSTED
  }
  override def cancel(): Unit = cs = ConsumptionState.EXHAUSTED
  override def await(): Boolean = cs == ConsumptionState.NOT_STARTED
  override def notifications(): util.Set[InternalNotification] = runtimeNotifications.asJava
}
