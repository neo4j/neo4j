/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.memory.OptionalMemoryTracker
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

/**
 * This plan calls the internal procedure dbms.admin.wait which waits for a transaction to replicate across a cluster.
 * It is used to implement the WAIT clause on admin commands, e.g. CREATE DATABASE foo WAIT
 */
case class WaitReconciliationExecutionPlan(
    name: String,
    normalExecutionEngine: ExecutionEngine,
    systemParams: MapValue,
    queryHandler: QueryHandler,
    databaseNameParamKey: String,
    timeoutInSeconds: Long,
    source: ExecutionPlan,
    parameterConverter: (Transaction, MapValue) => MapValue = (_, p) => p,
) extends AdministrationChainedExecutionPlan(Some(source)) {

  override def onSkip(ctx: SystemUpdateCountingQueryContext,
                      subscriber: QuerySubscriber): RuntimeResult = {
    SingleRowRuntimeResult(Array("address","state", "message", "success"),
      Array(Values.stringValue(""), Values.stringValue("CaughtUp"), Values.stringValue("No operation needed"), Values.booleanValue(true)),
      subscriber)
  }

  override def runSpecific(ctx: SystemUpdateCountingQueryContext,
                           executionMode: ExecutionMode,
                           params: MapValue,
                           prePopulateResults: Boolean,
                           ignore: InputDataStream,
                           subscriber: QuerySubscriber): RuntimeResult = {

    val query =
      s"""CALL dbms.admin.wait($$`__internal_transactionId`, $$`__internal_databaseUuid`, $$`$databaseNameParamKey`, $timeoutInSeconds)
         |YIELD address, state, message, success RETURN address, state, message, success""".stripMargin
    val tc: TransactionalContext = ctx.kernelTransactionalContext

    var revertAccessModeChange: KernelTransaction.Revertable = null
    try {
      val securityContext = tc.securityContext()
      val fullAccess = securityContext.withMode(AccessMode.Static.FULL)
      revertAccessModeChange = tc.kernelTransaction().overrideWith(fullAccess)

      val updatedParams = parameterConverter(tc.transaction(), safeMergeParameters(systemParams, params, ctx.contextVars))
      // We can't wait for a transaction from the same transaction so commit the existing transaction
      // and start a new one like PERIODIC COMMIT does
      val oldTxId = tc.commitAndRestartTx()

      // Need to wait for the old transaction
      val txParams =  VirtualValues.map(Array("__internal_transactionId"), Array(Values.longValue(oldTxId)))
      val paramsWithTxId = updatedParams.updatedWith(txParams)

      val systemSubscriber = new SystemCommandQuerySubscriber(ctx, subscriber, queryHandler, params)
      val execution = normalExecutionEngine.executeSubQuery(query, paramsWithTxId, tc, isOutermostQuery = false, executionMode == ProfileMode, prePopulateResults, systemSubscriber).asInstanceOf[InternalExecutionResult]
      systemSubscriber.assertNotFailed()

      SystemCommandRuntimeResult(ctx, new SystemCommandExecutionResult(execution), systemSubscriber, fullAccess, tc.kernelTransaction())
    } finally {
      if (revertAccessModeChange != null) revertAccessModeChange
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

case class SingleRowRuntimeResult(cols: Array[String], row: Array[Value], subscriber: QuerySubscriber) extends RuntimeResult {
  import org.neo4j.cypher.internal.runtime.QueryStatistics

  private var cs = ConsumptionState.NOT_STARTED
  subscriber.onResult(cols.length)

  override def fieldNames(): Array[String] = cols
  override def queryStatistics(): QueryStatistics = QueryStatistics()
  override def totalAllocatedMemory(): Long = OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED
  override def consumptionState: RuntimeResult.ConsumptionState = cs
  override def close(): Unit = {}
  override def queryProfile(): QueryProfile = QueryProfile.NONE
  override def request(numberOfRecords: Long): Unit = if (numberOfRecords > 0 && cs != ConsumptionState.EXHAUSTED) {
    subscriber.onRecord()
    row.zipWithIndex.foreach{ case (v, i) => subscriber.onField(i,v) }
    subscriber.onRecordCompleted()
    subscriber.onResultCompleted(queryStatistics())
    cs = ConsumptionState.EXHAUSTED
  }
  override def cancel(): Unit = cs = ConsumptionState.EXHAUSTED
  override def await(): Boolean = cs == ConsumptionState.NOT_STARTED
}
