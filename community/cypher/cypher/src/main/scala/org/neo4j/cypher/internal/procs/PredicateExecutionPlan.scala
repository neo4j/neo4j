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
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SystemCommandRuntimeName
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan.AccessModeChanger
import org.neo4j.cypher.internal.procs.PredicateExecutionPlan.NoAccessModeChange
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.EmptyQuerySubscription
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.cypher.result.RuntimeResult.ConsumptionState
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.KernelTransaction.Revertable
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.memory.HeapHighWaterMarkTracker
import org.neo4j.values.virtual.MapValue

import java.util

import scala.jdk.CollectionConverters.SetHasAsJava

trait Predicate {
  def execute(transactionalContext: TransactionalContext, params: MapValue): Boolean
}

case class SecurityPredicate(predicateFn: (MapValue, SecurityContext) => Boolean) extends Predicate {

  override def execute(transactionalContext: TransactionalContext, params: MapValue): Boolean =
    predicateFn(params, transactionalContext.securityContext)
}

case class DatabaseSecurityPredicate(predicateFn: (MapValue, TransactionalContext, SecurityContext) => Boolean)
    extends Predicate {

  override def execute(transactionalContext: TransactionalContext, params: MapValue): Boolean = {
    predicateFn(params, transactionalContext, transactionalContext.securityContext())
  }
}

import scala.util.Using

class PredicateExecutionPlan(
  predicate: Predicate,
  source: Option[ExecutionPlan] = None,
  onViolation: (MapValue, TransactionalContext, SecurityContext) => Exception,
  changeAccessMode: AccessModeChanger = NoAccessModeChange
) extends AdministrationChainedExecutionPlan(source) {

  override def runSpecific(
    originalCtx: SystemUpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {

    val securityContext = originalCtx.transactionalContext.securityContext
    val transactionalContext = originalCtx.kernelTransactionalContext

    Using(changeAccessMode(transactionalContext, securityContext)) { _ =>
      if (predicate.execute(transactionalContext, params)) {
        NoRuntimeResult(subscriber, previousNotifications)
      } else {
        throw onViolation(params, transactionalContext, securityContext)
      }
    }.get
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil
}

object PredicateExecutionPlan {

  type AccessModeChanger = (TransactionalContext, SecurityContext) => KernelTransaction.Revertable

  val NoAccessModeChange: AccessModeChanger = (_, _) =>
    new Revertable() {
      override def close(): Unit = {}
    }

  def apply(
    predicate: (MapValue, SecurityContext) => Boolean,
    source: Option[ExecutionPlan] = None,
    onViolation: (MapValue, TransactionalContext, SecurityContext) => Exception
  ) = new PredicateExecutionPlan(SecurityPredicate(predicate), source, onViolation)
}

case class NoRuntimeResult(subscriber: QuerySubscriber, runtimeNotifications: Set[InternalNotification])
    extends EmptyQuerySubscription(subscriber) with RuntimeResult {

  override def hasServedRows: Boolean = false

  override def fieldNames(): Array[String] = Array.empty

  override def queryStatistics(): QueryStatistics = QueryStatistics()

  override def heapHighWaterMark(): Long = HeapHighWaterMarkTracker.ALLOCATIONS_NOT_TRACKED

  override def consumptionState: RuntimeResult.ConsumptionState = ConsumptionState.EXHAUSTED

  override def close(): Unit = {}

  override def queryProfile(): QueryProfile = QueryProfile.NONE

  override def notifications(): util.Set[InternalNotification] = runtimeNotifications.asJava

  override def getErrorOrNull: Throwable = null
}
