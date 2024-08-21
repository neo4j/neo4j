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

import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

case class NonTransactionalUpdatingSystemCommandExecutionPlan(
  name: String,
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  query: String,
  systemParams: MapValue,
  queryHandler: QueryHandler,
  source: Option[ExecutionPlan] = None,
  checkCredentialsExpired: Boolean = true,
  initAndFinally: InitAndFinally = NoInitAndFinally,
  parameterTransformer: ParameterTransformer = ParameterTransformer(),
  assertPrivilegeAction: Transaction => Unit = _ => {}
) extends UpdatingSystemCommandExecutionPlanBase(
      name,
      normalExecutionEngine,
      securityAuthorizationHandler,
      query,
      systemParams,
      queryHandler,
      source,
      checkCredentialsExpired,
      initAndFinally,
      parameterTransformer,
      assertPrivilegeAction
    ) {

  override def runSpecific(
    ctx: SystemUpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {
    val innerTxContext = ctx.contextWithNewTransaction()
    val innerKTC = innerTxContext.kernelTransactionalContext
    var result: RuntimeResult = null
    try {
      result = super.runSpecific(
        innerTxContext,
        executionMode,
        params,
        prePopulateResults,
        subscriber,
        previousNotifications
      )
      innerKTC.statement().close()
      innerKTC.kernelTransaction().commit()
    } catch {
      case e: Throwable => throw e
    } finally {
      innerKTC.kernelTransaction().close()
      innerKTC.close()
      innerTxContext.close()
    }
    result
  }

}
