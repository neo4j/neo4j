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
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.StaticAccessMode
import org.neo4j.kernel.api.query.RuntimeName
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

import scala.util.Using

case class SystemGraphWriteExecutionPlan(
  name: String,
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  source: Option[ExecutionPlan],
  write: (Transaction, SecurityContext, MapValue) => Unit,
  parameterTransformer: ParameterTransformer = ParameterTransformer(),
  checkCredentialsExpired: Boolean = true
) extends AdministrationChainedExecutionPlan(source) {

  override def runSpecific(
    ctx: SystemUpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {
    val tc = ctx.kernelTransactionalContext
    val securityContext = tc.securityContext()
    if (securityContext.impersonating()) throw AuthorizationViolationException.updatesWhenImpersonating()
    if (checkCredentialsExpired) securityContext.assertCredentialsNotExpired(securityAuthorizationHandler)
    Using.resource(tc.kernelTransaction().overrideWith(securityContext.withMode(StaticAccessMode.FULL))) { _ =>
      val tx = tc.transaction()
      val (updatedParams, notifications) =
        parameterTransformer.transform(tx, securityContext, MapValue.EMPTY, params)
      write(tx, securityContext, updatedParams)
      ctx.systemUpdates.increase()
      UpdatingSystemCommandRuntimeResult(ctx, None, previousNotifications ++ notifications)
    }
  }

  override def runtimeName: RuntimeName = RuntimeName.SYSTEM
}
