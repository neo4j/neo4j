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
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SystemCommandRuntimeName
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.util.Using

/**
 * Execution plan for performing system commands, i.e. creating databases or showing roles and users.
 */
case class SystemCommandExecutionPlan(
  name: String,
  normalExecutionEngine: ExecutionEngine,
  securityAuthorizationHandler: SecurityAuthorizationHandler,
  query: String,
  systemParams: MapValue,
  source: Option[ExecutionPlan] = None,
  checkCredentialsExpired: Boolean = true,
  parameterTransformer: ParameterTransformerFunction = ParameterTransformer(),
  modeConverter: SecurityContext => SecurityContext = s => s.withMode(AccessMode.Static.READ)
) extends AdministrationChainedExecutionPlan(source) {

  override def runSpecific(
    ctx: SystemUpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {

    val tc: TransactionalContext = ctx.kernelTransactionalContext

    withFullDatabaseAccess(tc) { elevatedSecurityContext =>
      val securityContext = tc.securityContext()
      val tx = tc.transaction()
      val (updatedParams, notifications) =
        parameterTransformer.transform(tx, securityContext, systemParams, params)

      val systemSubscriber = new SystemCommandQuerySubscriber(ctx, subscriber, new QueryHandler(), updatedParams)
      val execution = normalExecutionEngine.executeSubquery(
        queryPrefix + query,
        updatedParams,
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
        elevatedSecurityContext,
        tc.kernelTransaction(),
        previousNotifications ++ notifications ++ systemSubscriber.getNotifications
      )
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  private def withFullDatabaseAccess(tc: TransactionalContext)(elevatedWork: SecurityContext => RuntimeResult)
    : RuntimeResult = {
    val securityContext = tc.securityContext()
    if (checkCredentialsExpired) securityContext.assertCredentialsNotExpired(securityAuthorizationHandler)
    val elevatedSecurityContext = modeConverter(securityContext)
    Using.resource(tc.kernelTransaction().overrideWith(elevatedSecurityContext)) { _ =>
      elevatedWork(elevatedSecurityContext)
    }
  }
}

/**
 * A wrapping QuerySubscriber that overrides the error handling to allow custom error messages for SystemCommands instead of the inner errors.
 * It also makes sure to return QueryStatistics that don't leak information about the system graph like how many nodes we created for a command etc.
 */
class SystemCommandQuerySubscriber(
  ctx: SystemUpdateCountingQueryContext,
  inner: QuerySubscriber,
  queryHandler: QueryHandler,
  params: MapValue
) extends QuerySubscriber {
  @volatile private var empty = true
  @volatile private var ignore = false
  @volatile private var failed: Option[Throwable] = None
  @volatile private var contextUpdates: MapValue = MapValue.EMPTY
  @volatile private var notifications: Set[InternalNotification] = Set.empty

  override def onResult(numberOfFields: Int): Unit = if (failed.isEmpty) {
    inner.onResult(numberOfFields)
  }

  override def onResultCompleted(statistics: QueryStatistics): Unit = {
    if (empty) {
      queryHandler.onNoResults(params) match {
        case ThrowException(error) =>
          inner.onError(error)
          failed = Some(error)
        case IgnoreResults =>
          ignore = true
        case UpdateContextParams(params)         => contextUpdates = contextUpdates.updatedWith(params)
        case NotifyAndContinue(newNotifications) => notifications = notifications ++ newNotifications
        case Continue                            => ()
      }
    }
    if (failed.isEmpty) {
      if (statistics.containsUpdates()) {
        ctx.systemUpdates.increase()
      }
      inner.onResultCompleted(ctx.getStatistics)
    }
  }

  override def onRecord(): Unit = {
    if (failed.isEmpty) {
      empty = false
      inner.onRecord()
    }
  }

  override def onRecordCompleted(): Unit = if (failed.isEmpty) {
    inner.onRecordCompleted()
  }

  override def onField(offset: Int, value: AnyValue): Unit = {
    queryHandler.onResult(offset, value, params) match {
      case ThrowException(error) =>
        inner.onError(error)
        failed = Some(error)
      case IgnoreResults =>
        ignore = true
      case UpdateContextParams(params)         => contextUpdates = contextUpdates.updatedWith(params)
      case NotifyAndContinue(newNotifications) => notifications = notifications ++ newNotifications
      case Continue                            => ()
    }
    if (failed.isEmpty) {
      inner.onField(offset, value)
    }
  }

  override def onError(throwable: Throwable): Unit = {
    val handledError = queryHandler.onError(throwable, params)
    inner.onError(handledError)
    failed = Some(handledError)
  }

  def assertNotFailed(onFailure: Throwable => Unit = _ => ()): Unit = failed.foreach { exception =>
    onFailure(exception) // used to close resources
    throw exception
  }

  def shouldIgnoreResult(): Boolean = ignore

  def getContextUpdates: MapValue = contextUpdates

  def getNotifications: Set[InternalNotification] = notifications

  override def equals(obj: Any): Boolean = inner.equals(obj)
}
