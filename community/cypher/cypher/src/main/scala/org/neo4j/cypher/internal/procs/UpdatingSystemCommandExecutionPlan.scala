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
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.ProfileMode
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.TransientFailureException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status.HasStatus
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

import scala.util.Using

case class UpdatingSystemCommandExecutionPlan(
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
    )

/**
 * Execution plan for performing system commands, i.e. starting, stopping or dropping databases.
 */
abstract class UpdatingSystemCommandExecutionPlanBase(
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
    withFullDatabaseAccess(tc) { () =>
      val tx = tc.transaction()
      assertPrivilegeAction(tx)

      val (updatedParams, notifications) =
        parameterTransformer.transform(tx, securityContext, systemParams, params)
      val systemSubscriber =
        new SystemCommandQuerySubscriber(ctx, new RowDroppingQuerySubscriber(subscriber), queryHandler, updatedParams)
      assertCanWrite(tc, systemSubscriber)
      initAndFinally.execute(ctx, previousNotifications ++ notifications, updatedParams) { () =>
        val execution = normalExecutionEngine.executeSubquery(
          queryPrefix + query,
          updatedParams,
          tc,
          isOutermostQuery = false,
          profile = executionMode == ProfileMode,
          prePopulateResults,
          systemSubscriber
        ).asInstanceOf[InternalExecutionResult]
        try {
          execution.consumeAll()
        } catch {
          case _: Throwable =>
          // do nothing, exceptions are handled by SystemCommandQuerySubscriber
        }
        systemSubscriber.assertNotFailed()

        if (systemSubscriber.shouldIgnoreResult()) {
          IgnoredRuntimeResult(previousNotifications ++ notifications ++ systemSubscriber.getNotifications)
        } else {
          UpdatingSystemCommandRuntimeResult(
            ctx.withContextVars(systemSubscriber.getContextUpdates),
            previousNotifications ++ notifications ++ systemSubscriber.getNotifications
          )
        }
      }
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  private def withFullDatabaseAccess(tc: TransactionalContext)(elevatedWork: () => RuntimeResult): RuntimeResult = {
    val securityContext = tc.securityContext()
    if (securityContext.impersonating()) {
      throw new AuthorizationViolationException(
        "Not allowed to run updating system commands when impersonating a user."
      )
    }
    if (checkCredentialsExpired) securityContext.assertCredentialsNotExpired(securityAuthorizationHandler)
    Using.resource(tc.kernelTransaction().overrideWith(securityContext.withMode(AccessMode.Static.FULL))) { _ =>
      elevatedWork()
    }
  }

  private def assertCanWrite(tc: TransactionalContext, systemSubscriber: SystemCommandQuerySubscriber): Unit = {
    try {
      tc.kernelTransaction().dataWrite() // assert that we are allowed to write
    } catch {
      case e: Throwable =>
        systemSubscriber.onError(e)
    }
    systemSubscriber.assertNotFailed()
  }

}

sealed trait QueryHandlerResult
case object Continue extends QueryHandlerResult
case object IgnoreResults extends QueryHandlerResult
case class ThrowException(throwable: Throwable) extends QueryHandlerResult
case class UpdateContextParams(params: MapValue) extends QueryHandlerResult
case class NotifyAndContinue(notifications: Set[InternalNotification]) extends QueryHandlerResult

class QueryHandler {
  def onError(t: Throwable, p: MapValue): Throwable = t

  def onResult(offset: Int, value: AnyValue, p: MapValue): QueryHandlerResult = Continue

  def onNoResults(p: MapValue): QueryHandlerResult = Continue

  def countSystemUpdates(statistics: QueryStatistics): Int = {
    if (statistics.containsUpdates()) 1 else 0
  }
}

class QueryHandlerBuilder(parent: QueryHandler) extends QueryHandler {
  override def onError(t: Throwable, p: MapValue): Throwable = parent.onError(t, p)

  override def onResult(offset: Int, value: AnyValue, params: MapValue): QueryHandlerResult =
    parent.onResult(offset, value, params)

  override def onNoResults(params: MapValue): QueryHandlerResult = parent.onNoResults(params)

  override def countSystemUpdates(statistics: QueryStatistics): Int = parent.countSystemUpdates(statistics)

  def handleError(f: (Throwable, MapValue) => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(this) {

    override def onError(t: Throwable, p: MapValue): Throwable = {
      val mappedError = t match {
        case t: TransientFailureException => t
        case _                            => f(t, p)
      }
      checkOnlyWhenAssertionsAreEnabled(if (t.isInstanceOf[HasStatus]) mappedError.isInstanceOf[HasStatus] else true)
      mappedError
    }
  }

  def handleNoResult(f: MapValue => Option[QueryHandlerResult]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {

    override def onNoResults(params: MapValue): QueryHandlerResult =
      f(params).getOrElse(Continue)
  }

  def handleCountSystemUpdates(f: QueryStatistics => Int): QueryHandlerBuilder = new QueryHandlerBuilder(this) {

    override def countSystemUpdates(statistics: QueryStatistics): Int = {
      f(statistics)
    }
  }

  def ignoreNoResult(): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onNoResults(params: MapValue): QueryHandlerResult = IgnoreResults
  }

  def handleResult(handler: (Int, AnyValue, MapValue) => QueryHandlerResult): QueryHandlerBuilder =
    new QueryHandlerBuilder(this) {

      override def onResult(offset: Int, value: AnyValue, p: MapValue): QueryHandlerResult =
        handler(offset, value, p)
    }

  def ignoreOnResult(): QueryHandlerBuilder = new QueryHandlerBuilder(this) {

    override def onResult(offset: Int, value: AnyValue, p: MapValue): QueryHandlerResult = IgnoreResults
  }
}

object QueryHandler {

  def handleError(f: (Throwable, MapValue) => Throwable): QueryHandlerBuilder =
    new QueryHandlerBuilder(new QueryHandler).handleError(f)

  def handleNoResult(f: MapValue => Option[QueryHandlerResult]): QueryHandlerBuilder =
    new QueryHandlerBuilder(new QueryHandler).handleNoResult(f)

  def handleCountSystemUpdates(f: QueryStatistics => Int): QueryHandlerBuilder =
    new QueryHandlerBuilder(new QueryHandler).handleCountSystemUpdates(f)

  def ignoreNoResult(): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).ignoreNoResult()

  def handleResult(handler: (Int, AnyValue, MapValue) => QueryHandlerResult): QueryHandlerBuilder =
    new QueryHandlerBuilder(new QueryHandler).handleResult(handler)

  def ignoreOnResult(): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).ignoreOnResult()
}

sealed trait InitAndFinally {

  def execute(
    context: SystemUpdateCountingQueryContext,
    notifications: Set[InternalNotification],
    params: MapValue
  )(queryFunction: () => RuntimeResult): RuntimeResult = queryFunction()
}

case object NoInitAndFinally extends InitAndFinally

case class InitAndFinallyFunctions(
  initFunction: MapValue => Boolean = _ => true,
  finallyFunction: MapValue => Unit = _ => {}
) extends InitAndFinally {

  override def execute(
    context: SystemUpdateCountingQueryContext,
    notifications: Set[InternalNotification],
    params: MapValue
  )(queryFunction: () => RuntimeResult): RuntimeResult =
    try {
      if (initFunction(params)) {
        queryFunction()
      } else {
        UpdatingSystemCommandRuntimeResult(context, notifications)
      }
    } finally {
      finallyFunction(params)
    }
}
