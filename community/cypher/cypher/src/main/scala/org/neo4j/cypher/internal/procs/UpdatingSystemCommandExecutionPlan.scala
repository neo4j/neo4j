/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{ExecutionEngine, ExecutionPlan, RuntimeName, SystemCommandRuntimeName}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscriberAdapter}
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

/**
  * Execution plan for performing system commands, i.e. starting, stopping or dropping databases.
  */
case class UpdatingSystemCommandExecutionPlan(name: String,
                                              normalExecutionEngine: ExecutionEngine,
                                              query: String,
                                              systemParams: MapValue,
                                              queryHandler: QueryHandler,
                                              source: Option[ExecutionPlan] = None)
  extends ExecutionPlan {

  override def run(originalCtx: QueryContext,
                   doProfile: Boolean,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult = {

    val ctx = SystemUpdateCountingQueryContext.from(originalCtx)
    // Only the outermost query should be tied into the reactive results stream. The source queries should use an empty subscriber
    val sourceResult = source.map(_.run(ctx, doProfile, params, prePopulateResults, ignore, new QuerySubscriberAdapter(){
      override def onResultCompleted(statistics: QueryStatistics): Unit = if(statistics.containsUpdates()) ctx.systemUpdates.increase()
    }))
    sourceResult match {
      case Some(IgnoredRuntimeResult) => IgnoredRuntimeResult
      case _ =>
        val tc = ctx.kernelTransactionalContext
        if (!name.equals("AlterCurrentUserSetPassword") && !tc.securityContext().isAdmin) throw new AuthorizationViolationException(PERMISSION_DENIED)

        var revertAccessModeChange: KernelTransaction.Revertable = null
        try {
          val fullAccess = tc.securityContext().withMode(AccessMode.Static.FULL)
          revertAccessModeChange = tc.kernelTransaction().overrideWith(fullAccess)
          tc.kernelTransaction().dataWrite() // assert that we are allowed to write

          val systemSubscriber = new SystemCommandQuerySubscriber(ctx, new RowDroppingQuerySubscriber(subscriber), queryHandler)
          val execution = normalExecutionEngine.executeSubQuery(query, systemParams, tc, shouldCloseTransaction = false, doProfile, prePopulateResults, systemSubscriber).asInstanceOf[InternalExecutionResult]
          try {
            execution.consumeAll()
          } catch {
            case _: Throwable =>
              // do nothing, exceptions are handled by SystemCommandQuerySubscriber
          }
          systemSubscriber.assertNotFailed()

          if (systemSubscriber.shouldIgnoreResult())
            IgnoredRuntimeResult
          else
            UpdatingSystemCommandRuntimeResult(ctx)
        } finally {
          if(revertAccessModeChange != null ) revertAccessModeChange
        }
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

// The main point of this class is to support the reactive results version of SystemCommandExecutionResult, but return no results in the outer system command
class UpdatingSystemCommandExecutionResult(inner: InternalExecutionResult) extends SystemCommandExecutionResult(inner) {
  override def fieldNames(): Array[String] = Array.empty
}

case class IgnoreResults()

class QueryHandler {
  def onError(t: Throwable): Throwable = t

  def onResult(offset: Int, value: AnyValue): Option[Either[Throwable, IgnoreResults]] = None

  def onNoResults(): Option[Either[Throwable, IgnoreResults]] = None
}

class QueryHandlerBuilder(parent: QueryHandler) extends QueryHandler {
  override def onError(t: Throwable): Throwable = parent.onError(t)

  override def onResult(offset: Int, value: AnyValue): Option[Either[Throwable, IgnoreResults]] = parent.onResult(offset, value)

  override def onNoResults(): Option[Either[Throwable, IgnoreResults]] = parent.onNoResults()

  def handleError(f: Throwable => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onError(t: Throwable): Throwable = f(t)
  }

  def handleNoResult(f: () => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onNoResults(): Option[Either[Throwable, IgnoreResults]] = f().map(t => Left(t))
  }

  def ignoreNoResult(): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onNoResults(): Option[Either[Throwable, IgnoreResults]] = Some(Right(new IgnoreResults))
  }

  def handleResult(handler: (Int, AnyValue) => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onResult(offset: Int, value: AnyValue): Option[Either[Throwable, IgnoreResults]] = handler(offset, value).map(t => Left(t))
  }

  def ignoreOnResult(): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onResult(offset: Int, value: AnyValue): Option[Either[Throwable, IgnoreResults]] = Some(Right(new IgnoreResults))
  }
}

object QueryHandler {
  def handleError(f: Throwable => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleError(f)

  def handleNoResult(f: () => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleNoResult(f)

  def ignoreNoResult(): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).ignoreNoResult()

  def handleResult(handler: (Int, AnyValue) => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleResult(handler)

  def ignoreOnResult(): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).ignoreOnResult()
}
