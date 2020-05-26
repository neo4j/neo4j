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
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.TransientFailureException
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.impl.query.QuerySubscriber
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
                                              source: Option[ExecutionPlan] = None,
                                              checkCredentialsExpired: Boolean = true,
                                              initFunction: (MapValue, KernelTransaction) => Boolean = (_, _) => true,
                                              finallyFunction: MapValue => Unit = _ => {},
                                              parameterGenerator: (Transaction, SecurityContext) => MapValue = (_, _) => MapValue.EMPTY,
                                              parameterConverter: (Transaction, MapValue) => MapValue = (_, p) => p,
                                              assertPrivilegeAction: Transaction => Unit = _ => {})
  extends ChainedExecutionPlan(source) {

  override def runSpecific(ctx: SystemUpdateCountingQueryContext,
                           executionMode: ExecutionMode,
                           params: MapValue,
                           prePopulateResults: Boolean,
                           ignore: InputDataStream,
                           subscriber: QuerySubscriber): RuntimeResult = {

    val tc = ctx.kernelTransactionalContext

    var revertAccessModeChange: KernelTransaction.Revertable = null
    try {
      val securityContext = tc.securityContext()
      if (checkCredentialsExpired) securityContext.assertCredentialsNotExpired()
      val fullAccess = securityContext.withMode(AccessMode.Static.FULL)
      revertAccessModeChange = tc.kernelTransaction().overrideWith(fullAccess)
      val tx = tc.transaction()
      assertPrivilegeAction(tx)

      val updatedParams = parameterConverter(tx, safeMergeParameters(systemParams, params, parameterGenerator.apply(tx, securityContext)))
      val systemSubscriber = new SystemCommandQuerySubscriber(ctx, new RowDroppingQuerySubscriber(subscriber), queryHandler, updatedParams)
      try {
        tc.kernelTransaction().dataWrite() // assert that we are allowed to write
      } catch {
        case e: Throwable =>
          systemSubscriber.onError(e)
      }
      systemSubscriber.assertNotFailed()
      try {
        if (initFunction(updatedParams, tc.kernelTransaction())) {
          val execution = normalExecutionEngine.executeSubQuery(query, updatedParams, tc, isOutermostQuery = false, executionMode == ProfileMode, prePopulateResults, systemSubscriber).asInstanceOf[InternalExecutionResult]
          try {
            execution.consumeAll()
          } catch {
            case _: Throwable =>
            // do nothing, exceptions are handled by SystemCommandQuerySubscriber
          }
          systemSubscriber.assertNotFailed()

          if (systemSubscriber.shouldIgnoreResult()) {
            IgnoredRuntimeResult
          } else {
            UpdatingSystemCommandRuntimeResult(ctx)
          }
        } else {
          UpdatingSystemCommandRuntimeResult(ctx)
        }
      } finally {
        finallyFunction(updatedParams)
      }
    } finally {
      if (revertAccessModeChange != null) revertAccessModeChange
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
  def onError(t: Throwable, p: MapValue): Throwable = t

  def onResult(offset: Int, value: AnyValue, p: MapValue): Option[Either[Throwable, IgnoreResults]] = None

  def onNoResults(p: MapValue): Option[Either[Throwable, IgnoreResults]] = None
}

class QueryHandlerBuilder(parent: QueryHandler) extends QueryHandler {
  override def onError(t: Throwable, p: MapValue): Throwable = parent.onError(t, p)

  override def onResult(offset: Int, value: AnyValue, params: MapValue): Option[Either[Throwable, IgnoreResults]] = parent.onResult(offset, value, params)

  override def onNoResults(params: MapValue): Option[Either[Throwable, IgnoreResults]] = parent.onNoResults(params)

  def handleError(f: (Throwable, MapValue) => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onError(t: Throwable, p: MapValue): Throwable = t match {
      case t: TransientFailureException => t
      case _ => f(t, p)
    }
  }

  def handleNoResult(f: MapValue => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onNoResults(params: MapValue): Option[Either[Throwable, IgnoreResults]] = f(params).map(t => Left(t))
  }

  def ignoreNoResult(): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onNoResults(params: MapValue): Option[Either[Throwable, IgnoreResults]] = Some(Right(IgnoreResults()))
  }

  def handleResult(handler: (Int, AnyValue, MapValue) => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onResult(offset: Int, value: AnyValue, p: MapValue): Option[Either[Throwable, IgnoreResults]] = handler(offset, value, p).map(t => Left(t))
  }

  def ignoreOnResult(): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onResult(offset: Int, value: AnyValue, p: MapValue): Option[Either[Throwable, IgnoreResults]] = Some(Right(IgnoreResults()))
  }
}

object QueryHandler {
  def handleError(f: (Throwable, MapValue) => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleError(f)

  def handleNoResult(f: MapValue => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleNoResult(f)

  def ignoreNoResult(): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).ignoreNoResult()

  def handleResult(handler: (Int, AnyValue, MapValue) => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleResult(handler)

  def ignoreOnResult(): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).ignoreOnResult()
}
