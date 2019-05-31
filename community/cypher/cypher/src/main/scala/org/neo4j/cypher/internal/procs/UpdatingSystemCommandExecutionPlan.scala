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

import java.util

import org.neo4j.cypher.internal.compatibility.v4_0.ExceptionTranslatingQueryContext
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{ExecutionEngine, ExecutionPlan, RuntimeName, SystemCommandRuntimeName}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED
import org.neo4j.kernel.impl.query.{QuerySubscriber, TransactionalContext}
import org.neo4j.values.virtual.MapValue

import scala.collection.JavaConverters._

/**
  * Execution plan for performing system commands, i.e. starting, stopping or dropping databases.
  */
case class UpdatingSystemCommandExecutionPlan(name: String, normalExecutionEngine: ExecutionEngine, query: String, systemParams: MapValue,
                                              queryHandler: QueryHandler, source: Option[ExecutionPlan] = None)
  extends ExecutionPlan {

  override def run(ctx: QueryContext,
                   doProfile: Boolean,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult = {

    val sourceResult = source.map(_.run(ctx,doProfile,params,prePopulateResults,ignore,subscriber))

    val tc: TransactionalContext = ctx.asInstanceOf[ExceptionTranslatingQueryContext].inner.asInstanceOf[TransactionBoundQueryContext].transactionalContext.tc
    if (!tc.securityContext().isAdmin) throw new AuthorizationViolationException(PERMISSION_DENIED)
    var error: Option[Throwable] = None
    try {
      val execution = normalExecutionEngine.execute(query, systemParams, tc, doProfile, prePopulateResults, subscriber).asInstanceOf[InternalExecutionResult]

      val results = execution.javaIterator
      if (results.hasNext) {
        error = results.stream().iterator().asScala.map(queryHandler.onResult(_)).collectFirst{ case Some(e) => e }
      }
      else
        error = queryHandler.onNoResults()
    }
    catch {
      case t: Throwable =>
        throw queryHandler.onError(t)
    }

    error.map(throw _)

    sourceResult.getOrElse(SchemaWriteRuntimeResult(ctx, subscriber))
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

class QueryHandler {
  def onError(t: Throwable): Throwable = throw t

  def onResult(record: util.Map[String, AnyRef]): Option[Throwable] = None

  def onNoResults(): Option[Throwable] = None
}

class QueryHandlerBuilder(parent: QueryHandler) extends QueryHandler {
  override def onError(t: Throwable): Throwable = parent.onError(t)

  override def onResult(record: util.Map[String, AnyRef]): Option[Throwable] = parent.onResult(record)

  override def onNoResults(): Option[Throwable] = parent.onNoResults()

  def handleError(f: Throwable => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onError(t: Throwable): Throwable = f(t)
  }

  def handleNoResult(f: () => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onNoResults(): Option[Throwable] = f()
  }

  def handleResult(handler: util.Map[String, AnyRef] => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(this) {
    override def onResult(record: util.Map[String, AnyRef]): Option[Throwable] = handler(record)
  }
}

object QueryHandler {
  def handleError(f: Throwable => Throwable): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleError(f)

  def handleNoResult(f: () => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleNoResult(f)

  def handleResult(handler: util.Map[String, AnyRef] => Option[Throwable]): QueryHandlerBuilder = new QueryHandlerBuilder(new QueryHandler).handleResult(handler)
}
