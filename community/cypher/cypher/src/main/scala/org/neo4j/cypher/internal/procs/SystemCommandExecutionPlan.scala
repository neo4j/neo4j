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

import org.neo4j.cypher.internal.compatibility.v4_0.ExceptionTranslatingQueryContext
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.result.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundQueryContext
import org.neo4j.cypher.internal.runtime.{InputDataStream, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{ExecutionEngine, ExecutionPlan, RuntimeName, SystemCommandRuntimeName}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

/**
  * Execution plan for performing system commands, i.e. creating or dropping databases.
  */
case class SystemCommandExecutionPlan(name: String, normalExecutionEngine: ExecutionEngine, query: String, systemParams: MapValue)
  extends ExecutionPlan {

  override def run(ctx: QueryContext,
                   doProfile: Boolean,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult = {

    val tc = ctx.asInstanceOf[ExceptionTranslatingQueryContext].inner.asInstanceOf[TransactionBoundQueryContext].transactionalContext.tc
    val newSubscriber = if (subscriber == QuerySubscriber.NOT_A_SUBSCRIBER) CustomSubscriber else subscriber
    val execution = normalExecutionEngine.execute(query, systemParams, tc, doProfile, prePopulateResults, newSubscriber)
    SystemCommandRuntimeResult(ctx, subscriber, execution.asInstanceOf[InternalExecutionResult])
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}

/**
  * A version of NOT_A_SUBSCRIBER that will throw whenever it is being called,
  * either a new error or by passing along occurring errors.
  */
object CustomSubscriber extends QuerySubscriber {
  override def onResult(numberOfFields: Int): Unit = throwError()
  override def onResultCompleted(statistics: QueryStatistics): Unit = throwError()
  override def onRecord(): Unit = throwError()
  override def onRecordCompleted(): Unit = throwError()
  override def onField(offset: Int, value: AnyValue): Unit = throwError()
  override def onError(throwable: Throwable): Unit = {
    val message = throwable.getMessage
    if (message.contains(" already exists with label `Database` and property `name` = "))
      throw new IllegalStateException("Cannot create already existing database")
    else if (message.contains(" already exists with label `Role` and property `name` = "))
      throw new IllegalStateException("Cannot create already existing role")
    else if (message.contains(" already exists with label `User` and property `name` = "))
      throw new IllegalStateException("Cannot create already existing user")
    else
      throw throwable
  }
  override def equals(obj: Any): Boolean = QuerySubscriber.NOT_A_SUBSCRIBER.equals(obj)
  private def throwError(): Unit = {
    throw new UnsupportedOperationException("Invalid operation, can't use this as a subscriber")
  }
}
