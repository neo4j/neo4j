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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InputDataStream, QueryContext, ResourceManager, ResourceMonitor}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.internal.kernel.api.CursorFactory
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

abstract class ExecutionPlan {

  def run(queryContext: QueryContext,
          executionMode: ExecutionMode,
          params: MapValue,
          prePopulateResults: Boolean,
          input: InputDataStream,
          subscriber: QuerySubscriber): RuntimeResult

  /**
    * @return if this ExecutionPlan needs a thread safe cursor factory and resource manager factory to be used from the TransactionBoundQueryContext,
    *         then it has to override this method and provide it here.
    */
  def threadSafeExecutionResources(): Option[(CursorFactory, ResourceManagerFactory)] = None

  def runtimeName: RuntimeName

  def metadata: Seq[Argument]

  def notifications: Set[InternalNotification]
}

trait ResourceManagerFactory {
  def apply(monitor: ResourceMonitor): ResourceManager
}

abstract class DelegatingExecutionPlan(inner: ExecutionPlan) extends ExecutionPlan {
  override def run(queryContext: QueryContext,
                   executionMode: ExecutionMode,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   input: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult =
    inner.run(queryContext, executionMode, params, prePopulateResults, input, subscriber)

  override def runtimeName: RuntimeName = inner.runtimeName

  override def metadata: Seq[Argument] = inner.metadata

  override def notifications: Set[InternalNotification] = inner.notifications
}
