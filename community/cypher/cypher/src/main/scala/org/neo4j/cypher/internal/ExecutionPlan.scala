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
package org.neo4j.cypher.internal

import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.plandescription.rewrite.InternalPlanDescriptionRewriter
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.ResourceMonitor
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

abstract class ExecutionPlan {

  def run(
    queryContext: QueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    input: InputDataStream,
    subscriber: QuerySubscriber
  ): RuntimeResult

  /**
   * @return if this ExecutionPlan needs a thread safe cursor factory and resource manager factory to be used from the TransactionBoundQueryContext,
   *         then it has to override this method and provide it here.
   */
  def threadSafeExecutionResources(): Option[ResourceManagerFactory] = None

  def runtimeName: RuntimeName

  def metadata: Seq[Argument]

  def operatorMetadata: Id => Seq[Argument] = _ => Seq.empty[Argument]

  def notifications: Set[InternalNotification]

  def rewrittenPlan: Option[LogicalPlan] = None

  def batchSize: Option[Int] = None

  def internalPlanDescriptionRewriter: Option[InternalPlanDescriptionRewriter] = None
}

trait ResourceManagerFactory {
  def apply(monitor: ResourceMonitor): ResourceManager
}

abstract class DelegatingExecutionPlan(inner: ExecutionPlan) extends ExecutionPlan {

  override def run(
    queryContext: QueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    input: InputDataStream,
    subscriber: QuerySubscriber
  ): RuntimeResult =
    inner.run(queryContext, executionMode, params, prePopulateResults, input, subscriber)

  override def runtimeName: RuntimeName = inner.runtimeName

  override def metadata: Seq[Argument] = inner.metadata

  override def notifications: Set[InternalNotification] = inner.notifications
}
