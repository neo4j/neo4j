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

import org.neo4j.cypher.internal.plandescription.Argument
import org.neo4j.cypher.internal.runtime.{ExecutionMode, InputDataStream, QueryContext}
import org.neo4j.cypher.internal.v4_0.util.InternalNotification
import org.neo4j.cypher.internal.{ExecutionPlan, RuntimeName, SystemCommandRuntimeName}
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.query.{QuerySubscriber, QuerySubscriberAdapter}
import org.neo4j.values.virtual.MapValue

/**
 * System commands are broken down into a linear chain of sub-commands. The outermost (or last) command
 * will be passed the original QuerySubscriber (coming from the BOLT server) and this can be tied into
 * the reactive-results system. For this reason it is important to make sure that outer subscriber is
 * treated correctly for reactive results. The inner commands can instead be passed simple QuerySubscribers
 * that either do nothing or simply track the existence of database changes in order to keep a count of inner
 * commands.
 */
abstract class ChainedExecutionPlan(source: Option[ExecutionPlan]) extends ExecutionPlan {
  def runSpecific(ctx: SystemUpdateCountingQueryContext,
                   executionMode: ExecutionMode,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult

  override def run(originalCtx: QueryContext,
                   executionMode: ExecutionMode,
                   params: MapValue,
                   prePopulateResults: Boolean,
                   ignore: InputDataStream,
                   subscriber: QuerySubscriber): RuntimeResult = {
    val ctx = SystemUpdateCountingQueryContext.from(originalCtx)
    // Only the outermost query should be tied into the reactive results stream. The source queries use a simplified counting subscriber
    val sourceResult = source.map(_.run(ctx, executionMode, params, prePopulateResults, ignore, new QuerySubscriberAdapter() {
      override def onResultCompleted(statistics: QueryStatistics): Unit = if (statistics.containsUpdates()) ctx.systemUpdates.increase()
    }))
    sourceResult match {
      case Some(IgnoredRuntimeResult) =>
        // When an operation in the chain switches the entire chain to ignore mode we still need to notify the outer most subscriber
        // This is a no-op for all elements of the chain except the last (outermost) which will be the BoltAdapterSubscriber
        subscriber.onResultCompleted(ctx.getStatistics)
        IgnoredRuntimeResult
      case _ =>
        runSpecific(ctx, executionMode, params, prePopulateResults, ignore, subscriber)
    }
  }

  override def runtimeName: RuntimeName = SystemCommandRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}
