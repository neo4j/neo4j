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
import org.neo4j.cypher.internal.RuntimeName
import org.neo4j.cypher.internal.SchemaRuntimeName
import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.util.InternalNotification
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.virtual.MapValue

/**
 * Execution plan for performing schema writes, i.e. creating or dropping indexes and constraints.
 *
 * @param name        A name of the schema write
 * @param schemaWrite The actual schema write to perform
 */
case class SchemaExecutionPlan(
  name: String,
  schemaWrite: (QueryContext, MapValue) => SchemaExecutionResult,
  source: Option[ExecutionPlan] = None
) extends ChainedExecutionPlan[UpdateCountingQueryContext](source) {
  override def createContext(originalCtx: QueryContext) = new UpdateCountingQueryContext(originalCtx)
  override def querySubscriber(context: UpdateCountingQueryContext, qs: QuerySubscriber): QuerySubscriber = qs

  override def runtimeName: RuntimeName = SchemaRuntimeName

  override def runSpecific(
    ctx: UpdateCountingQueryContext,
    executionMode: ExecutionMode,
    params: MapValue,
    prePopulateResults: Boolean,
    subscriber: QuerySubscriber,
    previousNotifications: Set[InternalNotification]
  ): RuntimeResult = {

    ctx.assertSchemaWritesAllowed()

    schemaWrite(ctx, params) match {
      case SuccessResult(notifications) =>
        // TODO: Do we really need to close transactional context here?
        ctx.transactionalContext.close()
        val runtimeResult = SchemaRuntimeResult(ctx, subscriber, previousNotifications ++ notifications)
        runtimeResult
      case IgnoredResult(notifications) =>
        IgnoredRuntimeResult(previousNotifications ++ notifications)
    }
  }
}

sealed trait SchemaExecutionResult {
  def notifications: Set[InternalNotification]
}

case class SuccessResult(notifications: Set[InternalNotification] = Set.empty) extends SchemaExecutionResult

case class IgnoredResult(notifications: Set[InternalNotification] = Set.empty) extends SchemaExecutionResult
