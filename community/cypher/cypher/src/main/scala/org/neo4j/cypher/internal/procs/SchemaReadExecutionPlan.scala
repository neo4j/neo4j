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

import org.neo4j.cypher.internal.runtime.ExecutionMode
import org.neo4j.cypher.internal.runtime.InputDataStream
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.kernel.impl.query.QuerySubscriber
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

/**
 * Execution plan for performing schema reads, i.e. showing indexes and constraints.
 *
 * @param name       A name of the schema read
 * @param schemaRead The actual schema read to perform
 */
case class SchemaReadExecutionPlan(name: String, assertType: AssertType, schemaRead: QueryContext => SchemaReadExecutionResult)
  extends SchemaCommandChainedExecutionPlan(None) {

  override def runSpecific(ctx: UpdateCountingQueryContext,
                           executionMode: ExecutionMode,
                           params: MapValue,
                           prePopulateResults: Boolean,
                           ignore: InputDataStream,
                           subscriber: QuerySubscriber): RuntimeResult = {

    assertType match {
      case AssertIndex      => ctx.assertShowIndexAllowed()
      case AssertConstraint => ctx.assertShowConstraintAllowed()
    }
    ctx.transactionalContext.close()
    val schemaReadResult = schemaRead(ctx)
    val runtimeResult = SchemaReadRuntimeResult(ctx, subscriber, schemaReadResult.columnNames, schemaReadResult.result)
    runtimeResult
  }
}

case class SchemaReadExecutionResult(columnNames: Array[String], result: List[Map[String, AnyValue]])

sealed trait AssertType
case object AssertIndex extends AssertType
case object AssertConstraint extends AssertType
