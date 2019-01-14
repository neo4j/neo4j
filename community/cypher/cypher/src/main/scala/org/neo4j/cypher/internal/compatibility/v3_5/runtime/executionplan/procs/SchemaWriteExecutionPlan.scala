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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.procs

import org.neo4j.cypher.internal.compatibility.v3_5.runtime._
import org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan.ExecutionPlan
import org.neo4j.cypher.internal.runtime._
import org.neo4j.cypher.internal.runtime.interpreted.UpdateCountingQueryContext
import org.neo4j.cypher.internal.runtime.planDescription.Argument
import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.values.virtual.MapValue
import org.neo4j.cypher.internal.v3_5.util.InternalNotification

/**
  * Execution plan for performing schema writes, i.e. creating or dropping indexes and constraints.
  *
  * @param name        A name of the schema write
  * @param schemaWrite The actual schema write to perform
  */
case class SchemaWriteExecutionPlan(name: String, schemaWrite: QueryContext => Unit)
  extends ExecutionPlan {

  override def run(ctx: QueryContext, doProfile: Boolean, params: MapValue): RuntimeResult = {

    ctx.assertSchemaWritesAllowed()

    val countingCtx = new UpdateCountingQueryContext(ctx)
    schemaWrite(countingCtx)
    ctx.transactionalContext.close(success = true)
    val runtimeResult = SchemaWriteRuntimeResult(countingCtx)
    runtimeResult
  }

  override def runtimeName: RuntimeName = ProcedureRuntimeName

  override def metadata: Seq[Argument] = Nil

  override def notifications: Set[InternalNotification] = Set.empty
}
