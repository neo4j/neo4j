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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.logical.plans.ProcedureAccessMode
import org.neo4j.cypher.internal.logical.plans.ProcedureDbmsAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSchemaWriteAccess
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.values.AnyValue

import scala.collection.mutable.ArrayBuffer

object ProcedureCallMode {

  def fromAccessMode(mode: ProcedureAccessMode): ProcedureCallMode = mode match {
    case ProcedureReadOnlyAccess    => LazyReadOnlyCallMode
    case ProcedureReadWriteAccess   => EagerReadWriteCallMode
    case ProcedureSchemaWriteAccess => SchemaWriteCallMode
    case ProcedureDbmsAccess        => DbmsCallMode
  }
}

sealed trait ProcedureCallMode {
  val queryType: InternalQueryType

  def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]]
}

case object LazyReadOnlyCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_ONLY

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    ctx.callReadOnlyProcedure(id, args, context)
}

case object EagerReadWriteCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_WRITE

  private def call(iterator: Iterator[Array[AnyValue]]) = {
    val builder = ArrayBuffer.newBuilder[Array[AnyValue]]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    call(ctx.callReadWriteProcedure(id, args, context))
}

case object SchemaWriteCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = SCHEMA_WRITE

  private def call(iterator: Iterator[Array[AnyValue]]) = {
    val builder = ArrayBuffer.newBuilder[Array[AnyValue]]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    call(ctx.callSchemaWriteProcedure(id, args, context))
}

case object DbmsCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = DBMS

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): Iterator[Array[AnyValue]] =
    call(ctx.callDbmsProcedure(id, args, context))

  private def call(iterator: Iterator[Array[AnyValue]]) = {
    val builder = ArrayBuffer.newBuilder[Array[AnyValue]]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }
}
