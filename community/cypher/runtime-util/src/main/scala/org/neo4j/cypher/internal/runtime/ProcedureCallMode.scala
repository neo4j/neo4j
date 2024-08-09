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

import org.neo4j.collection.ResourceRawIterator
import org.neo4j.cypher.internal.frontend.phases.ProcedureAccessMode
import org.neo4j.cypher.internal.frontend.phases.ProcedureDbmsAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSchemaWriteAccess
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.values.AnyValue

object ProcedureCallMode {

  def fromAccessMode(mode: ProcedureAccessMode): ProcedureCallMode = mode match {
    case ProcedureReadOnlyAccess    => LazyReadOnlyCallMode
    case ProcedureReadWriteAccess   => EagerReadWriteCallMode
    case ProcedureSchemaWriteAccess => SchemaWriteCallMode
    case ProcedureDbmsAccess        => DbmsCallMode
  }

  def eager(iter: ResourceRawIterator[Array[AnyValue], ProcedureException])
    : ResourceRawIterator[Array[AnyValue], ProcedureException] = {
    val builder = Array.newBuilder[Array[AnyValue]]
    while (iter.hasNext) {
      builder += iter.next()
    }
    ResourceRawIterator.of(builder.result(): _*)
  }
}

sealed trait ProcedureCallMode {
  val queryType: InternalQueryType

  def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): ResourceRawIterator[Array[AnyValue], ProcedureException]
}

case object LazyReadOnlyCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_ONLY

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): ResourceRawIterator[Array[AnyValue], ProcedureException] =
    ctx.callReadOnlyProcedure(id, args, context)
}

case object EagerReadWriteCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_WRITE

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): ResourceRawIterator[Array[AnyValue], ProcedureException] =
    ProcedureCallMode.eager(ctx.callReadWriteProcedure(id, args, context))
}

case object SchemaWriteCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = SCHEMA_WRITE

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): ResourceRawIterator[Array[AnyValue], ProcedureException] =
    ProcedureCallMode.eager(ctx.callSchemaWriteProcedure(id, args, context))
}

case object DbmsCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = DBMS

  override def callProcedure(
    ctx: ReadQueryContext,
    id: Int,
    args: Array[AnyValue],
    context: ProcedureCallContext
  ): ResourceRawIterator[Array[AnyValue], ProcedureException] =
    ProcedureCallMode.eager(ctx.callDbmsProcedure(id, args, context))
}
