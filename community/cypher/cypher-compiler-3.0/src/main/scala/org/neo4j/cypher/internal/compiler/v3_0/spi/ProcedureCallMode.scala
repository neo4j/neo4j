/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0.spi

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{DBMS, InternalQueryType, READ_ONLY, READ_WRITE}

import scala.collection.mutable.ArrayBuffer

sealed trait ProcedureCallMode {
  val queryType: InternalQueryType

  def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]]
}

case object LazyReadOnlyCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_ONLY

  override def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]] =
    ctx.callReadOnlyProcedure(signature.name, args)
}

case object EagerReadWriteCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_WRITE

  override def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    val iterator = ctx.callReadWriteProcedure(signature.name, args)
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }
}

case object DbmsCallMode extends ProcedureCallMode {
  override val queryType: InternalQueryType = DBMS

  override def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]] =
    ctx.callDbmsProcedure(signature.name, args)
}
