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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.v3_4.logical.plans._

import scala.collection.mutable.ArrayBuffer

object ProcedureCallMode {
  def fromAccessMode(mode: ProcedureAccessMode): ProcedureCallMode = mode match {
    case ProcedureReadOnlyAccess(overrides) => LazyReadOnlyCallMode(overrides)
    case ProcedureReadWriteAccess(overrides) => EagerReadWriteCallMode(overrides)
    case ProcedureSchemaWriteAccess(overrides) => SchemaWriteCallMode(overrides)
    case ProcedureDbmsAccess(overrides) => DbmsCallMode(overrides)
  }
}
sealed trait ProcedureCallMode {
  val queryType: InternalQueryType

  def callProcedure(ctx: QueryContext, id: Int, args: Seq[Any]): Iterator[Array[AnyRef]]
  def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]]

  val allowed: Array[String]
}

case class LazyReadOnlyCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_ONLY

  override def callProcedure(ctx: QueryContext, id: Int, args: Seq[Any]): Iterator[Array[AnyRef]] =
    ctx.callReadOnlyProcedure(id, args, allowed)

  override def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]] =
    ctx.callReadOnlyProcedure(name, args, allowed)
}

case class EagerReadWriteCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_WRITE

  private def call(iterator: Iterator[Array[AnyRef]]) = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }

  override def callProcedure(ctx: QueryContext, id: Int, args: Seq[Any]): Iterator[Array[AnyRef]] = call(ctx.callReadWriteProcedure(id, args, allowed))

  override def callProcedure(ctx: QueryContext,
                             name: QualifiedName,
                             args: Seq[Any]): Iterator[Array[AnyRef]] = call(ctx.callReadWriteProcedure(name, args, allowed))
}

case class SchemaWriteCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = SCHEMA_WRITE

  private def call(iterator: Iterator[Array[AnyRef]]) = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }

  override def callProcedure(ctx: QueryContext, id: Int, args: Seq[Any]): Iterator[Array[AnyRef]] = call(ctx
                                                                                                           .callSchemaWriteProcedure(
                                                                                                             id, args,
                                                                                                             allowed))

  override def callProcedure(ctx: QueryContext,
                             name: QualifiedName,
                             args: Seq[Any]): Iterator[Array[AnyRef]] =  call(ctx.callSchemaWriteProcedure(name, args, allowed))
}

case class DbmsCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = DBMS

  override def callProcedure(ctx: QueryContext, id: Int, args: Seq[Any]): Iterator[Array[AnyRef]] =
    call(ctx.callDbmsProcedure(id, args, allowed))

  override def callProcedure(ctx: QueryContext,
                             name: QualifiedName,
                             args: Seq[Any]): Iterator[Array[AnyRef]] =
    call(ctx.callDbmsProcedure(name, args, allowed))


  private def call(iterator: Iterator[Array[AnyRef]]) = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }
}
