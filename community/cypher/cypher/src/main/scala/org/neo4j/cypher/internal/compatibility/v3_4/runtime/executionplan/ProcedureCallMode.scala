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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.executionplan

import org.neo4j.cypher.internal.spi.v3_4.QueryContext
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

  def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]]

  val allowed: Array[String]
}

case class LazyReadOnlyCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_ONLY

  override def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]] =
    ctx.callReadOnlyProcedure(name, args, allowed)
}

case class EagerReadWriteCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = READ_WRITE

  override def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    val iterator = ctx.callReadWriteProcedure(name, args, allowed)
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }
}

case class SchemaWriteCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = SCHEMA_WRITE

  override def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    val iterator = ctx.callSchemaWriteProcedure(name, args, allowed)
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }
}

case class DbmsCallMode(allowed: Array[String]) extends ProcedureCallMode {
  override val queryType: InternalQueryType = DBMS

  override def callProcedure(ctx: QueryContext, name: QualifiedName, args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val builder = ArrayBuffer.newBuilder[Array[AnyRef]]
    val iterator = ctx.callDbmsProcedure(name, args, allowed)
    while (iterator.hasNext) {
      builder += iterator.next()
    }
    builder.result().iterator
  }
}
