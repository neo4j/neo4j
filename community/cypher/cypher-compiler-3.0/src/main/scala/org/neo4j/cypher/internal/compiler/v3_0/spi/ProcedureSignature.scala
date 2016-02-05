/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.{InternalQueryType, READ_ONLY, READ_WRITE}
import org.neo4j.cypher.internal.frontend.v3_0.symbols.CypherType

import scala.collection.JavaConverters._

sealed trait ProcedureMode {
  val queryType: InternalQueryType

  def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]]
}

case object ProcReadOnly extends ProcedureMode {
  override val queryType: InternalQueryType = READ_ONLY

  override def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]] =
    ctx.callReadOnlyProcedure(signature, args)
}

case object ProcReadWrite extends ProcedureMode {
  override val queryType: InternalQueryType = READ_WRITE

  override def call(ctx: QueryContext, signature: ProcedureSignature, args: Seq[Any]): Iterator[Array[AnyRef]] = {
    val buffer = new util.ArrayList[Array[AnyRef]]()
    val iterator = ctx.callReadWriteProcedure(signature, args)
    while (iterator.hasNext) {
      buffer.add(iterator.next())
    }
    buffer.iterator().asScala
  }
}

case class ProcedureSignature(name: ProcedureName,
                              inputSignature: Seq[FieldSignature],
                              outputSignature: Seq[FieldSignature],
                              mode: ProcedureMode = ProcReadOnly)

case class ProcedureName(namespace: Seq[String], name: String)

case class FieldSignature(name: String, typ: CypherType)
