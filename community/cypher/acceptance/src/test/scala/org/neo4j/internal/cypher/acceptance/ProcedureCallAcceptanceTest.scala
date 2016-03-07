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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.collection.RawIterator
import org.neo4j.cypher._
import org.neo4j.kernel.api.KernelAPI
import org.neo4j.kernel.api.exceptions.ProcedureException
import org.neo4j.kernel.api.proc.CallableProcedure.{BasicProcedure, Context}
import org.neo4j.kernel.api.proc.ProcedureSignature._
import org.neo4j.kernel.api.proc.{Neo4jTypes, ProcedureSignature}

abstract class ProcedureCallAcceptanceTest extends ExecutionEngineFunSuite {

  protected def registerDummyProc(types: Neo4jTypes.AnyType*) = {
    val builder = procedureSignature(Array("my", "first"), "proc")

    for (i <- types.indices) {

      builder
        .in(s"in$i", types(i))
        .out(s"out$i", types(i))
    }

    val proc = new BasicProcedure(builder.build) {
      override def apply(ctx: Context, input: Array[AnyRef]): RawIterator[Array[AnyRef], ProcedureException] =
        RawIterator.of[Array[AnyRef], ProcedureException](input)
    }
    kernel.registerProcedure(proc)
  }

  protected def registerValueProc(value: AnyRef) = {
    val builder = procedureSignature(Array("my", "first"), "value")
    builder.out("out", Neo4jTypes.NTAny)

    val proc = new BasicProcedure(builder.build) {
      override def apply(ctx: Context, input: Array[AnyRef]): RawIterator[Array[AnyRef], ProcedureException] =
        RawIterator.of[Array[AnyRef], ProcedureException](Array(value))
    }
    kernel.registerProcedure(proc)
  }

  protected def registerVoidProc() = {
    val builder = procedureSignature(Array("sys"), "do_nothing")
    builder.out(ProcedureSignature.VOID)

    val proc = new BasicProcedure(builder.build) {
      override def apply(ctx: Context, input: Array[AnyRef]): RawIterator[Array[AnyRef], ProcedureException] =
        RawIterator.empty()
    }
    kernel.registerProcedure(proc)
  }

  override protected def initTest() {
    super.initTest()
    kernel = graph.getDependencyResolver.resolveDependency(classOf[KernelAPI])
  }

  protected var kernel: KernelAPI = null
}
