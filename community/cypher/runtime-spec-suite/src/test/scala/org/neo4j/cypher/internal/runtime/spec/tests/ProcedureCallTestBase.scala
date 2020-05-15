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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.ProcedureSignature.VOID
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NumberValue
import org.neo4j.values.storable.Values

abstract class ProcedureCallTestBase[CONTEXT <: RuntimeContext](
                                                                 edition: Edition[CONTEXT],
                                                                 runtime: CypherRuntime[CONTEXT],
                                                                 val sizeHint: Int
                                                               ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  private var testVar = 0

  private val procedures = Seq(
    new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "readVoidProc").mode(Mode.READ).out(VOID).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
        testVar += 1
        RawIterator.empty[Array[AnyValue], ProcedureException]()
      }
    },

    new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "writeVoidProc").mode(Mode.WRITE).out(VOID).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
        ctx.graphDatabaseAPI().executeTransactionally("CREATE (n:INPROC)")
        RawIterator.empty[Array[AnyValue], ProcedureException]()
      }
    },


    new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "readIntProc").mode(Mode.READ).out("i", Neo4jTypes.NTInteger).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
        testVar += 1
        RawIterator.of[Array[AnyValue], ProcedureException](Array(Values.of(testVar)), Array(Values.of(testVar)))
      }
    },


    new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "readIntIntProc").mode(Mode.READ).in("j", Neo4jTypes.NTInteger).out("i", Neo4jTypes.NTInteger).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
        def twice(v: AnyValue): AnyValue = v.asInstanceOf[NumberValue].times(2L)
        RawIterator.of[Array[AnyValue], ProcedureException](input.map(twice), input.map(twice))
      }
    }
  )

  override protected def initTest(): Unit = {
    testVar = 0
    procedures.foreach(registerProcedure)
  }

  test("should call read void procedure") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .procedureCall("readVoidProc()")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))
    testVar should be(sizeHint)
  }

  test("should call read int procedure") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .procedureCall("readIntProc() YIELD i AS i")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.zipWithIndex.flatMap { case (n, i) => Seq(Array(n, i + 1), Array(n, i + 1)) }
    runtimeResult should beColumns("x", "i").withRows(expected)
    testVar should be(sizeHint)
  }

  test("should call read int->int procedure") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("j", "i")
      .procedureCall("readIntIntProc(j) YIELD i AS i")
      .unwind(s"range(0, $sizeHint) AS j")
      .argument()
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (0 to sizeHint).flatMap { j => Seq(Array(j, j * 2), Array(j, j * 2)) }
    runtimeResult should beColumns("j", "i").withRows(expected)
  }
}

trait WriteProcedureCallTestBase[CONTEXT <: RuntimeContext] {
  self: ProcedureCallTestBase[CONTEXT] =>

  test("should call write void procedure") {
    // given
    val nodes = given {
      nodeGraph(sizeHint, "OUTPROC")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .procedureCall("writeVoidProc()")
      .nodeByLabelScan("x", "OUTPROC", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(nodes))

    // and when
    val verificationQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeByLabelScan("x", "INPROC", IndexOrderNone)
      .build()

    val verificationResult = execute(verificationQuery, runtime)

    // then
    verificationResult should beColumns("x").withRows(rowCount(sizeHint))
  }
}