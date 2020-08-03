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
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue.ntAny
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
    },

    new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "cardinalityIncreasingProc").mode(Mode.READ).in("j", Neo4jTypes.NTInteger).out("i", Neo4jTypes.NTInteger).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {

        val nElemants = input.head.asInstanceOf[NumberValue].longValue().intValue()
        RawIterator.of[Array[AnyValue], ProcedureException]((1 to nElemants).map(i => Array[AnyValue](Values.intValue(i))): _*)
      }
    },

    new BasicProcedure(ProcedureSignature.procedureSignature(Array[String](), "echoProc").mode(Mode.READ).in("j", Neo4jTypes.NTAny, ntAny("default")).out("i", Neo4jTypes.NTAny).build()) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] = {
        RawIterator.of[Array[AnyValue], ProcedureException](input)
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

  test("should call echo procedure") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .procedureCall("echoProc(x) YIELD i AS i")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n))
    runtimeResult should beColumns("x", "i").withRows(expected)
  }

  test("should call echo procedure with default argument") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .procedureCall("echoProc() YIELD i AS i")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, "default"))
    runtimeResult should beColumns("x", "i").withRows(expected)
  }

  test("should call cardinality increasing procedure") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i")
      .procedureCall("cardinalityIncreasingProc(5) YIELD i AS i")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {n <- nodes
                        i <- 1 to 5} yield Array(n, i)

    runtimeResult should beColumns("x", "i").withRows(expected)
  }

  test("should work on rhs of apply") {
    // given
    val nodes = given { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n", "i")
      .apply()
      .|.filter(s"i < 10")
      .|.procedureCall(s"cardinalityIncreasingProc($sizeHint) YIELD i AS i")
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {n <- nodes
                        i <- 1 until 10} yield Array(n, i)
    runtimeResult should beColumns("n", "i").withRows(expected)
  }

  test("should call cardinality increasing procedure twice") {
    // given
    val nodes = given {
      nodeGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "i1", "i2")
      .procedureCall("cardinalityIncreasingProc(3) YIELD i AS i2")
      .procedureCall("cardinalityIncreasingProc(7) YIELD i AS i1")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {n <- nodes
                        i1 <- 1 to 7
                        i2 <- 1 to 3} yield Array(n, i1, i2)

    runtimeResult should beColumns("x", "i1", "i2").withRows(expected)
  }

  test("should profile rows with procedureCall and expand") {
    // given
    val nodesPerLabel = 20
    val procedureCallCardinality = 7
    given {
      bipartiteGraph(nodesPerLabel, "A", "B", "R")
    }
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.nonFuseable()
      .|.expandAll("(x)-->(y)")
      .|.procedureCall(s"cardinalityIncreasingProc(${procedureCallCardinality}) YIELD i AS i")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).rows() shouldBe (nodesPerLabel * nodesPerLabel * procedureCallCardinality) // produce results
    queryProfile.operatorProfile(1).rows() shouldBe (nodesPerLabel * nodesPerLabel * procedureCallCardinality) // apply
    queryProfile.operatorProfile(2).rows() shouldBe (nodesPerLabel * nodesPerLabel * procedureCallCardinality) // non-fuseable
    queryProfile.operatorProfile(3).rows() shouldBe (nodesPerLabel * nodesPerLabel * procedureCallCardinality) // expand all
    queryProfile.operatorProfile(4).rows() shouldBe (nodesPerLabel * 2L * procedureCallCardinality) // procedure call
    queryProfile.operatorProfile(5).rows() shouldBe (nodesPerLabel * 2L) // argument
    queryProfile.operatorProfile(6).rows() shouldBe (nodesPerLabel * 2L) // all node scan
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