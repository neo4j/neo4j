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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.result.OperatorProfile
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.ProcedureSignature.VOID
import org.neo4j.kernel.api.ResourceMonitor
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue

abstract class ProfileTimeTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  // We always get OperatorProfile.NO_DATA for page cache hits and misses in Pipelined
  val NO_PROFILE = new OperatorProfile.ConstOperatorProfile(0, 0, 0, 0, 0, OperatorProfile.NO_DATA)

  val NO_PROFILE_NO_TIME = new OperatorProfile.ConstOperatorProfile(
    OperatorProfile.NO_DATA,
    0,
    0,
    OperatorProfile.NO_DATA,
    OperatorProfile.NO_DATA,
    OperatorProfile.NO_DATA
  )

  // time is profiled in nano-seconds, but we can only assert > 0, because the operators take
  // different time on different tested systems.

  test("should profile time of all nodes scan + produce results") {
    givenGraph { nodeGraph(sizeHint) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // all nodes scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with apply") {
    val size = sizeHint / 10
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .skip(size / 4)
      .apply()
      .|.filter("y.prop % 2 = 0")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // skip
    queryProfile.operatorProfile(
      2
    ).time() should be > 0L // apply -  time of the output task of the previous pipeline gets attributed here
    queryProfile.operatorProfile(3).time() should be > 0L // filter
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    queryProfile.operatorProfile(5).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with conditional apply") {
    val size = sizeHint / 10
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .skip(size / 4)
      .conditionalApply("x")
      .|.filter("y.prop % 2 = 0")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // skip
    queryProfile.operatorProfile(2).time() should be > 0L // conditional apply
    queryProfile.operatorProfile(3).time() should be > 0L // filter
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    queryProfile.operatorProfile(5).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with rollup apply") {
    val size = Math.sqrt(sizeHint).toInt
    val (aNodes, bNodes) =
      givenGraph {
        bipartiteGraph(size, "A", "B", "R")
      }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "list")
      .rollUpApply("list", "y")
      .|.argument("y")
      .optionalExpandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // rollup apply
    queryProfile.operatorProfile(2).time() should be > 0L // argument
    queryProfile.operatorProfile(3).time() should be > 0L // optional expand
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with hash join") {
    val size = sizeHint / 10
    givenGraph { nodeGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .nodeHashJoin("x")
      .|.allNodeScan("x")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // hash join
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with value hash join") {
    val size = sizeHint / 10
    givenGraph {
      nodePropertyGraph(
        size,
        {
          case i => Map("prop" -> i)
        }
      )
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .valueHashJoin("x.prop = y.prop")
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // value hash join
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with expand all") {
    val size = sizeHint / 10
    givenGraph { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // expand all
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with var expand") {
    val size = sizeHint / 10
    givenGraph { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expand("(x)-[*1..5]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // var expand
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with pruning var expand") {
    val size = sizeHint / 10
    givenGraph { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .pruningVarExpand("(x)-[*1..5]->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(
      1
    ).time() should be > 0L // pruning var expand (slotted fallback with 1 pipe)
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with shortest path") {
    // TODO: flaky because of fallback and ambient cursors
    assume(!isParallel) // Parallel does not yet support `FindShortestPaths`

    // given
    val nodesPerLabel = 10
    givenGraph {
      for (_ <- 0 until nodesPerLabel)
        sineGraph()
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .shortestPath("(x)-[r*]-(y)", Some("path"))
      .cartesianProduct()
      .|.nodeByLabelScan("y", "END", IndexOrderNone)
      .nodeByLabelScan("x", "START", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // shortest path
    queryProfile.operatorProfile(2).time() should be > 0L // cartesian product
    queryProfile.operatorProfile(3).time() should be > 0L // nodeByLabelScan
    queryProfile.operatorProfile(4).time() should be > 0L // nodeByLabelScan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with expand into") {
    val size = sizeHint / 10
    givenGraph { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .expandInto("(x)-[r]->(y)")
      .expand("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // expand into
    queryProfile.operatorProfile(2).time() should be > 0L // expand
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with optional expand all") {
    // given
    val size = sizeHint / 10
    givenGraph { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // optional expand all
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with optional expand into") {
    // given
    val size = Math.sqrt(sizeHint).toInt
    givenGraph { circleGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .optionalExpandInto("(x)-->(y)")
      .cartesianProduct()
      .|.allNodeScan("y")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // optional expand into
    queryProfile.operatorProfile(2).time() should be > 0L // cartesian product
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    queryProfile.operatorProfile(4).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with optional") {
    val size = sizeHint / 10
    givenGraph { nodeGraph(size) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .optional()
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // optional
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of sort") {
    givenGraph { nodeGraph(sizeHint) }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .sort("x ASC")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // sort
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of cartesian product") {
    val size = Math.sqrt(sizeHint).toInt
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).time() should be > 0L // cartesian product
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan b
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan a
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of union") {
    val size = Math.sqrt(sizeHint).toInt
    givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .union()
      .|.allNodeScan("a")
      .allNodeScan("a")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).time() should be > 0L // union
    queryProfile.operatorProfile(2).time() should be > 0L // all node scan
    queryProfile.operatorProfile(3).time() should be > 0L // all node scan
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time with project endpoints") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, _) = givenGraph { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = aRels.map(r => Array[Any](r))

    // then
    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // project endpoints
    queryProfile.operatorProfile(2).time() should be > 0L // input
  }
}

trait NonParallelProfileTimeTestBase[CONTEXT <: RuntimeContext] {
  self: ProfileTimeTestBase[CONTEXT] =>

  test("should profile time of procedure call") {
    // given
    registerProcedure(new BasicProcedure(
      ProcedureSignature.procedureSignature(Array[String](), "proc").mode(Mode.READ).out(VOID).build()
    ) {
      override def apply(
        ctx: Context,
        input: Array[AnyValue],
        resourceMonitor: ResourceMonitor
      ): RawIterator[Array[AnyValue], ProcedureException] = {
        RawIterator.empty[Array[AnyValue], ProcedureException]()
      }
    })

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("j")
      .procedureCall("proc()")
      .unwind(s"range(0, ${sizeHint - 1}) AS j")
      .argument()
      .build()

    val result = profile(logicalQuery, runtime)
    consume(result)

    // then
    val queryProfile = result.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).time() should be > 0L
    queryProfile.operatorProfile(2).time() should be > 0L // unwind
    queryProfile.operatorProfile(3).time() should be > 0L // argument
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of ordered distinct") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("y")
      .orderedDistinct(Seq("x"), "x AS x", "y AS y")
      .input(nodes = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // orderedDistinct
    queryProfile.operatorProfile(2).time() should be > 0L // input
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of ordered aggregation") {
    // given
    val nodes = givenGraph {
      nodeGraph(sizeHint)
    }

    val input = for (n <- nodes) yield Array[Any](nodes.head, n)

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .orderedAggregation(Seq("x AS x", "y AS y"), Seq("collect(y) AS c"), Seq("x"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input.toSeq: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // orderedAggregation
    queryProfile.operatorProfile(2).time() should be > 0L // input
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of partial sort") {
    val input = for (i <- 0 until sizeHint) yield Array[Any](1, i)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .partialSort(Seq("x ASC"), Seq("y ASC"))
      .input(variables = Seq("x", "y"))
      .build()

    val runtimeResult = profile(logicalQuery, runtime, inputValues(input: _*))
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(0).time() should be > 0L // produce results
    queryProfile.operatorProfile(1).time() should be > 0L // partial sort
    queryProfile.operatorProfile(2).time() should be > 0L // input
    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }

  test("should profile time of triadic selection") {
    // given
    givenGraph { chainGraphs(sizeHint, "A", "B") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y", "z")
      .triadicSelection(positivePredicate = false, "x", "y", "z")
      .|.expandAll("(y)-->(z)")
      .|.argument("x", "y")
      .expandAll("(x)-->(y)")
      .allNodeScan("x")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    val time: Int => Long = queryProfile.operatorProfile(_).time()

    time(0) should be > 0L // produce results
    time(2) should be > 0L // expand
    time(3) should be > 0L // argument
    time(4) should be > 0L // expand
    time(5) should be > 0L // all node scan

    // in pipelined triadic selection is rewritten into build-apply-filter
    (time(1), time(6), time(7), time(8)) should {
      be >= ((1L, 0L, 0L, 0L)) or // triadic selection
        be >= ((0L, 1L, 1L, 1L)) // triadic build, apply, triadic filter
    }

    // Should not attribute anything to the invalid id
    queryProfile.operatorProfile(Id.INVALID_ID.x) should (be(NO_PROFILE) or be(NO_PROFILE_NO_TIME))
  }
}
