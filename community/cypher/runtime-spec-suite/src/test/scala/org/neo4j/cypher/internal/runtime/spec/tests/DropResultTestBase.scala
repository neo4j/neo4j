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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType.withName

abstract class DropResultTestBase[CONTEXT <: RuntimeContext](
                                                              edition: Edition[CONTEXT],
                                                              runtime: CypherRuntime[CONTEXT],
                                                              sizeHint: Int
                                                            ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should not produce any rows") {
    given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .limit(0)
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should not produce any rows on RHS of apply") {
    given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.limit(0)
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withNoRows()
  }

  test("should work with aggregation") {
    given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("c")
      .aggregation(Seq.empty, Seq("count(n) as c"))
      .limit(0)
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("c").withSingleRow(0)
  }

  test("should let through all LHS rows of antiSemiApply") {
    val nodes = given {
      nodeGraph(sizeHint)
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .antiSemiApply()
      .|.limit(0)
      .|.argument("n")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n").withRows(nodes.map(Array[Any](_)))
  }

  test("should work on top of apply") {
    val nodesPerLabel = 50
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(0)
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should support reduce -> limit 0on the RHS of apply") {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.limit(0)
      .|.sort(Seq(Ascending("y")))
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should support limit 0-> reduce on the RHS of apply") {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort(Seq(Ascending("y")))
      .|.limit(0)
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should support chained limit"0) {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2")
      .limit(0)
      .expandAll("(b2)<--(a2)")
      .limit(0)
      .expandAll("(a1)-->(b2)")
      .limit(0)
      .expandAll("(b1)<--(a1)")
      .limit(0)
      .expandAll("(x)-->(b1)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withNoRows()
  }

  test("should support optional expand(into) + limit 0under apply") {
    given {
      val (aNodes, bNodes) = bipartiteGraph(3, "A", "B", "R")
      for {a <- aNodes
           b <- bNodes} {
        a.createRelationshipTo(b, withName("R"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(0)
      .|.optionalExpandInto("(a1)-->(b)")
      .|.nonFuseable()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support unwind + limit 0under apply") {
    given {
      nodeGraph(sizeHint, "A")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.limit(0)
      .|.unwind("[1, 2, 3, 4, 5] AS a2")
      .|.argument()
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support chained limit 0on RHS of Apply") {
    given {
      bipartiteGraph(10, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.limit(0)
      .|.limit(0)
      .|.expandAll("(a)-->(b)")
      .|.argument()
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a").withNoRows()
  }

  test("should not exhaust input") {
    val inputStream = inputColumns(nBatches = sizeHint / 10, batchSize = 10, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .limit(0)
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputStream, TestSubscriber.concurrent)

    result.request(Long.MaxValue)
    result.await() shouldBe false

    inputStream.hasMore shouldBe true
  }

  test("should not exhaust input with aggregation") {
    val inputStream = inputColumns(nBatches = sizeHint / 10, batchSize = 10, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("sum")
      .limit(0)
      .aggregation(Seq.empty, Seq("sum(x) as sum"))
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputStream, TestSubscriber.concurrent)

    result.request(Long.MaxValue)
    result.await() shouldBe false

    inputStream.hasMore shouldBe true
  }

  test("should work on top of complex RHS") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.limit(0)
      .|.cartesianProduct()
      .|.|.argument("x")
      .|.argument("x")
      .input(variables = Seq("x"))
      .build()

    // then
    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputRows: _*))
    runtimeResult should beColumns("x").withRows(inputRows)
  }

}
