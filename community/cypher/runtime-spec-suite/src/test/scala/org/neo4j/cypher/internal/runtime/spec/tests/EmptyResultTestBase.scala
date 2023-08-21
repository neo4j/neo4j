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

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.TestSubscriber
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType.withName

abstract class EmptyResultTestBase[CONTEXT <: RuntimeContext](
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
      .emptyResult()
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
      .|.emptyResult()
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
      .emptyResult()
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
      .|.emptyResult()
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
      .emptyResult()
      .apply()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x").withNoRows()
  }

  test("should support reduce -> emptyResult on the RHS of apply") {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.emptyResult()
      .|.sort("y ASC")
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should support emptyResult -> reduce on the RHS of apply") {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.sort("y ASC")
      .|.emptyResult()
      .|.expandAll("(x)-->(y)")
      .|.argument()
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "y").withNoRows()
  }

  test("should support chained emptyResult") {
    val nodesPerLabel = 100
    given { bipartiteGraph(nodesPerLabel, "A", "B", "R") }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a2")
      .emptyResult()
      .expandAll("(b2)<--(a2)")
      .emptyResult()
      .expandAll("(a1)-->(b2)")
      .emptyResult()
      .expandAll("(b1)<--(a1)")
      .emptyResult()
      .expandAll("(x)-->(b1)")
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a2").withNoRows()
  }

  test("should support optional expand(into) + emptyResult under apply") {
    given {
      val (aNodes, bNodes) = bipartiteGraph(3, "A", "B", "R")
      for {
        a <- aNodes
        b <- bNodes
      } {
        a.createRelationshipTo(b, withName("R"))
      }
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.emptyResult()
      .|.optionalExpandInto("(a1)-->(b)")
      .|.nonFuseable()
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .nodeByLabelScan("a1", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support unwind + emptyResult under apply") {
    given {
      nodeGraph(sizeHint, "A")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a1")
      .apply()
      .|.emptyResult()
      .|.unwind("[1, 2, 3, 4, 5] AS a2")
      .|.argument()
      .allNodeScan("a1")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a1").withNoRows()
  }

  test("should support chained emptyResult on RHS of Apply") {
    given {
      bipartiteGraph(10, "A", "B", "R")
    }

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .apply()
      .|.emptyResult()
      .|.emptyResult()
      .|.expandAll("(a)-->(b)")
      .|.argument()
      .nodeByLabelScan("a", "A", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("a").withNoRows()
  }

  test("should exhaust input") {
    val inputStream = inputColumns(nBatches = sizeHint / 10, batchSize = 10, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .emptyResult()
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputStream, TestSubscriber.concurrent)

    result.request(1)
    result.await() shouldBe false

    inputStream.hasMore shouldBe false
  }

  test("should exhaust input with aggregation") {
    val inputStream = inputColumns(nBatches = sizeHint / 10, batchSize = 10, identity).stream()

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("sum")
      .emptyResult()
      .aggregation(Seq.empty, Seq("sum(x) as sum"))
      .input(variables = Seq("x"))
      .build()

    val result = execute(logicalQuery, runtime, inputStream, TestSubscriber.concurrent)

    result.request(1)
    result.await() shouldBe false

    inputStream.hasMore shouldBe false
  }

  test("should work on top of complex RHS") {
    val inputRows = (0 until sizeHint).map { i =>
      Array[Any](i.toLong)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .antiSemiApply()
      .|.emptyResult()
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
