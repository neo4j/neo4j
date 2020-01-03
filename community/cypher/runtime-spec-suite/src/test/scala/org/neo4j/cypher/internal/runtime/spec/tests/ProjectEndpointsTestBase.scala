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

import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.cypher.internal.{CypherRuntime, RuntimeContext}

abstract class ProjectEndpointsTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected  val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should project endpoints - directed - start in scope") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes) = given { bipartiteGraph(nNodes, "A", "B", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]->(y)", startInScope = true, endInScope = false)
      .expandAll("(x)-[r]->()")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      a <- aNodes
      b <- bNodes
    } yield {
      Array(a, b)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints - undirected - start in scope") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, _, _) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "RA", "RB") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]-(y)", startInScope = true, endInScope = false)
      .expandAll("(x)-[r]->()")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints - directed - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, _) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]->(y)", startInScope = false, endInScope = false)
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, aRels.map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = for {
      a <- aNodes
      b <- bNodes
    } yield {
      Array(a, b)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints - undirected - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, _) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, aRels.map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints with hash join under apply - directed - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, bRels) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.nodeHashJoin("x")
      .|.|.projectEndpoints("(x)<-[r]-(y)", startInScope = false, endInScope = false)
      .|.|.argument("r")
      .|.projectEndpoints("(x)-[r]->(y)", startInScope = false, endInScope = false)
      .|.argument("r")
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, (aRels ++ bRels).map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(rowCount(expected.size))
  }

  test("should project endpoints with hash join under apply - undirected/directed - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, bRels) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.nodeHashJoin("x", "y")
      .|.|.projectEndpoints("(x)<-[r]-(y)", startInScope = false, endInScope = false)
      .|.|.argument("r")
      .|.projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .|.argument("r")
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, (aRels ++ bRels).map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints with hash join under apply - directed/undirected - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, bRels) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .apply()
      .|.nodeHashJoin("x", "y")
      .|.|.projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false)
      .|.|.argument("r")
      .|.projectEndpoints("(x)-[r]->(y)", startInScope = false, endInScope = false)
      .|.argument("r")
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, (aRels ++ bRels).map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints - chained directed - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, bRels) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)<-[r]-(y)", startInScope = true, endInScope = true)   // chained middle
      .projectEndpoints("(x)-[r]->(y)", startInScope = false, endInScope = false) // middle
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, (aRels ++ bRels).map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should project endpoints - chained undirected/directed - nothing in scope - using input") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (aNodes, bNodes, aRels, bRels) = given { bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "R") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projectEndpoints("(x)<-[r]-(y)", startInScope = false, endInScope = true) // chained middle
      .projectEndpoints("(x)-[r]-(y)", startInScope = false, endInScope = false) // head
      .input(relationships = Seq("r"), nullable = false)
      .build()

    val input = batchedInputValues(sizeHint / 8, (aRels ++ bRels).map(r => Array[Any](r)): _*).stream()
    val runtimeResult = execute(logicalQuery, runtime, input)

    // then
    val expected = (for {
      a <- aNodes
      b <- bNodes
    } yield {
      (Array(a, b), Array(b, a))
    }).flatten {
      case (r1, r2) =>
        Array(r1, r2)
    }

    runtimeResult should beColumns("x", "y").withRows(expected)
  }
}
