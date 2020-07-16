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
import org.neo4j.cypher.internal.runtime.spec._

import scala.collection.JavaConverters._

abstract class ApplyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

 test("nested apply with identical branches ending in optional multiple identifiers") {
    val numberOfNodes = 3
    val (nodes, _) = given {
      bipartiteGraph(numberOfNodes, "A", "B", "R")
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.apply()
      .|.|.optional("x")
      .|.|.expandAll("(x)-->(y)")
      .|.|.argument("x")
      .|.optional("x")
      .|.expandAll("(x)-->(y)")
      .|.argument("x")
      .nodeByLabelScan("x", "A")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(n => Seq.fill(numberOfNodes * numberOfNodes)(Array[Any](n)))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("nested apply with identical branches ending in optional single identifier") {
    val numberOfNodes = 3
    val nodes = given {
      nodeGraph(numberOfNodes)
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.apply()
      .|.|.optional()
      .|.|.filter("true")
      .|.|.allNodeScan("y", "x")
      .|.optional()
      .|.filter("true")
      .|.allNodeScan("y", "x")
      .allNodeScan("x")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.flatMap(n => Seq.fill(numberOfNodes * numberOfNodes)(Array[Any](n)))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("cartesian product nested under apply") {
    // given
    val nodes = given {
      val (aNodes, bNodes, _, _) = bidirectionalBipartiteGraph(2, "A", "B", "AB", "BA")
      aNodes ++ bNodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "r", "y")
      .apply()
      .|.cartesianProduct()
      .|.|.expandAll("(x)-[r]->(y)")
      .|.|.argument("x")
      .|.argument("x")
      .allNodeScan("x")
      .build()

    // then
    val expected = for {
      x <- nodes
      r <- x.getRelationships().asScala if r.getStartNodeId == x.getId
    } yield {
      val y = r.getEndNode
      Array[Any](x, r, y)
    }

    val runtimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("x", "r", "y").withRows(expected)
  }
}
