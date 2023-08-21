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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.RelationshipType

abstract class AllRelationshipsScanTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should support directed relationship scan") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (_, _, relationships, _) = given {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("r:R")
      .allRelationshipsScan("(x)-[r]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows(relationships.map(r =>
      Array(r, r.getStartNode, r.getEndNode)
    ))
  }

  test("should combine directed relationship scan and filter") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    given {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .allRelationshipsScan("(x)-[r]->(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("should handle multiple directed scans") {
    // given
    val (_, relationships) = given { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.allRelationshipsScan("()-[r3]->()")
      .apply()
      .|.allRelationshipsScan("()-[r2]->()")
      .allRelationshipsScan("()-[r1]->()")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for { r1 <- relationships; r2 <- relationships; r3 <- relationships } yield Array(r1, r2, r3)
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected)
  }

  test("should handle an argument in a directed scan") {
    // given
    val (_, relationships) = given { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "r")
      .apply()
      .|.projection("a AS b")
      .|.allRelationshipsScan("()-[r]->()", "a")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(3)))
    val expected = for (i <- 1 to 3; r <- relationships) yield Array[Any](i, r)
    runtimeResult should beColumns("b", "r").withRows(expected)
  }

  test("should support undirected relationship scan") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    val (_, _, relsA2B, relsB2A) = given {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .allRelationshipsScan("(x)-[r]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withRows((relsA2B ++ relsB2A).flatMap(r =>
      Seq(Array(r, r.getStartNode, r.getEndNode), Array(r, r.getEndNode, r.getStartNode))
    ))
  }

  test("should combine undirected relationship scan and filter") {
    // given
    val nNodes = Math.sqrt(sizeHint).ceil.toInt
    given {
      bidirectionalBipartiteGraph(nNodes, "A", "B", "R", "S")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "x", "y")
      .filter("x:NOT_THERE")
      .allRelationshipsScan("(x)-[r]-(y)")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("r", "x", "y").withNoRows()
  }

  test("should handle multiple undirected scans") {
    // given
    val (_, relationships) = given { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2", "r3")
      .apply()
      .|.allRelationshipsScan("()-[r3]-()")
      .apply()
      .|.allRelationshipsScan("()-[r2]-()")
      .allRelationshipsScan("()-[r1]-()")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = for {
      r1 <- relationships; r2 <- relationships; r3 <- relationships
    } yield Seq.fill(8)(Array(r1, r2, r3))
    runtimeResult should beColumns("r1", "r2", "r3").withRows(expected.flatten)
  }

  test("should handle an argument in an undirected scan") {
    // given
    val (_, relationships) = given { circleGraph(10, "L") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b", "r")
      .apply()
      .|.projection("a AS b")
      .|.allRelationshipsScan("()-[r]-()", "a")
      .input(variables = Seq("a"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(1), Array(2), Array(3)))
    val expected = (for (i <- 1 to 3; r <- relationships) yield Seq(Array[Any](i, r), Array[Any](i, r))).flatten
    runtimeResult should beColumns("b", "r").withRows(expected)
  }

  test("should handle directed and continuation") {
    val size = 100
    val (_, rels) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .allRelationshipsScan("(n)-[r]->(m)")
      .build()

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(10)(r))))
  }

  test("should handle undirected and continuation") {
    val size = 100
    val (_, rels) = given {
      circleGraph(size)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .allRelationshipsScan("(n)-[r]-(m)")
      .build()

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(2 * 10)(r))))
  }

  test("undirected scans only find loop once") {
    val rel = given {
      val a = tx.createNode()
      a.createRelationshipTo(a, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .allRelationshipsScan("(n)-[r]-(m)")
      .build()

    execute(logicalQuery, runtime) should beColumns("r").withSingleRow(rel)
  }
}
