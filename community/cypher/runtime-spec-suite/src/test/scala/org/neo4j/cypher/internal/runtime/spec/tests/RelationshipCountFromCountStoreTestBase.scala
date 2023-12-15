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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType

abstract class RelationshipCountFromCountStoreTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {
  private val actualSize = 11

  test("should get count when both wildcard labels") {
    val (aNodes1, bNodes1, aNodes2, bNodes2) = givenGraph {
      val (aNodes1, bNodes1) = bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType")
      val (aNodes2, bNodes2) = bipartiteGraph(actualSize, "LabelC", "LabelD", "RelType")
      (aNodes1, bNodes1, aNodes2, bNodes2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = aNodes1.size * bNodes1.size + aNodes2.size * bNodes2.size
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(expectedCount)))
  }

  test("should get count for single relationship type and start label") {
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List("RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should get count for single relationship type and end label") {
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("RelType"), Some("LabelB"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should get count for wildcard relationship type and one label") {
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List(), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should work on rhs of apply") {
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("x > 0")
      .apply()
      .|.relationshipCountFromCountStore("x", Some("LabelA"), List(), None)
      .nodeByLabelScan("n", "LabelA", IndexOrderNone)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount: Long = aNodes.size * bNodes.size
    val expectedRows = aNodes.map(_ => expectedCount)
    runtimeResult should beColumns("x").withRows(singleColumn(expectedRows))
  }

  test("should get count for multiple relationship types and one provided label") {
    val (aNodes1, bNodes1, aNodes2, bNodes2) = givenGraph {
      val (aNodes1, bNodes1) = bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType1")
      val (aNodes2, bNodes2) = bipartiteGraph(actualSize, "LabelA", "LabelC", "RelType2")
      (aNodes1, bNodes1, aNodes2, bNodes2)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List("RelType1", "RelType2"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = aNodes1.size * bNodes1.size + aNodes2.size * bNodes2.size
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(expectedCount)))
  }

  test("should return zero for count of non-existent label") {
    // given
    givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("NonExistent"), List("RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(0)))
  }

  test("should handle start label not present at compile time") {
    givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("NotThereYet"), List("RelType"), None)
      .build()

    val plan = buildPlan(logicalQuery, runtime)
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(0)))

    givenGraph {
      tx.createNode(Label.label("NotThereYet")).createRelationshipTo(
        tx.createNode(),
        RelationshipType.withName("RelType")
      )
    }

    // then
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(1)))
  }

  test("should handle end label not present at compile time") {
    givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("RelType"), Some("NotThereYet"))
      .build()

    val plan = buildPlan(logicalQuery, runtime)
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(0)))

    givenGraph {
      tx.createNode().createRelationshipTo(
        tx.createNode(Label.label("NotThereYet")),
        RelationshipType.withName("RelType")
      )
    }

    // then
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(1)))
  }

  test("should handle relationship type not present at compile time") {
    givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("NotThereYet"), None)
      .build()

    val plan = buildPlan(logicalQuery, runtime)
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(0)))
    givenGraph { tx.createNode().createRelationshipTo(tx.createNode(), RelationshipType.withName("NotThereYet")) }

    // then
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(1)))
  }

  test("should get count when all wildcards") {
    // given
    val (aNodes, bNodes) = givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List(), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(aNodes.size * bNodes.size)))
  }

  test("should return zero for count of non-existent relationship type") {
    givenGraph { bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType") }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", None, List("NonExistent"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(0)))
  }

  test("should get count for multiple identical relationship types and one provided label") {
    val (aNodes, bNodes) = givenGraph {
      bipartiteGraph(actualSize, "LabelA", "LabelB", "RelType")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List("RelType", "RelType"), None)
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expectedCount = aNodes.size * bNodes.size + aNodes.size * bNodes.size
    runtimeResult should beColumns("x").withRows(singleColumn(Seq(expectedCount)))
  }

  test("should get count for multiple identical relationship types not there at compile time and one provided label") {

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .relationshipCountFromCountStore("x", Some("LabelA"), List("NotThereYet", "NotThereYet"), None)
      .build()

    val plan = buildPlan(logicalQuery, runtime)
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(0)))

    val (aNodes, bNodes) = givenGraph {
      bipartiteGraph(actualSize, "LabelA", "LabelB", "NotThereYet")
    }

    // then
    val expectedCount = aNodes.size * bNodes.size + aNodes.size * bNodes.size
    execute(plan) should beColumns("x").withRows(singleColumn(Seq(expectedCount)))
  }

  test("empty count followed by optional") {
    val query = new LogicalQueryBuilder(this)
      .produceResults("r1", "r2")
      .projection("1 % j AS r1", "j ^ j AS r2")
      .optional()
      .cartesianProduct()
      .|.relationshipCountFromCountStore("j", None, List("R"), None)
      .unwind("[] AS i")
      .argument()
      .build()

    execute(query, runtime) should beColumns("r1", "r2").withSingleRow(null, null)
  }
}
