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
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.lock.LockType.EXCLUSIVE
import org.neo4j.lock.LockType.SHARED
import org.neo4j.lock.ResourceType.INDEX_ENTRY
import org.neo4j.lock.ResourceType.LABEL
import org.neo4j.lock.ResourceType.NODE

abstract class SetNodePropertiesTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should set node properties") {
    // given a single node
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "1"), ("p2", "2"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = Iterables.asList(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 2).withStatistics(propertiesSet = 2)
    properties shouldBe java.util.List.of("p1", "p2")
  }

  test("should set node properties from refslot") {
    // given a single node
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("nRef.p1 as p1", "nRef.p2 as p2")
      .setNodeProperties("nRef", ("p1", "1"), ("p2", "2"))
      .unwind("[n] as nRef")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = Iterables.asList(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 2).withStatistics(propertiesSet = 2)
    properties shouldBe java.util.List.of("p1", "p2")
  }

  test("should remove and set node properties") {
    // given a single node
    given {
      nodePropertyGraph(1, { case i: Int => Map("p1" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "null"), ("p2", "2"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val node = Iterables.single(tx.getAllNodes)
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 2).withStatistics(propertiesSet = 2)
    node.hasProperty("p1") shouldBe false
    node.hasProperty("p2") shouldBe true
  }

  test("should set already existing node properties") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("p1" -> 0, "p2" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "1"), ("p2", "1"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 1).withStatistics(propertiesSet = 2)
  }

  test("should set properties on multiple nodes") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p1" -> i, "p2" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .filter("oldP1 < 5", "oldP2 < 5")
      .projection("n.p1 as oldP1", "n.p2 as oldP2")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 1, n + 1)))
      .withStatistics(propertiesSet = 2 * Math.min(5, sizeHint))
  }

  test("should set properties on rhs of apply") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p1" -> i, "p2" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .apply()
      .|.projection("n.p1 as p1", "n.p2 as p2")
      .|.setNodeProperties("n", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .|.filter("oldP1 < 5", "oldP2 < 5")
      .|.argument("oldP1", "oldP2")
      .projection("n.p1 as oldP1", "n.p2 as oldP2")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 1, n + 1)))
      .withStatistics(propertiesSet = 2 * Math.min(5, sizeHint))
  }

  test("should set properties after limit") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p1" -> i, "p2" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .limit(3)
      .filter("oldP1 < 5", "oldP2 < 5")
      .projection("n.p1 as oldP1", "n.p2 as oldP2")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(3 - 1, sizeHint)).map(n => Array(n + 1, n + 1)))
      .withStatistics(propertiesSet = 2 * Math.min(3, sizeHint))
  }

  test("should set same properties multiple times") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("p1" -> i, "p2" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "oldP1 + 2"), ("p2", "oldP2 + 2"))
      .apply()
      .|.setNodeProperties("n", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .|.filter("oldP1 < 5", "oldP2 < 5")
      .|.argument("oldP1", "oldP2")
      .projection("n.p1 as oldP1", "n.p2 as oldP2")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 2, n + 2)))
      .withStatistics(propertiesSet = 2 * Math.min(5, sizeHint) * 2)
  }

  test("should set cached node properties") {
    // given a single node
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("cache[n.p1] as p1", "cache[n.p2] as p2")
      .setNodeProperties("n", ("p1", "2"), ("p2", "2"))
      .cacheProperties("n.p1", "n.p2")
      .setNodeProperties("n", ("p1", "1"), ("p2", "1"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(2, 2).withStatistics(propertiesSet = 4)
  }

  test("set properties should invalidate cached properties") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("oldValue", "newValue")
      .projection("cacheN[n.prop] AS newValue")
      .setProperty("n", "prop", "n.prop + 1")
      .projection("cacheNFromStore[n.prop] AS oldValue")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("oldValue", "newValue")
      .withSingleRow(0, 1)
      .withStatistics(propertiesSet = 1)
  }

  test("set properties should invalidate cached properties with pipeline break") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("oldValue", "newValue")
      .projection("cacheN[n.prop] AS newValue")
      .eager()
      .setProperty("n", "prop", "n.prop + 1")
      .eager()
      .projection("cacheNFromStore[n.prop] AS oldValue")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("oldValue", "newValue")
      .withSingleRow(0, 1)
      .withStatistics(propertiesSet = 1)
  }

  test("should set node properties from null value") {
    // given a single node
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "null"), ("p2", "null"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, null).withNoUpdates()
  }

  test("should set node properties on null node") {
    // given a single node
    val n = given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "3"), ("p2", "3"))
      .input(nodes = Seq("n"))
      .build(readOnly = false)

    val input = inputValues(Array(n.head), Array(null))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("p1", "p2").withRows(Seq(Array(3, 3), Array(null, null))).withStatistics(
      propertiesSet = 2
    )
  }

  test("should set node properties from expression that requires null check") {
    // given a single node
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "sin(null)"), ("p2", "cos(null)"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, null).withNoUpdates()
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
  }

  test("should count node properties updates even if values are not changed") {
    // given single node
    val n = given {
      nodePropertyGraph(1, { case i => Map("p1" -> 100, "p2" -> 100) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "100"), ("p2", "100"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withRows(n.map(_ => Array(100, 100))).withStatistics(propertiesSet = 2)
  }

  test("should lock node") {
    // given a single node
    val n = given {
      nodeGraph(1).head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.p1 as p1", "n.p2 as p2")
      .setNodeProperties("n", ("p1", "1"), ("p2", "1"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 1).withStatistics(propertiesSet = 2).withLockedNodes(
      Set(n.getId)
    )
  }

  test("should set node properties between two loops with continuation") {
    val nodes = given {
      nodePropertyGraph(sizeHint, { case _ => Map("p1" -> 0, "p2" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setNodeProperties("n", ("p1", "n.p1 + 1"), ("p2", "n.p2 + 1"))
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(propertiesSet = 2 * sizeHint)
    nodes.map(_.getProperty("p1")).foreach(i => i should equal(1))
    nodes.map(_.getProperty("p2")).foreach(i => i should equal(1))
  }

  test("should set multiple properties without violating constraint") {
    val nodes = given {
      uniqueNodeIndex("L", "p1", "p2")

      // p1 = 0, p2 = 0
      // p1 = 1, p2 = 0
      nodePropertyGraph(2, { case i => Map("p1" -> i, "p2" -> 0) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      // this could temporarily make the property pair non-unique
      .setNodeProperties("n", ("p1", "1"), ("p2", "3"))
      .filter("n.p1 = 0", "n.p2 = 0")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withSingleRow(nodes.head)
      .withStatistics(propertiesSet = 2)
  }

  test("should not take exclusive lock if value not changing") {
    // given a single node
    given {
      uniqueNodeIndex("L", "prop")
      nodePropertyGraph(1, { case _ => Map("prop" -> 1) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop as p1", "n.other as p2")
      .setNodeProperties("n", ("other", "n.prop"), ("prop", "n.prop"))
      .nodeIndexOperator("n:L(prop = 1)", unique = true)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withSingleRow(1, 1)
      .withStatistics(propertiesSet = 2)
      .withLocks((EXCLUSIVE, NODE), (SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("should take exclusive lock if value changing") {
    // given a single node
    given {
      uniqueNodeIndex("L", "prop")
      nodePropertyGraph(1, { case _ => Map("prop" -> 1) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop as p1", "n.other as p2")
      .setNodeProperties("n", ("other", "n.prop + 1"), ("prop", "n.prop + 1"))
      .nodeIndexOperator("n:L(prop = 1)", unique = true)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withSingleRow(2, 2)
      .withStatistics(propertiesSet = 2)
      .withLocks((EXCLUSIVE, NODE), (EXCLUSIVE, INDEX_ENTRY), (SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }
}
