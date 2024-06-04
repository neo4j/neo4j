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

abstract class SetNodePropertyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should set node property") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set node property from refslot") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("nRef.prop as p")
      .setNodeProperty("nRef", "prop", "1")
      .unwind("[n] as nRef")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should remove node property") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case i: Int => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "null")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(null).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set already existing node property") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set property on multiple nodes") {
    // given a single node
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "oldP + 1")
      .filter("oldP < 5")
      .projection("n.prop as oldP")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 1)))
      .withStatistics(propertiesSet = Math.min(5, sizeHint))
    property shouldBe "prop"
  }

  test("should set property on rhs of apply") {
    // given a single node
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.projection("n.prop as p")
      .|.setNodeProperty("n", "prop", "oldP + 1")
      .|.filter("oldP < 5")
      .|.argument("oldP")
      .projection("n.prop as oldP")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 1)))
      .withStatistics(propertiesSet = Math.min(5, sizeHint))
    property shouldBe "prop"
  }

  test("should set property after limit") {
    // given a single node
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "oldP + 1")
      .limit(3)
      .filter("oldP < 5")
      .projection("n.prop as oldP")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p")
      .withRows((0 to Math.min(3 - 1, sizeHint)).map(n => Array(n + 1)))
      .withStatistics(propertiesSet = Math.min(3, sizeHint))
    property shouldBe "prop"
  }

  test("should set same property multiple times") {
    // given a single node
    givenGraph {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "oldP + 2")
      .apply()
      .|.setNodeProperty("n", "prop", "oldP + 1")
      .|.filter("oldP < 5")
      .|.argument("oldP")
      .projection("n.prop as oldP")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 2)))
      .withStatistics(propertiesSet = Math.min(5, sizeHint) * 2)
    property shouldBe "prop"
  }

  test("should set cached node property") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cache[n.prop] as p")
      .setNodeProperty("n", "prop", "2")
      .cacheProperties("n.prop")
      .setNodeProperty("n", "prop", "1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(2).withStatistics(propertiesSet = 2)
    property shouldBe "prop"
  }

  test("should set node property from null value") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "null")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should set node property on null node") {
    // given a single node
    val n = givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "3")
      .input(nodes = Seq("n"))
      .build(readOnly = false)

    val input = inputValues(Array(n.head), Array(null))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(singleColumn(Seq(3, null))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set node property from expression that requires null check") {
    // given a single node
    givenGraph {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "sin(null)")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
  }

  test("should count node property updates even if values are not changed") {
    // given single node
    val n = givenGraph {
      nodePropertyGraph(1, { case i => Map("prop" -> 100) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "100")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(n.map(_ => Array(100))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should lock node") {
    // given a single node
    val n = givenGraph {
      nodeGraph(1).head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodeProperty("n", "prop", "1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1).withLockedNodes(Set(n.getId))
  }

  test("should set node properties between two loops with continuation") {
    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(propertiesSet = sizeHint)
    nodes.map(_.getProperty("prop")).foreach(i => i should equal(1))
  }

  test("should not take exclusive lock if value not changing") {
    // given a single node
    givenGraph {
      uniqueNodeIndex("L", "prop")
      nodePropertyGraph(1, { case _ => Map("prop" -> 1) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop as p1", "n.other as p2")
      .setNodeProperty("n", "prop", "n.prop")
      .setNodeProperty("n", "other", "n.prop")
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
    givenGraph {
      uniqueNodeIndex("L", "prop")
      nodePropertyGraph(1, { case _ => Map("prop" -> 1) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop as p1", "n.other as p2")
      .setNodeProperty("n", "prop", "n.prop + 1")
      .setNodeProperty("n", "other", "n.prop+ 1")
      .nodeIndexOperator("n:L(prop = 1)", unique = true)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withSingleRow(2, 2)
      .withStatistics(propertiesSet = 2)
      .withLocks((EXCLUSIVE, NODE), (EXCLUSIVE, INDEX_ENTRY), (SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("set node property should invalidate cached properties") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    invalidateCachedPropsTest(0) { builder =>
      builder.setNodeProperty("n", "prop", "n.prop + 1")
    }
    invalidateCachedPropsTest(1) { builder =>
      builder.setNodeProperties("n", "prop" -> "n.prop + 1")
    }
    invalidateCachedPropsTest(2) { builder =>
      builder.setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false)
    }
    invalidateCachedPropsTest(3) { builder =>
      builder.setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = true)
    }
    invalidateCachedPropsTest(4) { builder =>
      builder.setProperty("n", "prop", "n.prop + 1")
    }
    invalidateCachedPropsTest(5) { builder =>
      builder.setProperties("n", "prop" -> "n.prop + 1")
    }
    invalidateCachedPropsTest(6) { builder =>
      builder.setPropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = false)
    }
    invalidateCachedPropsTest(7) { builder =>
      builder.setPropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = true)
    }
    invalidateCachedPropsTest(8) { builder =>
      builder.setDynamicProperty("n", "'prop'", "n.prop + 1")
    }
  }

  private def invalidateCachedPropsTest(expected: Long)(setOp: LogicalQueryBuilder => LogicalQueryBuilder): Unit = {
    restartTx()
    val queryBuilder = new LogicalQueryBuilder(this)
      .produceResults("oldValue", "newValue")
      .projection("cacheN[n.prop] as newValue")
    val logicalQuery = setOp.apply(queryBuilder)
      .projection("cacheNFromStore[n.prop] AS oldValue")
      .allNodeScan("n")
      .build(readOnly = false)

    execute(logicalQuery, runtime) should beColumns("oldValue", "newValue")
      .withSingleRow(expected, expected + 1)
      .withStatistics(propertiesSet = 1)
  }
}
