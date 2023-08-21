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
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables
import org.neo4j.lock.LockType.EXCLUSIVE
import org.neo4j.lock.LockType.SHARED
import org.neo4j.lock.ResourceType.INDEX_ENTRY
import org.neo4j.lock.ResourceType.LABEL
import org.neo4j.lock.ResourceType.NODE

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class SetNodePropertiesFromMapTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should add new node property with removeOtherProps") {
    given {
      nodePropertyGraph(1, { case _: Int => Map("prop1" -> 1) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop1 as p1", "n.prop2 as p2")
      .setNodePropertiesFromMap("n", "{prop2: 3}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 3).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should add new node property without removeOtherProps") {
    given {
      nodePropertyGraph(1, { case _: Int => Map("prop1" -> 1) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop1 as p1", "n.prop2 as p2")
      .setNodePropertiesFromMap("n", "{prop2: 3}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 3).withStatistics(propertiesSet = 1)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should remove all node properties with removeOtherProps") {
    given {
      nodePropertyGraph(1, { case _: Int => Map("prop1" -> 1, "prop2" -> 2) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop1 as p1", "n.prop2 as p2")
      .setNodePropertiesFromMap("n", "{prop1: null}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, null).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should remove specific node property without removeOtherProps") {
    given {
      nodePropertyGraph(1, { case _: Int => Map("prop1" -> 1, "prop2" -> 2) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop1 as p1", "n.prop2 as p2")
      .setNodePropertiesFromMap("n", "{prop1: null}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 2).withStatistics(propertiesSet = 1)
    properties shouldBe Seq("prop1", "prop2")
  }

  test("should handle multiple set/remove without removeOtherProps") {
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p", "otherP")
      .projection("n.prop as p", "n.propOther as otherP")
      .setNodePropertiesFromMap("n", "{prop: null}", removeOtherProps = false)
      .setNodePropertiesFromMap("n", "{propOther: n.prop + 1}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p", "otherP").withSingleRow(null, 1).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop", "propOther")
  }

  test("should handle multiple set/remove without and with removeOtherProps") {
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p", "otherP")
      .projection("n.prop as p", "n.propOther as otherP")
      .setNodePropertiesFromMap("n", "{prop: 1}", removeOtherProps = true)
      .setNodePropertiesFromMap("n", "{propOther: n.prop + 1}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p", "otherP").withSingleRow(1, null).withStatistics(propertiesSet = 3)
    properties shouldBe Seq("prop", "propOther")
  }

  test("should set and remove multiple node properties") {
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("n.prop as p1", "n.propCopy as p2", "n.newProp as p3")
      .setNodePropertiesFromMap("n", "{propCopy: n.prop, newProp: 1}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList.sorted
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(null, 0, 1).withStatistics(propertiesSet = 3)
    properties shouldBe Seq("newProp", "prop", "propCopy")
  }

  test("should set property on multiple nodes") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.projection("n.prop as p")
      .|.setNodePropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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

  test("should set same node property multiple times") {
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: oldP + 2}", removeOtherProps = true)
      .apply()
      .|.setNodePropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cache[n.prop] as p")
      .setNodePropertiesFromMap("n", "{prop: 2}", removeOtherProps = true)
      .cacheProperties("n.prop")
      .setNodePropertiesFromMap("n", "{prop: 1}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(2).withStatistics(propertiesSet = 2)
    property shouldBe "prop"
  }

  test("should not add new token if node property is set to null value") {
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: null}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should throw on null map") {
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "null", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    the[CypherTypeException] thrownBy consume(
      runtimeResult
    ) should have message "Expected Null() to be a map, but it was :`NO_VALUE`"
  }

  test("should handle empty map") {
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 1) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(null).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should handle set node property on null node") {
    val n = given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: 3}", removeOtherProps = true)
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
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: sin(null)}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should count node property updates even if values are not changed") {
    val n = given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 100) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop: 100}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(n.map(_ => Array(100))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should fail when setting non-map node property") {
    given {
      nodePropertyGraph(1, { case _: Int => Map("prop1" -> 1) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("n.prop1 as p1", "n.prop2 as p2")
      .setNodePropertiesFromMap("n", "3", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    val thrownException = the[CypherTypeException] thrownBy consume(runtimeResult)
    thrownException.getMessage should fullyMatch regex "Expected (.*)3(.*) to be a map, but it was :`Long\\(3\\)`".r
  }

  /*
   * virtual nodes
   */
  test("should delete existing properties from virtual node") {
    val nodes = given {
      val nodes = nodeGraph(2)
      nodes.head.setProperty("prop1", 100)
      nodes.head.setProperty("prop2", 200)
      nodes(1).setProperty("prop1", 200)
      nodes(1).setProperty("prop3", 300)
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("n2.prop1 as p1", "n2.prop2 as p2", "n2.prop3 as p3")
      .setNodePropertiesFromMap("n2", "n1", removeOtherProps = true)
      .apply()
      .|.nodeByIdSeek("n2", Set.empty, nodes(1).getId)
      .nodeByIdSeek("n1", Set.empty, nodes.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(100, 200, null).withStatistics(propertiesSet = 3)
    properties shouldEqual Seq("prop1", "prop2", "prop3")
  }

  test("should update existing properties from virtual node") {
    val nodes = given {
      val nodes = nodeGraph(2)
      nodes.head.setProperty("prop1", 100)
      nodes.head.setProperty("prop2", 200)
      nodes(1).setProperty("prop1", 200)
      nodes(1).setProperty("prop3", 300)
      nodes
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("n2.prop1 as p1", "n2.prop2 as p2", "n2.prop3 as p3")
      .setNodePropertiesFromMap("n2", "n1", removeOtherProps = false)
      .apply()
      .|.nodeByIdSeek("n2", Set.empty, nodes(1).getId)
      .nodeByIdSeek("n1", Set.empty, nodes.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(100, 200, 300).withStatistics(propertiesSet = 2)
    properties shouldEqual Seq("prop1", "prop2", "prop3")
  }

  test("should update existing properties from virtual relationship to virtual node") {
    val relationships = given {
      val nodes = nodeGraph(2)
      val relationships = Seq(nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R")))
      nodes.head.setProperty("prop1", 200)
      nodes.head.setProperty("prop2", 300)
      relationships.head.setProperty("prop1", 100)
      relationships.head.setProperty("prop3", 400)
      relationships
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("x1.prop1 as p1", "x1.prop2 as p2", "x1.prop3 as p3")
      .setNodePropertiesFromMap("x1", "r", removeOtherProps = true)
      .directedRelationshipByIdSeek("r", "x1", "y1", Set.empty, relationships.head.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(100, null, 400).withStatistics(propertiesSet = 3)
    properties shouldEqual Seq("prop1", "prop2", "prop3")
  }

  test("should lock node") {
    // given a single node
    val n = given {
      nodeGraph(1).head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setNodePropertiesFromMap("n", "{prop:  1}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1).withLockedNodes(Set(n.getId))
  }

  test("should set node properties from map between two loops with continuation") {
    val nodes = given {
      nodePropertyGraph(sizeHint, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setNodePropertiesFromMap("n", "{prop: n.prop + 1}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(propertiesSet = sizeHint)
    nodes.map(_.getProperty("prop")).foreach(i => i should equal(1))
  }

  test("should set multiple properties without violating constraint (removeOtherProps = true)") {
    val nodes = given {
      uniqueNodeIndex("L", "p1", "p2")

      // p1 = 0, p2 = 0
      // p1 = 1, p2 = 0
      nodePropertyGraph(2, { case i => Map("p1" -> i, "p2" -> 0, "toBeRemoved" -> 1337) }, "L")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      // this could temporarily make the property pair non-unique
      .setNodePropertiesFromMap("n", "{p1: 1, p2: 3}", removeOtherProps = true)
      .filter("n.p1 = 0", "n.p2 = 0")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withSingleRow(nodes.head)
      .withStatistics(propertiesSet = 3)
    // should have removed property from the first node
    nodes.foreach(n => n.hasProperty("toBeRemoved") shouldBe n.getId != nodes.head.getId)
  }

  test("should set multiple properties without violating constraint (removeOtherProps = false)") {
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
      .setNodePropertiesFromMap("n", "{p1: 1, p2: 3}", removeOtherProps = false)
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
      .setNodePropertiesFromMap("n", "{other: n.prop, prop: n.prop}", removeOtherProps = true)
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
      .setNodePropertiesFromMap("n", "{other: n.prop + 1, prop: n.prop + 1}", removeOtherProps = true)
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
