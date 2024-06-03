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
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables

import scala.jdk.CollectionConverters.IterableHasAsScala

abstract class SetDynamicPropertyTestBase[CONTEXT <: RuntimeContext](
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
      .setDynamicProperty("head(ns)", "'p' + 'r' + 'o' + 'p'", "1")
      .projection("[n] AS ns", "n AS n")
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
      .setDynamicProperty("n", "'prop'", "null")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(null).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship property") {
    // given a single node
    val (_, relationships) = givenGraph {
      circleGraph(sizeHint)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "p")
      .projection("r.prop as p")
      .setDynamicProperty("r", "'prop'", "id(r)")
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("r", "p")
      .withRows(relationships.map(r => Array[Any](r, r.getId)))
      .withStatistics(propertiesSet = sizeHint)
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
      .setDynamicProperty("n", "'prop'", "1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set and remove already existing node property") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setDynamicProperty("n", "'prop'", "null")
      .setDynamicProperty("n", "'propOther'", "n.prop + 1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p").withSingleRow(null).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop", "propOther")
  }

  test("should throw on none node or relationship entity") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case i => Map("prop" -> "1") })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .setDynamicProperty("p1", "'prop'", "1")
      .projection("n.prop as p1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then

    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    assertThrows[InvalidArgumentException]({
      consume(runtimeResult)
    })
  }

  test("should throw on null property key") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case i => Map("prop" -> "1") })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .setDynamicProperty("n", "null", "1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then

    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    assertThrows[CypherTypeException]({
      consume(runtimeResult)
    })
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
      .setDynamicProperty("n", "'prop'", "oldP + 1")
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
      .|.setDynamicProperty("n", "'prop'", "oldP + 1")
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
      .setDynamicProperty("n", "'prop'", "oldP + 1")
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
      .setDynamicProperty("n", "'prop'", "oldP + 2")
      .apply()
      .|.setDynamicProperty("n", "'prop'", "oldP + 1")
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
      .setDynamicProperty("n", "'prop'", "2")
      .cacheProperties("n.prop")
      .setDynamicProperty("n", "'prop'", "1")
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
      .setDynamicProperty("n", "'prop'", "null")
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
      .setDynamicProperty("n", "'prop'", "3")
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
      .setDynamicProperty("n", "'prop'", "sin(null)")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
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
      .setDynamicProperty("n", "'prop'", "100")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(n.map(_ => Array(100))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship property from null value") {
    // given a single relationship
    val r = givenGraph {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setDynamicProperty("r", "'prop'", "null")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should set relationship property on null node") {
    // given a single relationship
    val r = givenGraph {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setDynamicProperty("r", "'prop'", "3")
      .input(relationships = Seq("r"))
      .build(readOnly = false)

    val input = inputValues(Array(r), Array(null))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withRows(singleColumn(Seq(3, null))).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship property from expression that requires null check") {
    // given a single relationship
    val r = givenGraph {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setDynamicProperty("r", "'prop'", "sin(null)")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should count updates even if value is not changed") {
    // given a single relationship
    val r = givenGraph {
      val nodes = nodeGraph(2)
      val r = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      r.setProperty("prop", "100")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setDynamicProperty("r", "'prop'", "100")
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(100).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should remove relationship property with eager") {
    // given CREATE (a), (b), (a)-[:X {num: 42}]->(b)
    givenGraph {
      val a = runtimeTestSupport.tx.createNode(Label.label("START"))
      val b = runtimeTestSupport.tx.createNode(Label.label("START"))
      val r = a.createRelationshipTo(b, RelationshipType.withName("X"))
      r.setProperty("num", 42)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("still_there")
      .projection("r.num IS NOT NULL AS still_there")
      .eager()
      .setRelationshipProperty("r", "num", "NULL")
      .expandAll("(n)-[r]->(m)")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("still_there")
      .withSingleRow(false)
      .withStatistics(propertiesSet = 1)
  }

  test("should set properties between two loops with continuation") {
    val nodes = givenGraph {
      nodePropertyGraph(sizeHint, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setDynamicProperty("n", "'prop'", "n.prop + 1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("n")
      .withRows(singleColumn(nodes.flatMap(n => Seq.fill(10)(n))))
      .withStatistics(propertiesSet = sizeHint)
    nodes.map(_.getProperty("prop")).foreach(i => i should equal(1))
  }

  test("set properties should invalidate cached properties") {
    // given a single node
    givenGraph {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("oldValue", "newValue")
      .projection("cacheN[n.prop] AS newValue")
      .setDynamicProperty("n", "'prop'", "n.prop + 1")
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
    givenGraph {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("oldValue", "newValue")
      .projection("cacheN[n.prop] AS newValue")
      .eager()
      .setDynamicProperty("n", "'prop'", "n.prop + 1")
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
}
