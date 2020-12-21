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
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.RelationshipType
import org.neo4j.internal.helpers.collection.Iterables

import scala.collection.JavaConverters.iterableAsScalaIterableConverter

abstract class SetPropertiesFromMapTestBase[CONTEXT <: RuntimeContext](
                                                               edition: Edition[CONTEXT],
                                                               runtime: CypherRuntime[CONTEXT],
                                                               sizeHint: Int
                                                             ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should set node property") {
    // given a single node
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n","{prop: 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(1, { case _: Int => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: null}", removeOtherProps = true)
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
    val relationship = given {
      val nodes = nodeGraph(2)
       nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r","{prop: id(r)}", removeOtherProps = true)
      .expandAll("(n)-[r]->()")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("r", "p")
      .withRows(Seq(Array(relationship, relationship.getId)))
      .withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set already existing node property") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n","{prop: 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p", "otherP")
      .projection("n.prop as p", "n.propOther as otherP")
      .setPropertiesFromMap("n","{prop: null}", removeOtherProps = false)
      .setPropertiesFromMap("n","{propOther: n.prop + 1}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p", "otherP").withSingleRow(null, 1).withStatistics(propertiesSet = 2)
    properties shouldBe Seq("prop", "propOther")
  }

  test("should set and remove other node properties") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p", "otherP")
      .projection("n.prop as p", "n.propOther as otherP")
      .setPropertiesFromMap("n","{prop: 1}", removeOtherProps = true)
      .setPropertiesFromMap("n","{propOther: n.prop + 1}", removeOtherProps = false)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p", "otherP").withSingleRow(1, null).withStatistics(propertiesSet = 3)
    properties shouldBe Seq("prop", "propOther")
  }

  test("should set and remove multiple properties") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 0) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2", "p3")
      .projection("n.prop as p1", "n.propCopy as p2", "n.newProp as p3")
      .setPropertiesFromMap("n","{propCopy: n.prop, newProp: 1}", removeOtherProps = true)
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = tx.getAllPropertyKeys.asScala.toList
    runtimeResult should beColumns("p1", "p2", "p3").withSingleRow(null, 0, 1).withStatistics(propertiesSet = 3)
    properties shouldBe Seq("prop", "propCopy", "newProp")
  }

  test("should throw on non node or relationship entity") {
    // given a single node
    given {
      nodePropertyGraph(1, { case _ => Map("prop" -> "1") })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1")
      .setPropertiesFromMap("p1","{propCopy: n.prop}", removeOtherProps = true)
      .projection("n.prop as p1")
      .allNodeScan("n")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    assertThrows[InvalidArgumentException]({
      consume(runtimeResult)
    })
  }

  test("should set property on multiple nodes") {
    // given a single node
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.projection("n.prop as p")
      .|.setPropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) })
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: oldP + 2}", removeOtherProps = true)
      .apply()
      .|.setPropertiesFromMap("n", "{prop: oldP + 1}", removeOtherProps = true)
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
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cache[n.prop] as p")
      .setPropertiesFromMap("n", "{prop: 2}", removeOtherProps = true)
      .cacheProperties("n.prop")
      .setPropertiesFromMap("n", "{prop: 1}", removeOtherProps = true)
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
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: null}", removeOtherProps = true)
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
    val n = given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: 3}", removeOtherProps = true)
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
    given {
      nodeGraph(1)
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: sin(null)}", removeOtherProps = true)
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
    val n = given {
      nodePropertyGraph(1, { case _ => Map("prop" -> 100)})
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("n.prop as p")
      .setPropertiesFromMap("n", "{prop: 100}", removeOtherProps = true )
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
    val r = given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: null}", removeOtherProps = true)
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
    val r = given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: 3}", removeOtherProps = true)
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
    val r = given {
      val nodes = nodeGraph(2)
      nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: sin(null)}", removeOtherProps = true)
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
    val r = given {
      val nodes = nodeGraph(2)
      val r = nodes.head.createRelationshipTo(nodes(1), RelationshipType.withName("R"))
      r.setProperty("prop", "100")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setPropertiesFromMap("r", "{prop: 100}", removeOtherProps = true)
      .directedRelationshipByIdSeek("r", "x", "y", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(100).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }
}
