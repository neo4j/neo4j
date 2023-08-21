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
import org.neo4j.internal.helpers.collection.Iterables

abstract class SetRelationshipPropertyTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should set relationship property") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "1")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1).withLockedRelationships(Set(
      r.getId
    ))

    property shouldBe "prop"
  }

  test("should set relationship property from refslot") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("rRef.prop as p")
      .setRelationshipProperty("rRef", "prop", "1")
      .unwind("[r] as rRef")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should remove relationship property") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r.setProperty("prop", "1")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "null")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(null).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set already existing relationship property") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r.setProperty("prop", "0")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "1")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(1).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set property on multiple relationships") {
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("prop", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "oldP + 1")
      .filter("oldP < 5")
      .projection("r.prop as oldP")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
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
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("prop", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .apply()
      .|.projection("r.prop as p")
      .|.setRelationshipProperty("r", "prop", "oldP + 1")
      .|.filter("oldP < 5")
      .|.argument("oldP")
      .projection("r.prop as oldP")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
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
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("prop", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "oldP + 1")
      .limit(3)
      .filter("oldP < 5")
      .projection("r.prop as oldP")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
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
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("prop", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "oldP + 2")
      .apply()
      .|.setRelationshipProperty("r", "prop", "oldP + 1")
      .|.filter("oldP < 5")
      .|.argument("oldP")
      .projection("r.prop as oldP")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
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

  test("should set cached relationship property") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("cacheR[r.prop] as p")
      .setRelationshipProperty("r", "prop", "2")
      .cacheProperties("r.prop")
      .setRelationshipProperty("r", "prop", "1")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(2).withStatistics(propertiesSet = 2)
    property shouldBe "prop"
  }

  test("should set relationship property from null value") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "null")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should set relationship property on null relationship") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "3")
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
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "sin(null)")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p").withSingleRow(null).withNoUpdates()
  }

  test("should count relationship property updates even if values are not changed") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r.setProperty("prop", "100")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p")
      .projection("r.prop as p")
      .setRelationshipProperty("r", "prop", "100")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val property = Iterables.single(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p").withSingleRow(100L).withStatistics(propertiesSet = 1)
    property shouldBe "prop"
  }

  test("should set relationship properties between two loops with continuation") {
    val rels = given {
      val (_, rs) = circleGraph(sizeHint)
      rs.foreach(_.setProperty("prop", 0))
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setRelationshipProperty("r", "prop", "r.prop + 1")
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(10)(r))))
      .withStatistics(propertiesSet = sizeHint)
    rels.map(_.getProperty("prop")).foreach(i => i should equal(1))
  }

  test("set relationship property should invalidate cached properties") {
    // given a single node
    given {
      val node = runtimeTestSupport.tx.createNode()
      node.createRelationshipTo(node, RelationshipType.withName("REL")).setProperty("prop", 0L)
    }

    invalidateCachedPropsTest(0) { builder =>
      builder.setRelationshipProperty("r", "prop", "r.prop + 1")
    }
    invalidateCachedPropsTest(1) { builder =>
      builder.setRelationshipProperties("r", "prop" -> "r.prop + 1")
    }
    invalidateCachedPropsTest(2) { builder =>
      builder.setRelationshipPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = false)
    }
    invalidateCachedPropsTest(3) { builder =>
      builder.setRelationshipPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = true)
    }
    invalidateCachedPropsTest(4) { builder =>
      builder.setProperty("r", "prop", "r.prop + 1")
    }
    invalidateCachedPropsTest(5) { builder =>
      builder.setProperties("r", "prop" -> "r.prop + 1")
    }
    invalidateCachedPropsTest(6) { builder =>
      builder.setPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = false)
    }
    invalidateCachedPropsTest(7) { builder =>
      builder.setPropertiesFromMap("r", "{prop: r.prop + 1}", removeOtherProps = true)
    }
  }

  private def invalidateCachedPropsTest(expected: Long)(setOp: LogicalQueryBuilder => LogicalQueryBuilder): Unit = {
    restartTx()
    val builder = new LogicalQueryBuilder(this)
      .produceResults("oldValue", "newValue")
      .projection("cacheR[r.prop] as newValue")
    val logicalQuery = setOp.apply(builder)
      .projection("cacheRFromStore[r.prop] AS oldValue")
      .relationshipTypeScan("()-[r:REL]->()")
      .build(readOnly = false)

    execute(logicalQuery, runtime) should beColumns("oldValue", "newValue")
      .withSingleRow(expected, expected + 1)
      .withStatistics(propertiesSet = 1)
  }
}
