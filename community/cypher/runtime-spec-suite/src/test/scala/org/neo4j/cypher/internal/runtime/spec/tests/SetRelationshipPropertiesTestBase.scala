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

abstract class SetRelationshipPropertiesTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should set relationship properties") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "1"), ("p2", "2"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = Iterables.asList(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 2).withStatistics(propertiesSet = 2)
    properties shouldBe java.util.List.of("p1", "p2")
  }

  test("should set relationship properties from refslot") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      n1.createRelationshipTo(n2, RelationshipType.withName("R"))
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("rRef.p1 as p1", "rRef.p2 as p2")
      .setRelationshipProperties("rRef", ("p1", "1"), ("p2", "2"))
      .unwind("[r] as rRef")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val properties = Iterables.asList(tx.getAllPropertyKeys)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 2).withStatistics(propertiesSet = 2)
    properties shouldBe java.util.List.of("p1", "p2")
  }

  test("should remove and set relationship properties") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r.setProperty("p1", "1")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "null"), ("p2", "2"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    val rel = Iterables.single(tx.getAllRelationships)
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, 2).withStatistics(propertiesSet = 2)
    rel.hasProperty("p1") shouldBe false
    rel.hasProperty("p2") shouldBe true
  }

  test("should set already existing relationship properties") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r.setProperty("p1", "0")
      r.setProperty("p2", "0")
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "1"), ("p2", "1"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(1, 1).withStatistics(propertiesSet = 2)
  }

  test("should set properties on multiple relationships") {
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("p1", i)
        r.setProperty("p2", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .filter("oldP1 < 5", "oldP2 < 5")
      .projection("r.p1 as oldP1", "r.p2 as oldP2")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 1, n + 1)))
      .withStatistics(propertiesSet = 2 * Math.min(5, sizeHint))
  }

  test("should set properties on rhs of apply") {
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("p1", i)
        r.setProperty("p2", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .apply()
      .|.projection("r.p1 as p1", "r.p2 as p2")
      .|.setRelationshipProperties("r", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .|.filter("oldP1 < 5", "oldP2 < 5")
      .|.argument("oldP1", "oldP2")
      .projection("r.p1 as oldP1", "r.p2 as oldP2")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 1, n + 1)))
      .withStatistics(propertiesSet = 2 * Math.min(5, sizeHint))
  }

  test("should set properties after limit") {
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("p1", i)
        r.setProperty("p2", i)
        i += 1
      }
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .limit(3)
      .filter("oldP1 < 5", "oldP2 < 5")
      .projection("r.p1 as oldP1", "r.p2 as oldP2")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(3 - 1, sizeHint)).map(n => Array(n + 1, n + 1)))
      .withStatistics(propertiesSet = 2 * Math.min(3, sizeHint))
  }

  test("should set same properties multiple times") {
    val rs = given {
      val (_, r) = circleGraph(sizeHint, "A")
      var i = 0
      r.foreach { r =>
        r.setProperty("p1", i)
        r.setProperty("p2", i)
        i += 1
      }
      r
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "oldP1 + 2"), ("p2", "oldP2 + 2"))
      .apply()
      .|.setRelationshipProperties("r", ("p1", "oldP1 + 1"), ("p2", "oldP2 + 1"))
      .|.filter("oldP1 < 5", "oldP2 < 5")
      .|.argument("oldP1", "oldP2")
      .projection("r.p1 as oldP1", "r.p2 as oldP2")
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, rs.map(_.getId): _*)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2")
      .withRows((0 to Math.min(5 - 1, sizeHint)).map(n => Array(n + 2, n + 2)))
      .withStatistics(propertiesSet = 2 * Math.min(5, sizeHint) * 2)
  }

  test("should set cached relationship properties") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("cacheR[r.p1] as p1", "cacheR[r.p2] as p2")
      .setRelationshipProperties("r", ("p1", "2"), ("p2", "2"))
      .cacheProperties("r.p1", "r.p2")
      .setRelationshipProperties("r", ("p1", "1"), ("p2", "1"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(2, 2).withStatistics(propertiesSet = 4)
  }

  test("should set relationship properties from null value") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "null"), ("p2", "null"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, null).withNoUpdates()
  }

  test("should set relationship properties on null relationship") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "3"), ("p2", "3"))
      .input(relationships = Seq("r"))
      .build(readOnly = false)

    val input = inputValues(Array(r), Array(null))

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime, input)
    runtimeResult should beColumns("p1", "p2").withRows(Seq(Array(3, 3), Array(null, null))).withStatistics(
      propertiesSet = 2
    )
  }

  test("should set relationship properties from expression that requires null check") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "sin(null)"), ("p2", "cos(null)"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(null, null).withNoUpdates()
    tx.getAllPropertyKeys.iterator().hasNext shouldBe false
  }

  test("should count relationship properties updates even if values are not changed") {
    // given a single relationship
    val r = given {
      val Seq(n1, n2) = nodeGraph(2)
      val r = n1.createRelationshipTo(n2, RelationshipType.withName("R"))
      r
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("p1", "p2")
      .projection("r.p1 as p1", "r.p2 as p2")
      .setRelationshipProperties("r", ("p1", "100"), ("p2", "100"))
      .directedRelationshipByIdSeek("r", "a", "b", Set.empty, r.getId)
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("p1", "p2").withSingleRow(100, 100).withStatistics(propertiesSet = 2)
  }

  test("should set relationship properties between two loops with continuation") {
    val rels = given {
      val (_, rs) = circleGraph(sizeHint)
      rs.foreach(r => {
        r.setProperty("p1", 0)
        r.setProperty("p2", 0)
      })
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .nonFuseable()
      .unwind(s"range(1, 10) AS r2")
      .setRelationshipProperties("r", ("p1", "r.p1 + 1"), ("p2", "r.p2 + 1"))
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build(readOnly = false)

    // then
    val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
    runtimeResult should beColumns("r")
      .withRows(singleColumn(rels.flatMap(r => Seq.fill(10)(r))))
      .withStatistics(propertiesSet = 2 * sizeHint)
    rels.map(_.getProperty("p1")).foreach(i => i should equal(1))
    rels.map(_.getProperty("p2")).foreach(i => i should equal(1))
  }
}
