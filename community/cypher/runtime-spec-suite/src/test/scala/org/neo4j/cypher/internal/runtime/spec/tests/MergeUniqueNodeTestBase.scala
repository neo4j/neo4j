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
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder.nestedPlanCollectExpression
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.helpers.collection.Iterators
import org.neo4j.lock.LockType.EXCLUSIVE
import org.neo4j.lock.LockType.SHARED
import org.neo4j.lock.ResourceType
import org.neo4j.lock.ResourceType.INDEX_ENTRY
import org.neo4j.lock.ResourceType.LABEL

import scala.util.Random

abstract class MergeUniqueNodeTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  protected val sizeHint: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  // allows using shouldBe matcher with Object on LHS and primitive type on RHS
  given CanEqual[AnyRef, AnyVal] = CanEqual.derived

  test("should grab shared lock when finding a node") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> s"$propToFind"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(propToFind)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    ).withNoUpdates()
  }

  test("should grab shared lock when finding a node (multiple properties)") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop1" -> i, "prop2" -> s"$i")
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop1" -> s"$propToFind", "prop2" -> s"'$propToFind'"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(propToFind)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    ).withNoUpdates()
  }

  test("should grab an exclusive lock when not finding a node") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = sizeHint + 1

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> s"$propToFind"))
      .build(readOnly = false)

    // then
    execute(logicalQuery, runtime) should beColumns("x").withRows(rowCount(1)).withLocks(
      (EXCLUSIVE, ResourceType.NODE),
      (EXCLUSIVE, INDEX_ENTRY),
      (SHARED, LABEL),
      (SHARED, LABEL),
      (EXCLUSIVE, ResourceType.NODE_RELATIONSHIP_GROUP_DELETE)
    ).withStatistics(nodesCreated = 1, labelsAdded = 1, propertiesSet = 1)
    execute(logicalQuery, runtime) should beColumns("x").withRows(rowCount(1))
      .withNoUpdates()
  }

  test("should exact seek nodes of a locking unique index with a property") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> "20"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(20)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks(
      (SHARED, INDEX_ENTRY),
      (SHARED, LABEL)
    ).withNoUpdates()
  }

  test("should support composite unique index and unique locking") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop", "prop2")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i, "prop2" -> i.toString)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> "10", "prop2" -> "'10'"))
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes(10)
    runtimeResult should beColumns("x").withSingleRow(expected).withLocks((SHARED, INDEX_ENTRY), (SHARED, LABEL))
  }

  test("mergeUnique should perform on match side effect") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode(
        "x",
        "Honey",
        Seq("prop" -> s"$propToFind"),
        onMatch = Seq("p1" -> "true", "p2" -> "42"),
        onCreate = Seq("p1" -> "false", "p2" -> "'forty-two'")
      )
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val expected = nodes(propToFind)
    expected.getProperty("p1") shouldBe true
    expected.getProperty("p2") shouldBe 42
    runtimeResult should beColumns("x").withSingleRow(expected).withStatistics(propertiesSet = 2)
  }

  test("mergeUnique should perform on create side effect") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode(
        "x",
        "Honey",
        Seq("prop" -> s"$sizeHint"),
        onMatch = Seq("p1" -> "true", "p2" -> "42"),
        onCreate = Seq("p1" -> "false", "p2" -> "'forty-two'")
      )
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val newNode = Iterators.single(tx.findNodes(Label.label("Honey"), "prop", sizeHint))
    newNode.getProperty("p1") shouldBe false
    newNode.getProperty("p2") shouldBe "forty-two"
    runtimeResult should beColumns("x").withSingleRow(newNode).withStatistics(
      nodesCreated = 1,
      labelsAdded = 1,
      propertiesSet = 3
    )
  }

  test("profile dbhits on match") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> s"$propToFind"))
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() should equal(2)
  }

  test("profile dbhits on create") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> s"$sizeHint"))
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() should equal(2)
  }

  test("profile rows on match") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }
    val propToFind = Random.nextInt(sizeHint)

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> s"$propToFind"))
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).rows() should equal(1)
  }

  test("profile rows on create") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .filter("true")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> s"$sizeHint"))
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).rows() should equal(1)
  }

  test("should cache properties in mergeUniqueNode") {
    val nodes = givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop")
      nodeGraph(5, "Milk")
      nodePropertyGraph(
        sizeHint,
        {
          case i if i % 10 == 0 => Map("prop" -> i)
        },
        "Honey"
      )
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .mergeUniqueNode("x", "Honey", Seq("prop" -> "10"), cacheValues = true)
      .build(readOnly = false)

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    runtimeResult should beColumns("x", "prop").withSingleRow(nodes(10), 10)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() shouldBe 0
  }

  test("should only create one node for non-composite mergeUnique") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.mergeUniqueNode("x", "Honey", Seq("prop1" -> "1"))
      .unwind("range(1, 100) AS i")
      .argument()
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(100)).withStatistics(
      nodesCreated = 1,
      labelsAdded = 1,
      propertiesSet = 1
    )
  }

  test("should only create one node for multi-property composite mergeUnique") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2", "prop3", "prop4", "prop5")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.mergeUniqueNode(
        "x",
        "Honey",
        Seq("prop1" -> "1", "prop2" -> "2", "prop3" -> "3", "prop4" -> "4", "prop5" -> "5")
      )
      .unwind("range(1, 100) AS i")
      .argument()
      .build(readOnly = false)

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("x").withRows(rowCount(100)).withStatistics(
      nodesCreated = 1,
      labelsAdded = 1,
      propertiesSet = 5
    )
  }

  test("should not create existing nodes for composite") {
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Honey", "prop1", "prop2", "prop3", "prop4", "prop5")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .apply()
      .|.mergeUniqueNode(
        "x",
        "Honey",
        Seq("prop1" -> "i[0]", "prop2" -> "i[1]", "prop3" -> "i[2]", "prop4" -> "i[3]", "prop5" -> "i[4]")
      )
      .input(variables = Seq("i"))
      .build(readOnly = false)

    // Create all combinations where prop1 can have two values, prop2 three values, etc, so there should be 2 * 3 * 4 * 5 * 6 = 720 unique combinations,
    // but with lots of duplicate entries. Use a fixed seed, just in the very unlikely case we don't generate all the 720 combinations for some seed.
    val random = new Random(11)
    val inputs = (1 to 10000).map(_ =>
      Array[Any](Array(random.nextInt(2), random.nextInt(3), random.nextInt(4), random.nextInt(5), random.nextInt(6)))
    )

    val runtimeResult = execute(logicalQuery, runtime, inputValues(inputs: _*))

    // then
    runtimeResult should beColumns("x").withRows(rowCount(10000)).withStatistics(
      nodesCreated = 720,
      labelsAdded = 720,
      propertiesSet = 720 * 5
    )
  }

  test("mergeUnique should handle complicated nested expression in onCreate/onMatch") {
    // we expect fusing to fail since we don't support NestedPlanExpression
    assume(!canFuse)

    // given
    givenGraph {
      uniqueNodeIndex(IndexType.RANGE, "Label", "prop")
      val Seq(a) = nodeGraph(1, "A")
      nodeGraph(1, "B")
      val Seq(c1, c2, c3) = nodeGraph(3, "C")
      a.createRelationshipTo(c1, RelationshipType.withName("NOT_NEXT"))
      a.createRelationshipTo(c2, RelationshipType.withName("NOT_NEXT"))
      a.createRelationshipTo(c3, RelationshipType.withName("NOT_NEXT"))
      c1.setProperty("p1", "Just")
      c1.setProperty("p2", "It's")
      c2.setProperty("p1", "got")
      c2.setProperty("p2", "me")
      c3.setProperty("p1", "here")
      c3.setProperty("p2", "again")
    }
    // NOTE: we must use the same idGen in subplans, otherwise we will try to
    //      change attributes multiple times in
    val idGen = new SequentialIdGen()
    // when
    val logicalQuery = new LogicalQueryBuilder(this, idGen = idGen)
      .produceResults("res")
      .projection("x.newProp AS res")
      .apply()
      .|.mergeUniqueNodeExpression(
        "x",
        "Label",
        Seq("prop" -> "42"),
        onCreate = Seq("newProp" -> nestedPlanCollectExpression(
          new LogicalQueryBuilder(this, wholePlan = false, idGen = idGen)
            .expand("(a)-[:NOT_NEXT]->(c)")
            .nodeByLabelScan("a", "A"),
          prop("c", "p1")
        )),
        onMatch = Seq("newProp" -> nestedPlanCollectExpression(
          new LogicalQueryBuilder(this, wholePlan = false, idGen = idGen)
            .expand("(a)-[:NOT_NEXT]->(c)")
            .nodeByLabelScan("a", "A"),
          prop("c", "p2")
        )),
        args = Set("x", "y", "ONE", "TWO")
      )
      .projection("x AS x", "y AS y", "1 AS ONE", "2 AS TWO")
      .cartesianProduct()
      .|.nodeByLabelScan("y", "B")
      .nodeByLabelScan("x", "A")
      .build(readOnly = false)

    // then
    execute(
      logicalQuery,
      runtime
    ) should beColumns("res").withRows(Seq(Array(Array("Just", "got", "here"))), listInAnyOrder = true).withStatistics(
      nodesCreated = 1,
      labelsAdded = 1,
      propertiesSet = 2
    )

    execute(
      logicalQuery,
      runtime
    ) should beColumns("res").withRows(
      Seq(Array(Array("It's", "me", "again"))),
      listInAnyOrder = true
    ).withStatistics(propertiesSet = 1)
  }
}

object MergeUniqueNodeTestBase
