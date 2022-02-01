/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

import scala.util.Random

abstract class CachePropertiesTestBase[CONTEXT <: RuntimeContext](
                                                                   edition: Edition[CONTEXT],
                                                                   runtime: CypherRuntime[CONTEXT],
                                                                   sizeHint: Int,
                                                                   protected val tokenLookupDbHits: Int
                                                                 ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {
  test("should not explode on cached properties") {
    // given
    val nodes = given { nodePropertyGraph(sizeHint, { case i => Map("p" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filter("cache[n.p] < 20 == 0")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.take(20).map(n => Array(n))
    runtimeResult should beColumns("n").withRows(expected)
  }

  test("node index exact seek should cache properties") {
    given {
      nodeIndex("A", "prop")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i % 3) }, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .union()
      .|.filter("cache[a.prop] IN [1, 2]")
      .|.nodeIndexOperator("a:A(prop = 2)", getValue = _ => GetValue)
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }


  test("node index range seek should cache properties") {
    given {
      nodeIndex("A", "prop")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i % 3) }, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .union()
      .|.filter("cache[a.prop] IN [1, 2]")
      .|.nodeIndexOperator("a:A(prop >= 2)", getValue = _ => GetValue)
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("node asserting multi index exact seek should cache properties") {
    given {
      nodeIndex("A", "prop")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i) }, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .union()
      .|.filter("cache[a.prop] IN [1, 2]")
      .|.assertSameNode("a")
      .|.|.nodeIndexOperator("a:A(prop = 2)", getValue = _ => GetValue)
      .|.nodeIndexOperator("a:A(prop = 2)", getValue = _ => GetValue)
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("many node index exact seek should cache properties") {
    given {
      nodeIndex("A", "prop")
      nodePropertyGraph(sizeHint, { case i => Map("prop" -> i % 3) }, "A")
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("a")
      .union()
      .|.filter("cache[a.prop] IN [1, 2]")
      .|.nodeIndexOperator("a:A(prop = 2 OR 3)", getValue = _ => GetValue)
      .nodeByLabelScan("a", "A")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("directed relationship index exact seek should cache properties") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint, "A")
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 3)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.filter("cacheR[r.prop] IN [1, 2]")
      .|.relationshipIndexOperator("(a)-[r:R(prop=2)]->(b)", getValue = _ => GetValue)
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("undirected relationship index exact seek should cache properties") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint, "A")
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 3)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.filter("cacheR[r.prop] IN [1, 2]")
      .|.relationshipIndexOperator("(a)-[r:R(prop=2)]-(b)", getValue = _ => GetValue)
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

    test("directed relationship index range seek should cache properties") {
      given {
        relationshipIndex("R", "prop")
        val (_, rels) = circleGraph(sizeHint, "A")
        rels.zipWithIndex.foreach {
          case (r, i) => r.setProperty("prop", i % 3)
        }
      }

      // when
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("r")
        .union()
        .|.filter("cacheR[r.prop] IN [1, 2]")
        .|.relationshipIndexOperator("(a)-[r:R(prop>=2)]->(b)", getValue = _ => GetValue)
        .relationshipTypeScan("(a)-[r:R]->(b)")
        .build()

      val runtimeResult = profile(logicalQuery, runtime)
      consume(runtimeResult)

      // then
      val queryProfile = runtimeResult.runtimeResult.queryProfile()
      queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
    }

  test("undirected relationship index range seek should cache properties") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint, "A")
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 3)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.filter("cacheR[r.prop] IN [1, 2]")
      .|.relationshipIndexOperator("(a)-[r:R(prop>=2)]-(b)", getValue = _ => GetValue)
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("directed relationship multi index range seek should cache properties") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint, "A")
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 3)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.filter("cacheR[r.prop] IN [1, 2]")
      .|.relationshipIndexOperator("(a)-[r:R(prop = 2 OR 3)]->(b)", getValue = _ => GetValue)
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("undirected relationship multi index range seek should cache properties") {
    given {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint, "A")
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i % 3)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.filter("cacheR[r.prop] IN [1, 2]")
      .|.relationshipIndexOperator("(a)-[r:R(prop = 2 OR 3)]-(b)", getValue = _ => GetValue)
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("handle cached properties in node index seek on the RHS of an apply") {
    // given
    val b = given {
      nodeIndex("B", "id")
      nodePropertyGraph(
        sizeHint, { case i => Map("id" -> i)}, "A")
      nodePropertyGraph(1, {case _ => Map("id" -> 1)}, "B").head
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("b")
      .apply()
      .|.nodeIndexOperator("b:B(id = ???)", paramExpr = Some(cachedNodeProp("a", "id")), argumentIds = Set("b"))
      .nodeByLabelScan("a", "A")
      .build()
    val runtimeResult = execute(logicalQuery, runtime)

    // then
    runtimeResult should beColumns("b").withSingleRow(b)
  }

  test("should handle missing long entities") {
    // given
    val size = 10
    val nodes = given { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("cacheN[m.p] AS x", "cacheR[r.p] AS y")
      .cacheProperties("cacheN[m.p]", "cacheR[r.p]")
      .optionalExpandAll("(n)-[r]->(m)")
      .input(nodes = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(nodes.map(n => Array[Any](n)):_*))

    // then
    val expected = nodes.map(_ => Array(null, null))
    runtimeResult should beColumns("x", "y").withRows(expected)
  }

  test("should handle missing ref entities") {
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("cacheN[n.p] AS x", "cacheR[r.p] AS y")
      .cacheProperties("cacheN[n.p]", "cacheR[r.p]")
      .input(variables = Seq("n", "r"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(Array(null, null)))

    // then
    runtimeResult should beColumns("x", "y").withSingleRow(null, null)
  }

  test("should handle missing property token") {
    // given
    val size = 10
    val nodes = given { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.map(_ => Array(null))
    runtimeResult should beColumns("x").withRows(expected)
  }

  test("should handle missing property token becoming created") {
    // given
    val node = given { nodeGraph(1) }.head

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val profiled = profile(logicalQuery, runtime)
    profiled should beColumns("x").withSingleRow(null)
    profiled.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe tokenLookupDbHits

    // when
    given { node.setProperty("p", 42L) }

    // then
    val queryProfile = profile(logicalQuery, runtime)
    queryProfile should beColumns("x").withSingleRow(42L)
    queryProfile.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0
    queryProfile.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be >0L
  }

  test("should handle missing property value") {
    // given
    val size = 10
    given { nodePropertyGraph(size, { case i => if (i % 2 == 0) Map() else Map("p" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array(null, 1, null, 3, null, 5, null, 7, null, 9)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should handle mix of missing and existing property tokens") {
    // given
    val numProps = 100
    val numStoreProperties = 51
    val node = given { nodePropertyGraph(1, {case _ => Map((0 until numStoreProperties).map(i => "p"+i -> i):_*) }) }.head
    val random = new Random()
    def inAnyOrder[T](f: Int => T) =
      random.shuffle((0 until numProps).map(f))

    val resultColumns = inAnyOrder(i => i)
    val resultColumnNames = resultColumns.map(i => s"x$i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(resultColumnNames:_*)
      .projection(inAnyOrder(i => s"cache[n.p$i] AS x$i"):_*)
      .cacheProperties(inAnyOrder(i => s"cache[n.p$i]"):_*)
      .allNodeScan("n")
      .build()

    // when
    val result1 = profile(logicalQuery, runtime)

    // then
    result1 should beColumns(resultColumnNames:_*).withSingleRow(resultColumns.map(i => if (i < numStoreProperties) i else null):_*)
    result1.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe tokenLookupDbHits * (numProps - numStoreProperties)
    val dbHitsOp2 = result1.runtimeResult.queryProfile().operatorProfile(2).dbHits()
   if (tokenLookupDbHits == 1) {
     dbHitsOp2 shouldBe numProps // Slotted and interpreted count one db hit even if property does not exist
   } else {
     dbHitsOp2 should be >= 0L
   }

    // when
    val numTxProperties = 25
    val numExistingProperties = numStoreProperties + numTxProperties
    given {
      (numStoreProperties until numExistingProperties).map(i => node.setProperty("p"+i, i))
    }
    val result2 = profile(logicalQuery, runtime)

    // then
    result2 should beColumns(resultColumnNames:_*).withSingleRow(resultColumns.map(i => if (i < numExistingProperties) i else null):_*)
    result2.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe(tokenLookupDbHits * (numProps - numExistingProperties))
    val result2DbHitsOp2 = result2.runtimeResult.queryProfile().operatorProfile(2).dbHits()
    if (tokenLookupDbHits == 1) {
      result2DbHitsOp2 shouldBe numProps // Slotted and interpreted count one db hit even if property does not exist
    } else {
      result2DbHitsOp2 should be >= dbHitsOp2
    }
  }

  test("should cached node property existence") {
    // given
    val nodes = given { nodePropertyGraph(sizeHint, { case i if i % 2 == 0 => Map("p" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("nodeCachedHasProperty[n.p] IS NOT NULL AS exists")
      .filter("nodeCachedHasProperty[n.p] IS NOT NULL")
      .allNodeScan("n")
      .build()

    val result = profile(logicalQuery, runtime)

    // then
    val expected = nodes.filter(p => p.hasProperty("p")).map(_ => Array(true))
    result should beColumns("exists").withRows(expected)
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0
  }

  test("should cache node property existence on rhs of an apply") {
    // given
    val nodes = given { nodePropertyGraph(sizeHint, { case i if i % 2 == 0 => Map("p" -> i)}) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.filter("nodeCachedHasProperty[n.p] IS NOT NULL")
      .|.argument("n")
      .filter("nodeCachedHasProperty[n.p] IS NOT NULL")
      .allNodeScan("n")
      .build()

    val result = profile(logicalQuery, runtime)

    // then
    val expected = nodes.filter(p => p.hasProperty("p")).map(n => Array(n))
    result should beColumns("n").withRows(expected)
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() shouldBe 0
  }

  test("should cache relationship property existence") {
    // given
    val rels = given {
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach{
        case (r, i) if i % 2 == 0 => r.setProperty("prop", i)
        case _ => //do nothing
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("relCachedHasProperty[r.p] IS NOT NULL AS exists")
      .filter("relCachedHasProperty[r.p] IS NOT NULL")
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build()

    val result = profile(logicalQuery, runtime)

    // then
    val expected = rels.filter(p => p.hasProperty("p")).map(_ => Array(true))
    result should beColumns("exists").withRows(expected)
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0
  }

  test("should cache relationship property existence on rhs of an apply") {
    // given
    val rels = given {
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach{
        case (r, i) if i % 2 == 0 => r.setProperty("prop", i)
        case _ => //do nothing
      }
      rels
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.filter("relCachedHasProperty[r.p] IS NOT NULL")
      .|.argument("n")
      .filter("relCachedHasProperty[r.p] IS NOT NULL")
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build()

    val result = profile(logicalQuery, runtime)

    // then
    val expected = rels.filter(p => p.hasProperty("p")).map(r => Array(r))
    result should beColumns("r").withRows(expected)
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() shouldBe 0
  }
}

trait CachePropertiesTxStateTestBase[CONTEXT <: RuntimeContext] {
  self: CachePropertiesTestBase[CONTEXT] =>

  test("should not have any db hits if properties are in transaction state") {
    // given
    val node = given {  nodePropertyGraph(1, { case i =>  Map("p1" -> 1, "p2" -> 2)}) }.head

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p1] AS x")
      .cacheProperties("cache[n.p1]")
      .allNodeScan("n")
      .build()

    // when
    node.removeProperty("p1")
    val result = profile(logicalQuery, runtime)

    // then
    result should beColumns("x").withSingleRow(null)
    result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0
  }

  test("should not whole chain when one sought property has been removed in transaction state") {
    // given
    val node = given { nodePropertyGraph(1, { case i =>
      Map("removed" -> 1, "p2" -> 2, "middle" -> 3, "p4" -> 4, "p5" -> 5, "p6" -> 6) }) }.head

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("removed", "middle")
      .projection("cache[n.removed] AS removed", "cache[n.middle] AS middle")
      .cacheProperties("cache[n.removed]", "cache[n.middle]")
      .allNodeScan("n")
      .build()

    // when
    node.removeProperty("removed")
    val result = profile(logicalQuery, runtime)

    // then
    result should beColumns("removed", "middle").withSingleRow(null, 3)
     result.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be < 5L // less that num remaining props
  }

  test("should handle mix of properties in transaction state and store") {

    val numProps = 100
    val numStoreProperties = 51
    val node = given { nodePropertyGraph(1, {case _ => Map((0 until numStoreProperties).map(i => "p"+i -> i):_*) }) }.head
    val random = new Random()
    def inAnyOrder[T](f: Int => T) =
      random.shuffle((0 until numProps).map(f))

    val resultColumns = inAnyOrder(i => i)
    val resultColumnNames = resultColumns.map(i => s"x$i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(resultColumnNames:_*)
      .projection(inAnyOrder(i => s"cache[n.p$i] AS x$i"):_*)
      .cacheProperties(inAnyOrder(i => s"cache[n.p$i]"):_*)
      .allNodeScan("n")
      .build()

    // when
    val result1 = profile(logicalQuery, runtime)

    // then
    result1 should beColumns(resultColumnNames:_*).withSingleRow(resultColumns.map(i => if (i < numStoreProperties) i else null):_*)
    result1.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe(tokenLookupDbHits * (numProps - numStoreProperties))
    result1.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be > 0L

    // when
    (25 until 75).foreach(i => node.setProperty("p"+i, numProps+i))

    val result2 = profile(logicalQuery, runtime)

    // then
    result2 should beColumns(resultColumnNames:_*).withSingleRow(resultColumns
      .map(i =>
        if (i < 25) {
          i
        } else if (i < 75) {
          i + 100
        } else {
          null
        }
      ):_*)

    result2.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe (25 * tokenLookupDbHits)
    result2.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be > 0L
  }
}
