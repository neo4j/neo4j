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
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.runtime.NoInput
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.graphdb.Label

import scala.util.Random

abstract class CachePropertiesTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  sizeHint: Int,
  protected val tokenLookupDbHits: Int
) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should not explode on cached properties") {
    // given
    val nodes = givenGraph { nodePropertyGraph(sizeHint, { case i => Map("p" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .filter("cache[n.p] < 20")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = nodes.take(20).map(n => Array(n))
    runtimeResult should beColumns("n").withRows(expected)
  }

  test("node index exact seek should cache properties") {
    givenGraph {
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
    givenGraph {
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
    givenGraph {
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

  test("relationship asserting multi relationship index exact seek should cache properties") {
    givenGraph {
      relationshipIndex("R", "prop")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) => r.setProperty("prop", i)
      }
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .union()
      .|.filter("cacheR[r.prop] IN [1, 2]")
      .|.assertSameRelationship("r")
      .|.|.relationshipIndexOperator("(a)-[r:R(prop = 2)]->(b)", getValue = _ => GetValue)
      .|.relationshipIndexOperator("(a)-[r:R(prop = 2)]->(b)", getValue = _ => GetValue)
      .relationshipTypeScan("(a)-[r:R]->(b)")
      .build()

    val runtimeResult = profile(logicalQuery, runtime)
    consume(runtimeResult)

    // then
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(2).dbHits() shouldBe 0 // filter
  }

  test("many node index exact seek should cache properties") {
    givenGraph {
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
    givenGraph {
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
    givenGraph {
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
    givenGraph {
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
    givenGraph {
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
    givenGraph {
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
    givenGraph {
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
    val b = givenGraph {
      nodeIndex("B", "id")
      nodePropertyGraph(
        sizeHint,
        { case i => Map("id" -> i) },
        "A"
      )
      nodePropertyGraph(1, { case _ => Map("id" -> 1) }, "B").head
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
    val nodes = givenGraph { nodeGraph(size) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "y")
      .projection("cacheN[m.p] AS x", "cacheR[r.p] AS y")
      .cacheProperties("cacheN[m.p]", "cacheR[r.p]")
      .optionalExpandAll("(n)-[r]->(m)")
      .input(nodes = Seq("n"))
      .build()

    val runtimeResult = execute(logicalQuery, runtime, inputValues(nodes.map(n => Array[Any](n)): _*))

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
    val nodes = givenGraph { nodeGraph(size) }

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
    val node = givenGraph { nodeGraph(1) }.head

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
    givenGraph { node.setProperty("p", 42L) }

    // then
    val queryProfile = profile(logicalQuery, runtime)
    queryProfile should beColumns("x").withSingleRow(42L)
    queryProfile.runtimeResult.queryProfile().operatorProfile(1).dbHits() shouldBe 0
    queryProfile.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be > 0L
  }

  test("should handle missing property value") {
    // given
    val size = 10
    givenGraph { nodePropertyGraph(size, { case i => if (i % 2 == 0) Map() else Map("p" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x")
      .projection("cache[n.p] AS x")
      .cacheProperties("cache[n.p]")
      .allNodeScan("n")
      .build()

    val runtimeResult = execute(logicalQuery, runtime)

    // then
    val expected = Array[Any](null, 1, null, 3, null, 5, null, 7, null, 9)
    runtimeResult should beColumns("x").withRows(singleColumn(expected))
  }

  test("should handle token being created") {
    val node = givenGraph { nodePropertyGraph(1, { case _ => Map("x1" -> "1") }) }.head

    // given
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x1", "x2", "x3")
      .projection(s"cache[n.x1] AS x1", "cache[n.x2] AS x2", "cache[n.x3] AS x3")
      .cacheProperties("cache[n.x1]", "cache[n.x2]", "cache[n.x3]")
      .allNodeScan("n")
      .build()

    // when
    val executablePlan: ExecutionPlan = buildPlan(logicalQuery.copy(doProfile = true), runtime)
    val result1 = profile(executablePlan, NoInput, readOnly = true)

    // then
    result1 should beColumns("x1", "x2", "x3").withSingleRow("1", null, null)
    val profileResult1 = result1.runtimeResult.queryProfile()
    profileResult1.operatorProfile(1).dbHits() shouldBe 2 * tokenLookupDbHits // projection
    assert(profileResult1.operatorProfile(2).dbHits() >= (1 + 2 * tokenLookupDbHits)) // cache properties

    // when
    givenGraph {
      node.setProperty("x2", "2")
    }
    val result2 = profile(executablePlan, NoInput, readOnly = true)

    // then
    result2 should beColumns("x1", "x2", "x3").withSingleRow("1", "2", null)
    val profileResult2 = result2.runtimeResult.queryProfile()

    profileResult2.operatorProfile(1).dbHits() should be <= 2L * tokenLookupDbHits // projection
    assert(profileResult2.operatorProfile(2).dbHits() >= (2 + 2 * tokenLookupDbHits)) // cache properties
  }

  test("should handle mix of missing and existing property tokens") {
    // given
    val numProps = 100
    val numStoreProperties = 51
    val numUnresolvedPropertyTokens = numProps - numStoreProperties
    val node =
      givenGraph {
        nodePropertyGraph(1, { case _ => Map((0 until numStoreProperties).map(i => "p" + i -> i): _*) })
      }.head
    val random = new Random()
    def inAnyOrder[T](f: Int => T) =
      random.shuffle((0 until numProps).map(f))

    val resultColumns = inAnyOrder(i => i)
    val resultColumnNames = resultColumns.map(i => s"x$i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(resultColumnNames: _*)
      .projection(inAnyOrder(i => s"cache[n.p$i] AS x$i"): _*)
      .cacheProperties(inAnyOrder(i => s"cache[n.p$i]"): _*)
      .allNodeScan("n")
      .build()

    // when
    val executionPlan = buildPlan(logicalQuery.copy(doProfile = true), runtime)
    val resultNoTokenUpdates = profile(executionPlan, NoInput, readOnly = true)

    // then
    resultNoTokenUpdates should beColumns(resultColumnNames: _*).withSingleRow(resultColumns.map(i =>
      if (i < numStoreProperties) i else null
    ): _*)

    // DB hits in projection
    resultNoTokenUpdates.runtimeResult.queryProfile().operatorProfile(
      1
    ).dbHits() shouldBe tokenLookupDbHits * numUnresolvedPropertyTokens

    // DB hits in cache properties
    val cachePropsDBHits1 = resultNoTokenUpdates.runtimeResult.queryProfile().operatorProfile(2).dbHits()
    if (tokenLookupDbHits == 1) {
      // For existing properties: one db to get property value.
      // For properties which doesn't have a resolved token: 1 db hit to resolve token
      cachePropsDBHits1 shouldBe numStoreProperties + numUnresolvedPropertyTokens
    } else {
      cachePropsDBHits1 should be >= 0L
    }

    // when creating new properties
    val numTxProperties = 25
    val numExistingProperties = numStoreProperties + numTxProperties
    givenGraph {
      (numStoreProperties until numExistingProperties).map(i => node.setProperty("p" + i, i))
    }
    val resultTokenWithUpdates = profile(executionPlan, NoInput, readOnly = true)

    // then
    resultTokenWithUpdates should beColumns(resultColumnNames: _*).withSingleRow(resultColumns.map(i =>
      if (i < numExistingProperties) i else null
    ): _*)

    // DB hits in projection
    resultTokenWithUpdates.runtimeResult.queryProfile().operatorProfile(
      1
    ).dbHits() shouldBe tokenLookupDbHits * numUnresolvedPropertyTokens

    // DB hits in cache properties
    val cachePropsDBHits2 = resultTokenWithUpdates.runtimeResult.queryProfile().operatorProfile(2).dbHits()
    if (tokenLookupDbHits == 1) {
      // For existing properties: one db to get property value.
      // For properties which doesn't have a resolved token: 1 db hit to resolve token
      cachePropsDBHits2 shouldBe numExistingProperties + numUnresolvedPropertyTokens
    } else {
      cachePropsDBHits2 should be >= cachePropsDBHits1
    }
  }

  test("should cached node property existence") {
    // given
    val nodes = givenGraph { nodePropertyGraph(sizeHint, { case i if i % 2 == 0 => Map("p" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("cacheNHasProperty[n.p] IS NOT NULL AS exists")
      .filter("cacheNHasProperty[n.p] IS NOT NULL")
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
    val nodes = givenGraph { nodePropertyGraph(sizeHint, { case i if i % 2 == 0 => Map("p" -> i) }) }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("n")
      .apply()
      .|.filter("cacheNHasProperty[n.p] IS NOT NULL")
      .|.argument("n")
      .filter("cacheNHasProperty[n.p] IS NOT NULL")
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
    val rels = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("prop", i)
        case _                    => // do nothing
      }
      rels
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("exists")
      .projection("cacheRHasProperty[r.p] IS NOT NULL AS exists")
      .filter("cacheRHasProperty[r.p] IS NOT NULL")
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
    val rels = givenGraph {
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("prop", i)
        case _                    => // do nothing
      }
      rels
    }
    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r")
      .apply()
      .|.filter("cacheRHasProperty[r.p] IS NOT NULL")
      .|.argument("n")
      .filter("cacheRHasProperty[r.p] IS NOT NULL")
      .relationshipTypeScan("(n)-[r:R]->(m)")
      .build()

    val result = profile(logicalQuery, runtime)

    // then
    val expected = rels.filter(p => p.hasProperty("p")).map(r => Array(r))
    result should beColumns("r").withRows(expected)
    result.runtimeResult.queryProfile().operatorProfile(2).dbHits() shouldBe 0
  }

  test("should handle duplicated cached properties on rhs of nested cartesian product") {

    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p3")
      .projection("cache[n3.p3] as p3")
      .cacheProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.cartesianProduct()
      .|.|.|.filter("cache[n1.p2] = 11")
      .|.|.|.nodeByLabelScan("n3", "C")
      .|.|.cacheProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.filter("n2.p = 20")
      .|.allNodeScan("n2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p3").withSingleRow(30)
  }

  test("should handle duplicated cached properties on rhs of cartesian product with additional slots") {

    givenGraph {
      val n1 = tx.createNode(Label.label("A"))
      n1.setProperty("p", 10)
      n1.setProperty("p2", 11)
      val n2 = tx.createNode(Label.label("A"))
      n2.setProperty("p", 20)
      val n3 = tx.createNode(Label.label("C"))
      n3.setProperty("p3", 30)
    }

    val query = new LogicalQueryBuilder(this)
      .produceResults("p3")
      .projection("cache[n3.p3] as p3")
      .cacheProperties("cache[n3.p3]")
      .apply()
      .|.cartesianProduct()
      .|.|.projection("2 as p5")
      .|.|.cartesianProduct()
      .|.|.|.projection("1 as p4")
      .|.|.|.filter("cache[n1.p2] = 11")
      .|.|.|.nodeByLabelScan("n3", "C")
      .|.|.cacheProperties("cache[n1.p2]")
      .|.|.argument("n1")
      .|.filter("n2.p = 20")
      .|.allNodeScan("n2")
      .allNodeScan("n1")
      .build()

    val result = execute(query, runtime)

    result should beColumns("p3").withSingleRow(30)
  }
}

trait CachePropertiesTxStateTestBase[CONTEXT <: RuntimeContext] {
  self: CachePropertiesTestBase[CONTEXT] =>

  test("should not have any db hits if properties are in transaction state") {
    // given
    val node = givenGraph { nodePropertyGraph(1, { case i => Map("p1" -> 1, "p2" -> 2) }) }.head

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
    val node = givenGraph {
      nodePropertyGraph(
        1,
        { case i =>
          Map("removed" -> 1, "p2" -> 2, "middle" -> 3, "p4" -> 4, "p5" -> 5, "p6" -> 6)
        }
      )
    }.head

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
    val node =
      givenGraph {
        nodePropertyGraph(1, { case _ => Map((0 until numStoreProperties).map(i => "p" + i -> i): _*) })
      }.head
    val random = new Random()
    def inAnyOrder[T](f: Int => T) =
      random.shuffle((0 until numProps).map(f))

    val resultColumns = inAnyOrder(i => i)
    val resultColumnNames = resultColumns.map(i => s"x$i")

    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults(resultColumnNames: _*)
      .projection(inAnyOrder(i => s"cache[n.p$i] AS x$i"): _*)
      .cacheProperties(inAnyOrder(i => s"cache[n.p$i]"): _*)
      .allNodeScan("n")
      .build()

    // when
    val result1 = profile(logicalQuery, runtime)

    // then
    result1 should beColumns(resultColumnNames: _*).withSingleRow(resultColumns.map(i =>
      if (i < numStoreProperties) i else null
    ): _*)
    result1.runtimeResult.queryProfile().operatorProfile(
      1
    ).dbHits() shouldBe (tokenLookupDbHits * (numProps - numStoreProperties))
    result1.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be > 0L

    // when
    (25 until 75).foreach(i => node.setProperty("p" + i, numProps + i))

    val result2 = profile(logicalQuery, runtime)

    // then
    result2 should beColumns(resultColumnNames: _*).withSingleRow(resultColumns
      .map(i =>
        if (i < 25) {
          i
        } else if (i < 75) {
          i + 100
        } else {
          null
        }
      ): _*)

    result2.runtimeResult.queryProfile().operatorProfile(1).dbHits() should be >= (25L * tokenLookupDbHits)
    result2.runtimeResult.queryProfile().operatorProfile(2).dbHits() should be > 0L
  }
}
