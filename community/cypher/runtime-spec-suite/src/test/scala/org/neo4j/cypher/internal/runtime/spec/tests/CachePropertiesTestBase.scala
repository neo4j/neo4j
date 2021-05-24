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

abstract class CachePropertiesTestBase[CONTEXT <: RuntimeContext](
                                                                   edition: Edition[CONTEXT],
                                                                   runtime: CypherRuntime[CONTEXT],
                                                                   sizeHint: Int
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
}
