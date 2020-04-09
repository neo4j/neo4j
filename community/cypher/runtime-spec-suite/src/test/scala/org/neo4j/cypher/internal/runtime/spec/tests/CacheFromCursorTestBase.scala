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
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite

abstract class CacheFromCursorTestBase[CONTEXT <: RuntimeContext](
                                                                   edition: Edition[CONTEXT],
                                                                   runtime: CypherRuntime[CONTEXT],
                                                                   sizeHint: Int
                                                                 ) extends RuntimeTestSuite[CONTEXT](edition, runtime) {

  test("should cache node properties in expand") {
    // given
    val nodes = given {
      val (ns, _) = circleGraph(sizeHint, "A")
      for (n <- ns) {
        n.setProperty("prop", n.getId)
      }
      ns
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .expand("(x)-[r]->(y)", cacheNodeProperties = Seq("prop"))
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n.getId))
    runtimeResult should beColumns("x", "prop").withRows(expected)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(0L) //projection
  }

  test("should cache relationship properties in expand") {
    // given
    val relationships = given {
      val (_, rs) = circleGraph(sizeHint, "A")
      for (r <- rs) {
        r.setProperty("prop", r.getId)
      }
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .expand("(x)-[r]->(y)", cacheRelProperties = Seq("prop"))
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val expected = relationships.map(r => Array(r, r.getId))
    runtimeResult should beColumns("r", "prop").withRows(expected)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(0L) //projection
  }

  test("should cache multiple node properties and relationship cursors in expand") {
    // given
    val nodes = given {
      val (ns, rs) = circleGraph(sizeHint, "A")
      for (n <- ns) {
        n.setProperty("prop1", "nodeProp1")
        n.setProperty("prop2", "nodeProp2")
      }
      for (r <- rs) {
        r.setProperty("prop1", "relProp1")
        r.setProperty("prop2", "relProp2")
      }
      ns
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("np1", "np2", "rp1", "rp2")
      .projection("cache[x.prop1] AS np1", "cache[x.prop2] AS np2", "cacheR[r.prop1] AS rp1", "cacheR[r.prop2] AS rp2")
      .expand("(x)-[r]->(y)", cacheNodeProperties = Seq("prop1", "prop2"), cacheRelProperties = Seq("prop1", "prop2"))
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array("nodeProp1", "nodeProp2", "relProp1", "relProp2"))
    runtimeResult should beColumns("np1", "np2", "rp1", "rp2").withRows(expected)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(0L) //projection
  }

  test("should cache node properties in optionalExpand") {
    // given
    val nodes = given {
      val (ns, _) = circleGraph(sizeHint, "A")
      for (n <- ns) {
        n.setProperty("prop", n.getId)
      }
      ns
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("x", "prop")
      .projection("cache[x.prop] AS prop")
      .optionalExpandAll("(x)-[r]->(y)", cacheNodeProperties = Seq("prop"))
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array(n, n.getId))
    runtimeResult should beColumns("x", "prop").withRows(expected)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(0L) //projection
  }

  test("should cache relationship properties in optionalRxpand") {
    // given
    val relationships = given {
      val (_, rs) = circleGraph(sizeHint, "A")
      for (r <- rs) {
        r.setProperty("prop", r.getId)
      }
      rs
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("r", "prop")
      .projection("cacheR[r.prop] AS prop")
      .optionalExpandAll("(x)-[r]->(y)", cacheRelProperties = Seq("prop"))
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val expected = relationships.map(r => Array(r, r.getId))
    runtimeResult should beColumns("r", "prop").withRows(expected)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(0L) //projection
  }

  test("should cache multiple node properties and relationship cursors in optionalExpand") {
    // given
    val nodes = given {
      val (ns, rs) = circleGraph(sizeHint, "A")
      for (n <- ns) {
        n.setProperty("prop1", "nodeProp1")
        n.setProperty("prop2", "nodeProp2")
      }
      for (r <- rs) {
        r.setProperty("prop1", "relProp1")
        r.setProperty("prop2", "relProp2")
      }
      ns
    }

    // when
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("np1", "np2", "rp1", "rp2")
      .projection("cache[x.prop1] AS np1", "cache[x.prop2] AS np2", "cacheR[r.prop1] AS rp1", "cacheR[r.prop2] AS rp2")
      .optionalExpandAll("(x)-[r]->(y)", cacheNodeProperties = Seq("prop1", "prop2"), cacheRelProperties = Seq("prop1", "prop2"))
      .nodeByLabelScan("x", "A", IndexOrderNone)
      .build()

    val runtimeResult = profile(logicalQuery, runtime)

    // then
    val expected = nodes.map(n => Array("nodeProp1", "nodeProp2", "relProp1", "relProp2"))
    runtimeResult should beColumns("np1", "np2", "rp1", "rp2").withRows(expected)
    val queryProfile = runtimeResult.runtimeResult.queryProfile()
    queryProfile.operatorProfile(1).dbHits() should equal(0L) //projection
  }
}
