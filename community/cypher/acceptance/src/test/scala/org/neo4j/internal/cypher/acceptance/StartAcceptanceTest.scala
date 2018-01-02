/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, QueryStatisticsTestSupport}

class StartAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("START r=rel(0) RETURN r") {
    val rel = relate(createNode(), createNode())
    val result = executeWithRulePlanner("START r=rel(0) RETURN r").toList

    result should equal(List(Map("r"-> rel)))
  }

  test("START r=rel:index(key = \"value\") RETURN r") {
    val rel = relate(createNode(), createNode())
    graph.inTx {
      graph.index.forRelationships("index").add(rel, "key", "value")
    }

    val result = executeWithRulePlanner("""START r=rel:index(key = "value") RETURN r""").toList

    result should equal(List(Map("r"-> rel)))
  }

  test("START n=node(0) RETURN n") {
    val node = createNode()
    val result = executeWithRulePlanner("START n=node(0) RETURN n").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START n=node:index(key = \"value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
    }

    val result = executeWithAllPlanners("""START n=node:index(key = "value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("START r=rel:index(\"key:value\") RETURN r") {
    val rel = relate(createNode(), createNode())
    graph.inTx {
      graph.index.forRelationships("index").add(rel, "key", "value")
    }

    val result = executeWithRulePlanner("""START r=rel:index("key:value") RETURN r""").toList

    result should equal(List(Map("r"-> rel)))
  }

  test("START n=node:index(\"key:value\") RETURN n") {
    val node = createNode()
    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
    }

    val result = executeWithAllPlanners("""START n=node:index("key:value") RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("start + where") {
    val node = createNode(Map("prop" -> 42))
    val otherNode = createNode(Map("prop" -> 21))

    graph.inTx {
      graph.index.forNodes("index").add(node, "key", "value")
      graph.index.forNodes("index").add(otherNode, "key", "value")
    }

    val result = executeWithAllPlanners("""START n=node:index("key:value") WHERE n.prop = 42 RETURN n""").toList

    result should equal(List(Map("n"-> node)))
  }

  test("Should return unique relationship on rel-id") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val result = executeWithRulePlanner(
      """start a=node(0), ab=relationship(0)
        |match (a)-[ab]->(b)
        |return b
      """.stripMargin)

    result.toList should equal(List(Map("b" -> b)))
  }

  test("Should return unique node on node-id") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val result = executeWithRulePlanner(
      """start a=node(0), b=node(1)
        |match (a)-[ab]->(b)
        |return b
      """.stripMargin)

    result.toList should equal(List(Map("b" -> b)))
  }

  test("Should return unique relationship on multiple rel-id") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val e = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val bd = relate(b, d)
    val ce = relate(c, e)
    val result = executeWithRulePlanner(
      """start a=node(0), ab=relationship(0), bd=relationship(2)
        |match (a)-[ab]->(b)-[bd]->(d)
        |return b, d
      """.stripMargin)

    result.toList should equal(List(Map("b" -> b, "d" -> d)))
  }

  test("Should return unique relationship on rel-ids") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val ab = relate(a, b)
    val ac = relate(a, c)
    val ad = relate(a, d)
    val result = executeWithRulePlanner(
      """start a=node(0), ab=relationship(0, 1)
        |match (a)-[ab]->(b)
        |return b
      """.stripMargin)

    result.toSet should equal(Set(Map("b" -> c), Map("b" -> b)))
  }

  test("should return correct results on combined node and relationship index starts") {
    val node = createNode()
    val resultNode = createNode()
    val rel = relate(node, resultNode)
    relate(node, createNode())

    graph.inTx {
      graph.index.forNodes("nodes").add(node, "key", "A")
      graph.index.forRelationships("rels").add(rel, "key", "B")
    }

    val result = executeWithRulePlanner("START n=node:nodes(key = 'A'), r=rel:rels(key = 'B') MATCH (n)-[r]->(b) RETURN b")
    result.toList should equal(List(Map("b" -> resultNode)))
  }
}
