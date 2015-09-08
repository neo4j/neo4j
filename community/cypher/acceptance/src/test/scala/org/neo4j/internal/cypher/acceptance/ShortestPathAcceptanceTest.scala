/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}
import org.neo4j.graphdb.Node

class ShortestPathAcceptanceTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  var nodeA: Node = null
  var nodeB: Node = null
  var nodeC: Node = null
  var nodeD: Node = null

  override protected def initTest(): Unit = {
    super.initTest()
    nodeA = createLabeledNode("A")
    nodeB = createLabeledNode("B")
    nodeC = createLabeledNode("C")
    nodeD = createLabeledNode("D")
  }

  test("finds shortest path that fulfills predicate on nodes") {
    /* a-b-c-d */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    relate(nodeA, nodeX)
    relate(nodeX, nodeD)

    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:A)-[*]->(dst:D))
        | WHERE NONE(n in nodes(p) WHERE n:X)
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    result should equal(List(List(nodeA, nodeB, nodeC, nodeD)))
  }

  test("finds shortest path that fulfills predicate on relationships 1") {
    /* a-b-c-d */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    relate(nodeA, nodeX, "blocked" -> true)
    relate(nodeX, nodeD, "blocked" -> true)

    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:A)-[*]->(dst:D))
        | WHERE NONE(r in rels(p) WHERE exists(r.blocked))
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    result should equal(List(List(nodeA, nodeB, nodeC, nodeD)))
  }

  test("finds shortest path that fulfills predicate on relationships 2") {
    /* a-b-c-d */
    relate(nodeA, nodeB, "blocked" -> false)
    relate(nodeB, nodeC, "blocked" -> false)
    relate(nodeC, nodeD, "blocked" -> false)

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    relate(nodeA, nodeX, "blocked" -> true)
    relate(nodeX, nodeD, "blocked" -> true)

    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:A)-[rs*]->(dst:D))
        | WHERE NONE(r in rs WHERE r.blocked)
        |RETURN nodes(p) AS nodes""".stripMargin)

    result.columnAs[List[Node]]("nodes").toList should equal(List(List(nodeA, nodeB, nodeC, nodeD)))
  }

  test("finds shortest path that fulfills predicate on path") {
    /* a-b-c-d */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    relate(nodeA, nodeX)
    relate(nodeX, nodeD)

    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:A)-[rs*]->(dst:D))
        | WHERE length(p) % 2 = 1 // Only uneven paths wanted!
        |RETURN nodes(p) AS nodes""".stripMargin)

    result.columnAs[List[Node]]("nodes").toList should equal(List(List(nodeA, nodeB, nodeC, nodeD)))

  }

  test("shortest path shouldn't lose context information at runtime") {

    val query =
      """MATCH (src:A), (dest:D)
        |MATCH p = shortestPath((src)-[rs*]->(dest))
        |WHERE ALL(r in rs WHERE type(rs[0]) = type(r)) AND ALL(r in rs WHERE r.blocked <> true)
        |RETURN p
      """.stripMargin

    val result = executeWithAllPlanners(query)
  }

  test("should still be able to return shortest path expression") {
    /* a-b-c-d */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    val r1 = relate(nodeA, nodeX)
    var r2 = relate(nodeX, nodeD)

    val result = executeWithAllPlanners("MATCH (src:A), (dst:D) RETURN shortestPath((src:A)-[*]->(dst:D)) as path")

    graph.inTx {
      result.columnAs("path").toList should equal(List(PathImpl(nodeA, r1, nodeX, r2, nodeD)))
    }
  }

  test("finds shortest path that fulfills predicate on all relationships") {
    /* a-b-c-d */
    relate(nodeA, nodeB, "X")
    relate(nodeB, nodeC, "X")
    relate(nodeC, nodeD, "X")

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    relate(nodeA, nodeX, "A")
    relate(nodeX, nodeD, "B")

    val result = executeWithAllPlanners(
      """MATCH p = shortestPath((src:A)-[rs*]->(dst:D))
        | WHERE ALL(r in rs WHERE type(rs[0]) = type(r) )
        |RETURN nodes(p) AS nodes""".stripMargin)
      .columnAs[List[Node]]("nodes").toList

    result should equal(List(List(nodeA, nodeB, nodeC, nodeD)))
  }

  test("finds shortest path") {
    /*
       a-b-c-d
       b-d
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)
    relate(nodeB, nodeD)

    val result = executeWithAllPlanners("MATCH p = shortestPath((src:A)-[*]->(dst:D)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

    result should equal(List(List(nodeA, nodeB, nodeD)))
  }

  test("optionally finds shortest path") {
    /*
       a-b-c-d
       b-d
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)
    relate(nodeB, nodeD)

    val result = executeWithAllPlanners("OPTIONAL MATCH p = shortestPath((src:A)-[*]->(dst:D)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

    result should equal(List(List(nodeA, nodeB, nodeD)))
  }

  test("apply-arguments with optional shortest path should be plannable in IDP") {
    /*
       a-b-c-d
       b-d
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)
    relate(nodeB, nodeD)

    val result = executeWithAllPlanners("MATCH (a:A), (d:D) OPTIONAL MATCH p = shortestPath((a)-[*]->(d)) RETURN nodes(p) AS nodes").toList

    result should equal(List(Map("nodes" -> List(nodeA, nodeB, nodeD))))
  }

  test("returns null when no shortest path is found") {
    val result = executeWithAllPlanners("MATCH (a:A), (b:B) OPTIONAL MATCH p = shortestPath( (a)-[*]->(b) ) RETURN p").toList

    result should equal(List(Map("p" -> null)))
  }

  test("finds shortest path rels") {
    /*
       a-b-c-d
       b-d
     */
    val r1 = relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)
    val r4 = relate(nodeB, nodeD)

    val result = executeWithAllPlanners("MATCH shortestPath((src:A)-[r*]->(dst:D)) RETURN r AS rels").columnAs[List[Node]]("rels").toList

    result should equal(List(List(r1, r4)))
  }

  test("finds no shortest path due to length limit") {
    // a-b-c-d
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    val result = executeWithAllPlanners("MATCH p = shortestPath((src:A)-[*..1]->(dst:D)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

    result should be(empty)
  }

  // todo: broken for rule planner
  test("finds no shortest path due to start node being null") {
    // a-b-c-d
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    val result = executeWithAllPlanners("OPTIONAL MATCH (src:Y) WITH src MATCH p = shortestPath(src-[*..1]->dst) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

    result should equal(List())
  }

  test("rejects shortest path with minimal length different from 0 or 1") {
    // a-b-c-d
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    evaluating {
      executeWithAllPlanners("MATCH p = shortestPath((src:A)-[*2..3]->(dst:D)) RETURN nodes(p) AS nodes").toList
    } should produce[SyntaxException]
  }

  test("finds all shortest paths") {
    /*
       a-b-c
       a-d-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeA, nodeD)
    relate(nodeD, nodeC)

    val result = executeWithAllPlanners("MATCH p = allShortestPaths((src:A)-[*]->(dst:C)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB, nodeC), List(nodeA, nodeD, nodeC)))
  }

  test("finds a single path for paths of length one") {
    /*
       a-b-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("if asked for also return paths of length 0") {
    /*
       a-b-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*0..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB)))
  }

  test("we can ask explicitly for paths of minimal length 1") {
    /*
       a-b-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*1..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("finds a single path for non-variable length paths") {
    /*
       a-b-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("handle combination of shortestPath and pattern expressions") {
    val a = createLabeledNode("A")
    val b1 = createLabeledNode("B")
    val b2 = createLabeledNode("B")
    val c = createLabeledNode("C")
    relate(a, b1, "R")
    relate(b1, b2, "R")
    relate(b2, c, "R")

    val query = """MATCH path = allShortestPaths((a:A)-[:R*0..100]-(c:C))
                  |WITH nodes(path) AS pathNodes
                  |WITH pathNodes[0] AS p, pathNodes[3] as c
                  |RETURN length((c)-[:R]-(:B)-[:R]-(:B)-[:R]-(p)) AS res""".stripMargin

    executeWithAllPlanners(query).toList should equal(List(Map("res" -> 1)))
  }

  test("shortest path should work with predicates that can be applied to relationship expanders") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(r in rels(p) WHERE NOT exists(r.blocked))
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with multiple expressions and predicates") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p1 = shortestPath((a)-[rs1:REL*]->(b))
                  |MATCH p2 = shortestPath((a)-[rs2:REL*]->(b))
                  |WHERE ALL(r in rels(p1) WHERE NOT exists(r.blocked))
                  |RETURN nodes(p1) AS nodes1, nodes(p2) as nodes2
                """.stripMargin

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("nodes1" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")),
                                        "nodes2" -> List(nodes("source"), nodes("target")))))

    println(result.executionPlanDescription())
  }

  test("shortest path should work with predicates that depend on the path expression") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p = shortestPath((a)-[r:REL*]->(b))
                  |WHERE ALL(r in rels(p) WHERE type(r) = type(rels(p)[0]) AND NOT exists(r.blocked))
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)
    println(result.executionPlanDescription())

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that can be applied to relationship expanders and include dependencies on execution context") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(r in rels(p) WHERE NOT exists(r.blocked) AND a:X) AND NOT has(b.property)
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)
    println(result.executionPlanDescription())

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that reference shortestPath relationship identifier") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH p = shortestPath((a:X)-[rs:REL*]->(b:Y))
                  |WHERE ALL(r in rs WHERE NOT exists(r.blocked))
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that reference the path and cannot be applied to expanders") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH p = shortestPath((a:X)-[:REL*]->(b:Y))
                  |WHERE length(p) > 2
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  def shortestPathModel(): Map[String, Node] = {
    val nodes = Map[String, Node](
      "source" -> createLabeledNode(Map("name" -> "x"), "X"),
      "target" -> createLabeledNode("Y"),
      "node3" -> createNode(),
      "node4" -> createNode(),
      "node5" -> createNode())
    relate(createLabeledNode("X"), createLabeledNode("Y"), "NOTAREL")

    relate(nodes("source"), nodes("target"), "REL", Map("blocked" -> true))
    relate(nodes("source"), nodes("node3"))
    relate(nodes("node3"), nodes("target"), "REL", Map("blocked" -> true))
    relate(nodes("node3"), nodes("node4"))
    relate(nodes("node4"), nodes("target"))
    relate(nodes("node4"), nodes("node5"))
    relate(nodes("node5"), nodes("target"))

    nodes
  }
}
