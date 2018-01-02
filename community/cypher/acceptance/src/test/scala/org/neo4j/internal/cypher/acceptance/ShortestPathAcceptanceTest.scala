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

  test("shortest path should work with predicates that can be applied to node expanders") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE NOT exists(n.blocked))
                  |RETURN nodes(p) as nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")))))
  }

  test("shortest path should work with predicates that can be applied to both relationship and node expanders") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE exists(n.name) OR exists(n.age))
                  |AND ALL(r in rels(p) WHERE r.likesLevel > 10)
                  |RETURN nodes(p) as nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(
      Map("nodes" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy"))),
      Map("nodes" -> List(nodes("Mickey"), nodes("Minnie"), nodes("Daisy"))),
      Map("nodes" -> List(nodes("Minnie"), nodes("Daisy")))
    ))
  }

  test("shortest path should work with multiple expressions and predicates - relationship expander") {
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
  }

  test("shortest path should work with multiple expressions and predicates - node expander") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p1 = shortestPath((a)-[rs1:REL*]->(b))
                  |MATCH p2 = shortestPath((a)-[rs2:REL*]->(b))
                  |WHERE ALL(n in nodes(p2) WHERE exists(n.name) or n.age > 50)
                  |RETURN nodes(p1) AS nodes1, nodes(p2) as nodes2
                """.stripMargin

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("nodes1" -> List(nodes("Donald"), nodes("Goofy"), nodes("Daisy")),
      "nodes2" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")))))
  }

  test("shortest path should work with multiple expressions and predicates - relationship and node expander") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p1 = shortestPath((a)-[rs1:REL*]->(b))
                  |MATCH p2 = shortestPath((a)-[rs2:REL*]->(b))
                  |WHERE ALL(n in nodes(p2) WHERE exists(n.name) or n.age > 50)
                  |AND NONE(r in rels(p1) WHERE exists(r.blocked) OR NOT exists(r.likesLevel))
                  |RETURN nodes(p1) AS nodes1, nodes(p2) as nodes2
                """.stripMargin

    val result = executeWithCostPlannerOnly(query)

    result.toList should equal(List(Map("nodes1" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")),
      "nodes2" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")))))
  }

  test("shortest path should work with predicates that depend on the path expression (relationships)") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p = shortestPath((a)-[r:REL*]->(b))
                  |WHERE ALL(r in rels(p) WHERE type(r) = type(rels(p)[0]) AND NOT exists(r.blocked))
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that depend on the path expression (nodes)") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:A)
                  |MATCH p = shortestPath((a)-[r:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE labels(n) = labels(nodes(p)[0]) AND exists(n.age))
                  |RETURN nodes(p) as nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("Donald"), nodes("Mickey"))),
      Map("nodes" -> List(nodes("Donald"), nodes("Mickey"), nodes("Minnie"))),
      Map("nodes" -> List(nodes("Mickey"), nodes("Minnie")))))
  }

  test("shortest path should work with predicates that depend on the path expression (relationships and nodes)") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:A)
                  |MATCH p = shortestPath((a)-[r:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE labels(n) = labels(nodes(p)[0]) AND exists(n.age))
                  |AND ALL(r in rels(p) WHERE type(r) = type(rels(p)[0]) AND exists(r.likesLevel))
                  |RETURN nodes(p) as nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("Donald"), nodes("Mickey"))),
      Map("nodes" -> List(nodes("Donald"), nodes("Mickey"), nodes("Minnie"))),
      Map("nodes" -> List(nodes("Mickey"), nodes("Minnie")))))
  }

  test("shortest path should work with predicates that can be applied to relationship expanders and include dependencies on execution context") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(r in rels(p) WHERE NOT exists(r.blocked) AND a:X) AND NOT exists(b.property)
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that can be applied to node expanders and include dependencies on execution context") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE exists(n.name) AND a.name = 'Donald Duck') AND b.name = 'Daisy Duck'
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")))))
  }

  test("shortest path should work with predicates that can be applied to relationship and node expanders and include dependencies on execution context") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE (exists(n.name) OR exists(n.age)) AND a.name = 'Donald Duck')
                  |AND ALL(r in rels(p) WHERE r.likesLevel > 10)
                  |AND b.name = 'Daisy Duck'
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("nodes" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")))))
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


  test("should be able to do find shortest paths longer than 15 hops") {
    //given
    //({prop: "bar"})-[:R]->({prop: "bar"})…-[:R]->({prop: "foo"})
    val start = createNode(Map("prop" -> "start"))
    val end = createNode(Map("prop" -> "end"))
    val nodes = start +: (for (i <- 1 to 15) yield createNode(Map("prop" -> "bar"))) :+ end
    nodes.sliding(2).foreach {
      case Seq(node1, node2) => relate(node1, node2, "R")
    }

    val result = executeWithAllPlanners("MATCH p = shortestPath((n {prop: 'start'})-[:R*]->(m {prop: 'end'})) RETURN length(p) AS l")

    result.toList should equal(List(Map("l" -> 16)))
  }

  test("shortest path and unwind should work together") {
    val a1 = createLabeledNode("A")
    val a2 = createLabeledNode("A")
    val a3 = createLabeledNode("A")
    val a4 = createLabeledNode("A")

    relate(a1, a2, "T")
    relate(a2, a3, "T")
    relate(a3, a4, "T")

    val result = executeWithAllPlanners(
      """
        |MATCH p = (:A)-[:T*]-(:A)
        |WITH p WHERE length(p) > 1
        |UNWIND nodes(p)[1..-1] as n
        |RETURN id(n) as n, count(*) as c""".stripMargin)

    result.toList should equal(List(
      Map("n" -> 5, "c" -> 4), Map("n" -> 6, "c" -> 4)
    ))

    result.close()
  }

  test("should work with path expression with 2 repeating bound relationships") {
    createLdbc14Model()

    // This is a simplified version of ldbc q14
    val query =
      """MATCH path = allShortestPaths((person1:Person {id:0})-[:KNOWS*0..]-(person2:Person {id:5}))
        |RETURN
        | reduce(weight=0.0, r IN rels(path) |
        |            weight +
        |            length(()-[r]->()<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->()-[r]->())*1.0
        | ) AS weight
        |ORDER BY weight DESC""".stripMargin

    val result = executeWithAllPlanners(query)

    // Four shortest path with the same weight
    result.toList should equal(List(Map("weight" -> 2.0), Map("weight" -> 2.0), Map("weight" -> 2.0), Map("weight" -> 2.0)))
  }

  test("should work with path expression with multiple repeating bound relationships") {
    createLdbc14Model()

    // This is a simplified version of ldbc q14 with intentional redundant pattern part duplication
    val query =
      """MATCH path = allShortestPaths((person1:Person {id:0})-[:KNOWS*0..]-(person2:Person {id:5}))
        |RETURN
        | reduce(weight=0.0, r IN rels(path) |
        |            weight +
        |            length((:Comment)-[:COMMENT_HAS_CREATOR]->()<-[r]-()-[r]->()<-[:COMMENT_HAS_CREATOR]-(:Comment)-[:REPLY_OF_POST]->(:Post)-[:POST_HAS_CREATOR]->()-[r]->()<-[r]-())*1.0
        | ) AS weight
        |ORDER BY weight DESC""".stripMargin

    val result = executeWithAllPlanners(query)

    // Four shortest path with the same weight
    result.toList should equal(List(Map("weight" -> 2.0), Map("weight" -> 2.0), Map("weight" -> 2.0), Map("weight" -> 2.0)))
  }

  private def createLdbc14Model(): Unit = {
    def createPersonNode( id: Int ) = createLabeledNode(Map("id" -> id), "Person")

    def createCommentNode( id: Int ) = createLabeledNode(Map("id" -> id, "creationDate" -> 1), "Comment")

    def createPostNode( id: Int) =  createLabeledNode(Map("id" -> id, "creationDate" -> 1), "Post")

    val p0: Node = createPersonNode(0)
    val p1: Node = createPersonNode(1)
    val p2: Node = createPersonNode(2)
    val p3: Node = createPersonNode(3)
    val p4: Node = createPersonNode(4)
    val p5: Node = createPersonNode(5)
    val p6: Node = createPersonNode(6)
    val p7: Node = createPersonNode(7)
    val p8: Node = createPersonNode(8)
    val p9: Node = createPersonNode(9)

    val p0Post1 = createPostNode(0)
    val p1Post1 = createPostNode(1)
    val p3Post1 = createPostNode(2)
    val p5Post1 = createPostNode(3)
    val p6Post1 = createPostNode(4)
    val p7Post1 = createPostNode(5)
    val p0Comment1 = createCommentNode(6)
    val p1Comment1 = createCommentNode(7)
    val p1Comment2 = createCommentNode(8)
    val p4Comment1 = createCommentNode(9)
    val p4Comment2 = createCommentNode(10)
    val p5Comment1 = createCommentNode(11)
    val p5Comment2 = createCommentNode(12)
    val p7Comment1 = createCommentNode(13)
    val p8Comment1 = createCommentNode(14)
    val p8Comment2 = createCommentNode(15)

    relate(p0, p1, "KNOWS")
    relate(p1, p3, "KNOWS")
    relate(p3, p2, "KNOWS")
    relate(p4, p7, "KNOWS")
    relate(p4, p8, "KNOWS")
    relate(p4, p6, "KNOWS")

    relate(p4, p2, "KNOWS")
    relate(p5, p6, "KNOWS")
    relate(p5, p8, "KNOWS")
    relate(p2, p1, "KNOWS")
    relate(p7, p1, "KNOWS")

    relate(p0Post1, p0, "POST_HAS_CREATOR")
    relate(p3Post1, p3, "POST_HAS_CREATOR")
    relate(p1Post1, p1, "POST_HAS_CREATOR")
    relate(p5Post1, p5, "POST_HAS_CREATOR")
    relate(p6Post1, p6, "POST_HAS_CREATOR")
    relate(p7Post1, p7, "POST_HAS_CREATOR")

    relate(p0Comment1, p0, "COMMENT_HAS_CREATOR")
    relate(p1Comment1, p1, "COMMENT_HAS_CREATOR")
    relate(p1Comment2, p1, "COMMENT_HAS_CREATOR")
    relate(p4Comment1, p4, "COMMENT_HAS_CREATOR")
    relate(p4Comment2, p4, "COMMENT_HAS_CREATOR")
    relate(p5Comment1, p5, "COMMENT_HAS_CREATOR")
    relate(p5Comment2, p5, "COMMENT_HAS_CREATOR")
    relate(p7Comment1, p7, "COMMENT_HAS_CREATOR")
    relate(p8Comment1, p8, "COMMENT_HAS_CREATOR")
    relate(p8Comment2, p8, "COMMENT_HAS_CREATOR")

    relate(p0Comment1, p1Post1, "REPLY_OF_POST")
    relate(p1Comment1, p0Post1, "REPLY_OF_POST")
    relate(p1Comment2, p0Post1, "REPLY_OF_POST")
    relate(p4Comment1, p3Post1, "REPLY_OF_POST")
    relate(p4Comment2, p7Post1, "REPLY_OF_POST")
    relate(p5Comment1, p5Post1, "REPLY_OF_POST")
    relate(p8Comment1, p6Post1, "REPLY_OF_POST")

    relate(p7Comment1, p4Comment2, "REPLY_OF_COMMENT")
    relate(p8Comment2, p4Comment1, "REPLY_OF_COMMENT")
    relate(p5Comment2, p8Comment2, "REPLY_OF_COMMENT")
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

  def largerShortestPathModel(): Map[String, Node] = {
    val nodes = Map[String, Node](
      "Donald" -> createLabeledNode(Map("id" -> "Donald", "name" -> "Donald Duck", "age" -> 15), "A"),
      "Daisy" -> createLabeledNode(Map("id" -> "Daisy", "name" -> "Daisy Duck"), "D"),
      "Huey" -> createLabeledNode(Map("id" -> "Huey", "name" -> "Huey Duck"), "B"),
      "Dewey" -> createLabeledNode(Map("id" -> "Dewey", "name" -> "Dewey Duck"), "B"),
      "Louie" -> createLabeledNode(Map("id" -> "Louie", "name" -> "Louie Duck"), "B"),
      "Goofy" -> createLabeledNode(Map("id" -> "Goofy", "blocked" -> true), "C"),
      "Mickey" -> createLabeledNode(Map("id" -> "Mickey", "age" -> 10), "A"),
      "Minnie" -> createLabeledNode(Map("id" -> "Minnie", "age" -> 20, "blocked" -> true), "A"),
      "Pluto" -> createLabeledNode(Map("id" -> "Pluto", "age" -> 2), "E"))

    relate(nodes("Donald"), nodes("Goofy"), "REL", Map("blocked" -> true))
    relate(nodes("Donald"), nodes("Huey"), "REL", Map("likesLevel" -> 20))
    relate(nodes("Huey"), nodes("Dewey"), "REL", Map("likesLevel" -> 11))
    relate(nodes("Dewey"), nodes("Louie"), "REL", Map("likesLevel" -> 13))
    relate(nodes("Louie"), nodes("Daisy"), "REL", Map("likesLevel" -> 26))
    relate(nodes("Goofy"), nodes("Daisy"), "REL", Map("likesLevel" -> 45))
    relate(nodes("Donald"), nodes("Mickey"), "REL", Map("blocked" -> true, "likesLevel" -> 2))
    relate(nodes("Mickey"), nodes("Minnie"), "REL", Map("likesLevel" -> 25))
    relate(nodes("Minnie"), nodes("Daisy"), "REL", Map("likesLevel" -> 20))
    relate(nodes("Donald"), nodes("Pluto"))
    relate(nodes("Pluto"), nodes("Minnie"))

    nodes
  }
}
