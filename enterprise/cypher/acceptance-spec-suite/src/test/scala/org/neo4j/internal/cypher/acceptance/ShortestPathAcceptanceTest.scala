/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.internal.cypher.acceptance


import org.neo4j.cypher.internal.runtime.PathImpl
import org.neo4j.cypher.ExecutionEngineFunSuite

import org.neo4j.graphdb.factory.GraphDatabaseSettings
import org.neo4j.graphdb.{Node, Path}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class ShortestPathAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  val expectedToSucceed = Configs.Interpreted

  var nodeA: Node = _
  var nodeB: Node = _
  var nodeC: Node = _
  var nodeD: Node = _

  override def databaseConfig() = Map(
    GraphDatabaseSettings.forbid_shortestpath_common_nodes -> "false",
    GraphDatabaseSettings.cypher_idp_solver_duration_threshold -> "10000")
  // Added an increased duration to make up for the test running in parallel, should preferably be solved in a different way

  override protected def initTest(): Unit = {
    super.initTest()
    nodeA = createLabeledNode("A")
    nodeB = createLabeledNode("B")
    nodeC = createLabeledNode("C")
    nodeD = createLabeledNode("D")
  }

  test("shortest path in a with clause") {

    relate(nodeA, nodeB)

    val query =
      """
        | MATCH (a:A), (b:B)
        | WITH shortestPath((a)-[:REL]->(b)) AS x
        | RETURN nodes(x)
      """.stripMargin

    val result = executeWith(expectedToSucceed - Configs.Version2_3, query).columnAs[List[Node]]("nodes(x)").toList

    result should equal(List(List(nodeA, nodeB)))
  }

  test("shortest path in a with clause and no paths found") {

    relate(nodeA, nodeB)

    val query =
      """
        | MATCH (a:A), (b:B)
        | WITH shortestPath((a)-[:XXX]->(b)) AS x
        | RETURN nodes(x)
      """.stripMargin

    val result = executeWith(expectedToSucceed - Configs.Version2_3, query).columnAs[List[Node]]("nodes(x)").toList

    result should equal(List(null))
  }

  test("all shortest paths in a with clause") {

    relate(nodeA, nodeB)

    val query =
      """
        | MATCH (a:A), (b:B)
        | WITH allShortestPaths((a)-[:REL]->(b)) AS p
        | UNWIND p AS x
        | RETURN nodes(x)
      """.stripMargin

    val result = executeWith(expectedToSucceed - Configs.Version2_3, query).columnAs[List[Node]]("nodes(x)").toList

    result should equal(List(List(nodeA, nodeB)))
  }

  test("all shortest paths in a with clause and no paths found") {

    relate(nodeA, nodeB)

    val query =
      """
        | MATCH (a:A), (b:B)
        | WITH allShortestPaths((a)-[:XXX]->(b)) AS p
        | UNWIND p AS x
        | RETURN nodes(x)
      """.stripMargin

    val result = executeWith(expectedToSucceed - Configs.Version2_3, query).columnAs[List[Node]]("nodes(x)").toList

    result should equal(List.empty)
  }

  // THESE NEED TO BE REVIEWED FOR SEMANTIC CORRECTNESS
  test("unnamed shortest path with fallback-required predicate should work") {
    val r1 = relate(nodeA, nodeB, "bar" -> 1)
    relate(nodeB, nodeC, "foo" -> 1)
    relate(nodeC, nodeD, "foo" -> 1)
    val r4 = relate(nodeB, nodeD, "foo" -> 1)

    val queryWithComplexPredicate = "MATCH shortestPath((src:A)-[r*]->(dst:D)) WHERE ALL (x IN tail(r) WHERE x.foo = (head(r)).bar) RETURN r AS rels"

    val result = executeWith(expectedToSucceed, queryWithComplexPredicate).columnAs[List[Node]]("rels").toList

    result should equal(List(List(r1, r4)))
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

    val result = executeWith(expectedToSucceed,
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

    val result = executeWith(expectedToSucceed,
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

    val result = executeWith(expectedToSucceed,
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

    val result = executeWith(expectedToSucceed,
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

    val result = executeWith(expectedToSucceed, query)
  }

  test("should still be able to return shortest path expression") {
    /* a-b-c-d */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    /* a-x-d */
    val nodeX = createLabeledNode("X")
    val r1 = relate(nodeA, nodeX)
    val r2 = relate(nodeX, nodeD)

    val result = executeWith(expectedToSucceed, "MATCH (src:A), (dst:D) RETURN shortestPath((src:A)-[*]->(dst:D)) as path")

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

    val result = executeWith(expectedToSucceed,
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

    val result = executeWith(expectedToSucceed, "MATCH p = shortestPath((src:A)-[*]->(dst:D)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

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

    val result = executeWith(expectedToSucceed, "OPTIONAL MATCH p = shortestPath((src:A)-[*]->(dst:D)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

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

    val result = executeWith(expectedToSucceed, "MATCH (a:A), (d:D) OPTIONAL MATCH p = shortestPath((a)-[*]->(d)) RETURN nodes(p) AS nodes").toList

    result should equal(List(Map("nodes" -> List(nodeA, nodeB, nodeD))))
  }

  test("returns null when no shortest path is found") {
    val result = executeWith(expectedToSucceed, "MATCH (a:A), (b:B) OPTIONAL MATCH p = shortestPath( (a)-[*]->(b) ) RETURN p").toList

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

    val result = executeWith(expectedToSucceed, "MATCH shortestPath((src:A)-[r*]->(dst:D)) RETURN r AS rels").columnAs[List[Node]]("rels").toList

    result should equal(List(List(r1, r4)))
  }

  test("finds no shortest path due to length limit") {
    // a-b-c-d
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    val result = executeWith(expectedToSucceed, "MATCH p = shortestPath((src:A)-[*..1]->(dst:D)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

    result should be(empty)
  }

  test("finds no shortest path due to start node being null") {
    // a-b-c-d
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    val result = executeWith(expectedToSucceed, "OPTIONAL MATCH (src:Y) WITH src MATCH p = shortestPath((src)-[*..1]->(dst)) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

    result should equal(List())
  }

  test("rejects shortest path with minimal length different from 0 or 1") {
    // a-b-c-d
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)
    relate(nodeC, nodeD)

    val expectedToFail = Configs.All + Configs.Morsel + TestConfiguration(Versions.Default, Planners.Default,
      Runtimes(Runtimes.Default, Runtimes.ProcedureOrSchema, Runtimes.CompiledSource, Runtimes.CompiledBytecode))

    failWithError(expectedToFail, "MATCH p = shortestPath((src:A)-[*2..3]->(dst:D)) RETURN nodes(p) AS nodes", List("shortestPath(...) does not support a minimal length different from 0 or 1"))
  }

  test("if asked for also return paths of length 0") {
    /*
       a-b-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWith(expectedToSucceed, "match p = shortestpath((a:A)-[r*0..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB)))
  }

  test("if asked for also return paths of length 0, even when no max length is specified") {
    /*
       a-b-c
     */
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWith(expectedToSucceed, "match p = shortestpath((a:A)-[r*0..]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB), List(nodeA, nodeB, nodeC)))
  }

  // THESE YET NEED TO BE PORTED TO THE TCK

  test("shortest path should work with predicates that can be applied to relationship expanders") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:X), (b:Y)
                  |MATCH p = shortestPath((a)-[rs:REL*]->(b))
                  |WHERE ALL(r in rels(p) WHERE NOT exists(r.blocked))
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query, expectedDifferentResults = Configs.AllRulePlanners)

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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query, expectedDifferentResults = Configs.AllRulePlanners)

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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that can be applied to node expanders and include dependencies on execution context") {
    val nodes = largerShortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH (a:A), (b:D)
                  |MATCH p = shortestPath((a)-[:REL*]->(b))
                  |WHERE ALL(n in nodes(p) WHERE exists(n.name) AND a.name = 'Donald Duck') AND b.name = 'Daisy Duck'
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("nodes" -> List(nodes("Donald"), nodes("Huey"), nodes("Dewey"), nodes("Louie"), nodes("Daisy")))))
  }

  test("shortest path should work with predicates that reference shortestPath relationship variable") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH p = shortestPath((a:X)-[rs:REL*]->(b:Y))
                  |WHERE ALL(r in rs WHERE NOT exists(r.blocked))
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWith(expectedToSucceed, query)

    result.toList should equal(List(Map("nodes" -> List(nodes("source"), nodes("node3"), nodes("node4"), nodes("target")))))
  }

  test("shortest path should work with predicates that reference the path and cannot be applied to expanders") {
    val nodes = shortestPathModel()

    val query = """PROFILE CYPHER
                  |MATCH p = shortestPath((a:X)-[:REL*]->(b:Y))
                  |WHERE length(p) > 2
                  |RETURN nodes(p) AS nodes
                """.stripMargin

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, "MATCH p = shortestPath((n {prop: 'start'})-[:R*]->(m {prop: 'end'})) RETURN length(p) AS l")

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

    val result = executeWith(expectedToSucceed + Configs.SlottedInterpreted,
      """
        |MATCH p = (:A)-[:T*]-(:A)
        |WITH p WHERE length(p) > 1
        |UNWIND nodes(p)[1..-1] as n
        |RETURN id(n) as n, count(*) as c""".stripMargin)

    result.toSet should equal(Set(
      Map("n" -> a2.getId, "c" -> 4), Map("n" -> a3.getId, "c" -> 4)
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

    val result = executeWith(expectedToSucceed, query)

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

    val result = executeWith(expectedToSucceed, query)

    // Four shortest path with the same weight
    result.toList should equal(List(Map("weight" -> 2.0), Map("weight" -> 2.0), Map("weight" -> 2.0), Map("weight" -> 2.0)))
  }

  test("should return shortest paths if using a ridiculously unhip cypher") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    val query = s"""MATCH (a), (c)
                  |WHERE id(a) = ${a.getId} AND id(c) = ${c.getId}
                  |RETURN shortestPath((a)-[*]->(c)) AS p
                  """.stripMargin

    val result = executeWith(expectedToSucceed, query).columnAs[Path]("p").toList.head

    result.endNode() should equal(c)
    result.startNode() should equal(a)
    result.length() should equal(2)
  }

  test("should return shortest path") {
    val a = createLabeledNode("Start")
    val b = createNode()
    val c = createLabeledNode("End")
    relate(a, b)
    relate(b, c)

    val result = executeWith(expectedToSucceed, "match (a:Start), (c:End) return shortestPath((a)-[*]->(c))").columnAs[Path]("shortestPath((a)-[*]->(c))").toList.head
    result.endNode() should equal(c)
    result.startNode() should equal(a)
    result.length() should equal(2)
  }

  test("should handle all predicate in optional match") {
    // Given the graph:
    // (p1)-[:KNOWS {prop:1337}]-> (p2)
    // (p1)-[:KNOWS {prop:42}]->(intermediate)-[:KNOWS {prop:42}]->(p2)
    val p1 = createLabeledNode(Map("id" -> 1), "Person")
    val p2 = createLabeledNode(Map("id" -> 2), "Person")
    val intermediate = createLabeledNode(Map("id" -> 3), "Person")
    relate(p1, p2, "KNOWS", Map("prop" -> 1337))
    relate(p1, intermediate, "KNOWS", Map("prop" -> 42))
    relate(intermediate, p2, "KNOWS", Map("prop" -> 42))

    // When
    val result = executeWith(expectedToSucceed - Configs.Cost2_3,
      """MATCH (person1:Person {id:1}), (person2:Person {id:2})
        |OPTIONAL MATCH path = shortestPath((person1)-[k:KNOWS*0..]-(person2))
        |WHERE all(r in k WHERE r.prop IN [42])
        |RETURN length(path)""".stripMargin)
    // Then
    result.toList should equal(List(Map("length(path)" -> 2)))
  }

  test("should not require named nodes in shortest path") {
    graph.execute("CREATE (a:Person)-[:WATCH]->(b:Movie)")

    val query =
      """MATCH p=shortestPath( (:Person)-[*1..4]->(:Movie) )
        |RETURN length(p)
      """.stripMargin
    graph.execute(query).columnAs[Int]("length(p)").next() should be (1)
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
