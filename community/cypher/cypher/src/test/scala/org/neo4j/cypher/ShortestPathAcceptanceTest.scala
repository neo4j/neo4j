/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.graphdb.Node

import scala.collection.JavaConverters._

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

    val result = innerExecute("CYPHER PLANNER IDP MATCH (a:A), (d:D) OPTIONAL MATCH p = shortestPath((a)-[*]->(d)) RETURN nodes(p) AS nodes").toList

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

    val result = executeWithCostPlannerOnly("OPTIONAL MATCH (src:Y) WITH src MATCH p = shortestPath(src-[*..1]->dst) RETURN nodes(p) AS nodes").columnAs[List[Node]]("nodes").toList

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

  test("shortest path and unwind should work together")
  {
    val a1 = createLabeledNode("A")
    val a2 = createLabeledNode("A")
    val a3 = createLabeledNode("A")
    val a4 = createLabeledNode("A")

    relate(a1, a2, "T")
    relate(a2, a3, "T")
    relate(a3, a4, "T")

    val result = graph.execute(
      """
        |CYPHER PLANNER=COST
        |MATCH p = (:A)-[:T*]-(:A)
        |WITH p WHERE length(p) > 1
        |UNWIND nodes(p)[1..-1] as n
        |RETURN id(n) as n, count(*) as c""".stripMargin)

    result.asScala.toList.map(_.asScala) should equal(List(
      Map("n" -> 5, "c" -> 4),Map("n" -> 6, "c" -> 4)
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
}
