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
package org.neo4j.cypher

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
}
