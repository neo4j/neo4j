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

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb._
import org.neo4j.test.TestGraphDatabaseFactory
import org.scalatest.BeforeAndAfterAll

class OptionalBehaviourAcceptanceTest extends AbstractAcceptanceTest {

  var nodeA: Node = null
  var nodeB: Node = null
  var nodeC: Node = null
  var selfRel: Relationship = null

  def initDatabase() {
    val single = createLabeledNode(Map.empty, "Single")
    nodeA = createLabeledNode(Map("prop" -> 42), "A")
    nodeB = createLabeledNode(Map("prop" -> 46), "B")
    nodeC = createLabeledNode(Map.empty, "C")
    relate(single, nodeA)
    relate(single, nodeB)
    relate(nodeA, nodeC)
    selfRel = relate(nodeB, nodeB)
  }

  test("optional nodes with labels in match clause should return null when there is no match") {
    val result = execute("match (n:Single) optional match n-[r]-(m:NonExistent) return r")
    assert(result.toList === List(Map("r" -> null)))
  }

  test("optional nodes with labels in match clause should not return if where is no match") {
    val result = execute("match (n:Single) optional match (n)-[r]-(m) where m:NonExistent return r")
    assert(result.toList === List(Map("r" -> null)))
  }

  test("predicates on optional matches should be respected") {
    val result = execute("match (n:Single) optional match n-[r]-(m) where m.prop = 42 return m")
    assert(result.toList === List(Map("m" -> nodeA)))
  }

  test("should allow match following optional match if there is an intervening WITH when there are no results") {
    val result = execute("MATCH (a:Single) OPTIONAL MATCH (a)-->(b:NonExistent) OPTIONAL MATCH (a)-->(c:NonExistent) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")
    assert(result.toList === List())
  }

  test("should allow match following optional match if there is an intervening WITH when there are no results 23") {
    val result = execute("MATCH (a:Single) OPTIONAL MATCH (a)-->(b:A) OPTIONAL MATCH (a)-->(c:B) WITH coalesce(b, c) as x MATCH (x)-->(d) RETURN d")
    assert(result.toList === List(Map("d" -> nodeC)))
  }

  test("should support optional match without any external dependencies in WITH") {
    val result = execute("OPTIONAL MATCH (a:A) WITH a AS a MATCH (b:B) RETURN a, b")

    assert(result.toList === List(Map("a" -> nodeA, "b" -> nodeB)))
  }

  test("should support named paths inside of optional matches") {
    val result = execute("match (a:A) optional match p = a-[:X]->b return p")

    assert(result.toList === List(Map("p" -> null)))
  }

  test("optional matching between two found nodes behaves as expected") {
    val result = execute(
      """match (a:A), (b:C)
        |optional match (x)-->(b)
        |return x""".stripMargin)

    assert(result.toSet === Set(
      Map("x" -> nodeA)
    ))
  }


  test("should support names paths inside of option matches with node predicates") {
    val result = execute("match (a:A), (b:B) optional match p = a-[:X]->b return p")

    assert(result.toList === List(Map("p" -> null)))
  }

  test("should support varlength optional relationships") {
    val result = execute("match (a:Single) optional match (a)-[*]->(b) return b")

    assert(result.toSet === Set(
      Map("b" -> nodeA),
      Map("b" -> nodeB),
      Map("b" -> nodeC)
    ))
  }

  test("should support varlength optional relationships that is longer than the existing longest") {
    val result = execute("match (a:Single) optional match a-[*3..]->b return b")

    assert(result.toSet === Set(Map("b" -> null)))
  }

  test("should support optional match to find self loops") {
    val result = execute("match (a:B) optional match a-[r]->a return r")

    assert(result.toSet === Set(Map("r" -> selfRel)))
  }

  test("should support optional match to not find self loops") {
    val result = execute("match (a) where not (a:B) optional match (a)-[r]->(a) return r")

    assert(result.toSet === Set(Map("r" -> null)))
  }

  test("should support varlength optional relationships where both ends are already bound") {
    val result = execute("match (a:Single), (x:C) optional match (a)-[*]->(x) return x")

    assert(result.toSet === Set(
      Map("x" -> nodeC)
    ))
  }

  test("should support varlength optional relationships where both ends are already bound but no paths exist") {
    val result = execute("match (a:A), (b:B) optional match p = (a)-[*]->(b) return p")

    assert(result.toSet === Set(
      Map("p" -> null)
    ))
  }

  test("should support optional relationships where both ends are already bound") {
    val result = execute("match (a:Single), (c:C) optional match (a)-->(b)-->(c) return b")

    assert(result.toSet === Set(
      Map("b" -> nodeA)
    ))
  }

  test("should support optional relationships where both ends are already bound and no paths exist") {
    val result = execute("match (a:A), (c:C) optional match (a)-->(b)-->(c) return b")

    assert(result.toSet === Set(
      Map("b" -> null)
    ))
  }

  test("should handle pattern predicates in optional match") {
    val result = execute("match (a:A), (c:C) optional match (a)-->(b) WHERE (b)-->(c) return b")

    assert(result.toSet === Set(
      Map("b" -> null)
    ))
  }

  test("should handle pattern predicates in optional match with hit") {
    val result = execute("match (a:Single), (c:C) optional match (a)-->(b) WHERE (b)-->(c) return b")

    assert(result.toSet === Set(
      Map("b" -> nodeA)
    ))
  }

  test("should handle correlated optional matches - the first does not match, and the second must not match") {
    val result = execute(
      """match (a:A), (b:B)
        |optional match (a)-->(x)
        |optional match (x)-[r]->(b)
        |return x, r""".stripMargin)

    assert(result.toSet === Set(
      Map("x" -> nodeC, "r" -> null)
    ))
  }
}

trait AbstractAcceptanceTest extends CypherFunSuite with BeforeAndAfterAll {

  var graph: GraphDatabaseService = null
  var engine: ExecutionEngine = null

  override protected def beforeAll() {
    super.beforeAll()
    graph = new TestGraphDatabaseFactory().newImpermanentDatabase()
    engine = new ExecutionEngine(graph)
    initDatabase()
  }

  def initDatabase(): Unit

  def relate(a: Node, b: Node, typeName: String = "REL"): Relationship = inTx {
    a.createRelationshipTo(b, DynamicRelationshipType.withName(typeName))
  }

  def createLabeledNode(map: Map[String, Any], labels: String*): Node = inTx {
    val result = graph.createNode()
    labels.foreach(l => result.addLabel(DynamicLabel.label(l)))
    setProperties(result, map)
    result
  }

  def createNode(map: Map[String, Any] = Map.empty): Node = inTx {
    val result = graph.createNode()
    setProperties(result, map)
    result
  }

  def setProperties(n: Node, map: Map[String, Any]): Unit = {
    map.foreach {
      case (k, v) => n.setProperty(k, v)
    }
  }

  def execute(query: String): ExecutionResult =
    engine.execute(query)

  private def inTx[T](f: => T): T = {
    val tx = graph.beginTx()
    val result = f
    tx.success()
    tx.close()
    result
  }
}
