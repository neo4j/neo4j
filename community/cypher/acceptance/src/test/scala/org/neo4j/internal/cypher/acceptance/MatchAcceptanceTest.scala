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

import org.neo4j.cypher._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.PathImpl
import org.neo4j.graphdb._
import org.neo4j.helpers.collection.IteratorUtil.single

import scala.collection.JavaConverters._

class MatchAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("make sure non-existing nodes are not returned") {
    executeWithAllPlanners("match n where id(n) = 10 return n") should be(empty)
    executeWithAllPlanners("match ()-[r]->() where id(r) = 10 return r") should be(empty)
  }

  test("should fail if columnAs refers to unknown column") {
    val n1 = createNode()
    val n2 = createNode()
    val r = relate(n1, n2)
    val result = executeWithAllPlanners("MATCH (n)-[r]->() RETURN n, r")
    a[NotFoundException] should be thrownBy result.columnAs("m")
  }

  test("AND'd predicates that throw exceptions should not matter if other predicates return false") {
    val root = createLabeledNode(Map("name" -> "x"), "Root")
    val child1 = createLabeledNode(Map("id" -> "text"), "TextNode")
    val child2 = createLabeledNode(Map("id" -> 0), "IntNode")
    relate(root, child1)
    relate(root, child2)

    val query = "MATCH (:Root {name:'x'})-->(i:TextNode) WHERE i.id =~ 'te.*' RETURN i"
    val result = executeWithAllPlanners(query)

    result.toList should equal(List(Map("i" -> child1)))
  }

  test("OR'd predicates that throw exceptions should not matter if other predicates return true") {
    val root = createLabeledNode(Map("name" -> "x"), "Root")
    val child1 = createLabeledNode(Map("id" -> "text"), "TextNode")
    val child2 = createLabeledNode(Map("id" -> 0), "IntNode")
    relate(root, child1)
    relate(root, child2)

    val query = "MATCH (:Root {name:'x'})-->(i) WHERE has(i.id) OR i.id =~ 'te.*' RETURN i"
    val result = executeWithAllPlanners(query)

    result.columnAs("i").toSet[Node] should equal(Set(child1, child2))
  }

  test("exceptions should be thrown if rows are kept through AND'd predicates") {
    val root = createLabeledNode(Map("name" -> "x"), "Root")
    val child = createLabeledNode(Map("id" -> 0), "Child")
    relate(root, child)

    val query = "MATCH (:Root {name:'x'})-->(i:Child) WHERE i.id > 'te' RETURN i"

    a [IncomparableValuesException] should be thrownBy executeWithAllPlanners(query)
  }

  test("exceptions should be thrown if rows are kept through OR'd predicates") {
    val root = createLabeledNode(Map("name" -> "x"), "Root")
    val child = createLabeledNode(Map("id" -> 0), "Child")
    relate(root, child)

    val query = "MATCH (:Root {name:'x'})-->(i) WHERE NOT has(i.id) OR i.id > 'te' RETURN i"

    a [IncomparableValuesException] should be thrownBy executeWithAllPlanners(query)
  }

  test("combines aggregation and named path") {
    val node1 = createNode("num" -> 1)
    val node2 = createNode("num" -> 2)
    val node3 = createNode("num" -> 3)
    val node4 = createNode("num" -> 4)
    relate(node1, node2)
    relate(node3, node4)

    val result = executeWithCostPlannerOnly(
      """MATCH p=()-[*]->()
        |WITH count(*) AS count, p AS p
        |WITH nodes(p) AS nodes
        |RETURN *""".stripMargin)

    result.toSet should equal(Set(
      Map("nodes" -> Seq(node1, node2)),
      Map("nodes" -> Seq(node3, node4))
    ))
  }

  test("a merge following a delete of multiple rows should not match on a deleted entity") {
    // GIVEN
    val a = createLabeledNode("A")
    val branches = 2
    val b = (0 until branches).map(n => createLabeledNode(Map("value" -> n), "B"))
    val c = (0 until branches).map(_ => createLabeledNode("C"))
    (0 until branches).foreach(n => {
      relate(a, b(n))
      relate(b(n), c(n))
    })

    val query =
      """
        |MATCH (a:A) -[ab]-> (b:B) -[bc]-> (c:C)
        |DELETE ab, bc, b, c
        |MERGE (newB:B { value: 1 })
        |MERGE (a) -[:REL]->  (newB)
        |MERGE (newC:C)
        |MERGE (newB) -[:REL]-> (newC)
      """.stripMargin

    // WHEN
    executeWithRulePlanner(query)

    // THEN
    assert(true)
  }

  test("identifiers of deleted nodes should not be able to cause errors in later merge actions that do not refer to them") {
    // GIVEN
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val c = createLabeledNode("C")
    relate(a, b)
    relate(b, c)

    val query =
      """
        |MATCH (a:A) -[ab]-> (b:B) -[bc]-> (c:C)
        |DELETE ab, bc, b, c
        |MERGE (newB:B)
        |MERGE (a) -[:REL]->  (newB)
        |MERGE (newC:C)
        |MERGE (newB) -[:REL]-> (newC)
      """.stripMargin

    // WHEN
    executeWithRulePlanner(query)

    // THEN query should not crash
    assert(true)
  }

  test("merges should not be able to match on deleted nodes") {
    // GIVEN
    val node1 = createLabeledNode(Map("value" -> 1), "A")
    val node2 = createLabeledNode(Map("value" -> 2), "A")

    val query = """
                  |MATCH (a:A)
                  |DELETE a
                  |MERGE (a2:A)
                  |RETURN a2
                """.stripMargin

    // WHEN
    val result = executeWithRulePlanner(query)

    // THEN
    result.toList should not contain Map("a2" -> node1)
    result.toList should not contain Map("a2" -> node2)
  }

  test("merges should not be able to match on deleted relationships") {
    // GIVEN
    val a = createNode()
    val b = createNode()
    val rel1 = relate(a, b, "T")
    val rel2 = relate(a, b, "T")

    val query = """
                  |MATCH (a)-[t:T]->(b)
                  |DELETE t
                  |MERGE (a)-[t2:T]->(b)
                  |RETURN t2
                """.stripMargin

    // WHEN
    val result = executeWithRulePlanner(query)

    // THEN
    result.toList should not contain Map("t2" -> rel1)
    result.toList should not contain Map("t2" -> rel2)
  }

  test("comparing numbers should work nicely") {
    val n1 = createNode(Map("x" -> 50))
    val n2 = createNode(Map("x" -> 50l))
    val n3 = createNode(Map("x" -> 50f))
    val n4 = createNode(Map("x" -> 50d))
    val n5 = createNode(Map("x" -> 50.toByte))

    val result = executeWithAllPlanners(
      s"match n where n.x < 100 return n"
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2, n3, n4, n5))
  }

  test("comparing string and chars should work nicely") {
    val n1 = createNode(Map("x" -> "Anders"))
    val n2 = createNode(Map("x" -> 'C'))
    createNode(Map("x" -> "Zzing"))
    createNode(Map("x" -> 'Ä'))

    val result = executeWithAllPlanners(
      s"match n where n.x < 'Z' AND n.x < 'z' return n"
    )

    result.columnAs("n").toList should equal(List(n1, n2))
  }

  test("test zero length var len path in the middle") {
    createNodes("A", "B", "C", "D", "E")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "FRIEND" -> "C")

    val result = executeWithAllPlanners("match (a {name:'A'})-[:CONTAINS*0..1]->b-[:FRIEND*0..1]->c return a,b,c")

    result.toSet should equal(
      Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C")))
      )
  }

  test("simple var length acceptance test") {
    createNodes("A", "B", "C", "D")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "CONTAINS" -> "C")
    relate("C" -> "CONTAINS" -> "D")


    val result = executeWithAllPlanners("match (a {name:'A'})-[*]->x return x")

    result.toSet should equal(
      Set(
        Map("x" -> node("B")),
        Map("x" -> node("C")),
        Map("x" -> node("D")))
      )
  }

  test("should return a var length path without minimal length") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = executeWithAllPlanners("match p=(n {name:'A'})-[:KNOWS*..2]->x return p")

    result.columnAs[Path]("p").toList should equal(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ))
  }

  test("should return a var length path with unbound max") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = executeWithAllPlanners("match p=(n {name:'A'})-[:KNOWS*..]->x return p")

    result.columnAs[Path]("p").toList should equal(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ))
  }

  test("should handle bound nodes not part of the pattern") {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")

    val result = executeWithAllPlanners("MATCH (a {name:'A'}),(c {name:'C'}) match a-->b return a,b,c").toSet

    result should equal (Set(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))))
  }

  test("should return shortest path") {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val result = executeWithAllPlanners("match p = shortestPath((a {name:'A'})-[*..15]-(b {name:'B'})) return p").
      toList.head("p").asInstanceOf[Path]

    graph.inTx {
      val number_of_relationships_in_path = result.length()
      number_of_relationships_in_path should equal (1)
      result.startNode() should equal (node("A"))
      result.endNode() should equal (node("B"))
      result.lastRelationship() should equal (r1)
    }
  }

  test("should return shortest path unbound length") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    //Checking that we don't get an exception
    executeWithAllPlanners("match p = shortestPath((a {name:'A'})-[*]-(b {name:'B'})) return p").toList
  }

  test("should not traverse same relationship twice in shortest path") {
    // given
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    // when
    val result = executeWithAllPlanners("MATCH (a{name:'A'}), (b{name:'B'}) MATCH p=allShortestPaths((a)-[:KNOWS|KNOWS*]->(b)) RETURN p").
      toList

    // then
    graph.inTx {
      result.size should equal (1)
    }
  }

  test("finds a single path for paths of length one") {
    /*
       a-b-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("if asked for also return paths of length 0") {
    /*
       a-b-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*0..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB)))
  }

  test("if asked for also return paths of length 0, even when no max length is speficied") {
    /*
       a-b-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*0..]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA), List(nodeA, nodeB), List(nodeA, nodeB, nodeC)))
  }

  test("we can ask explicitly for paths of minimal length 1") {
    /*
       a-b-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r*1..1]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("finds a single path for non-variable length paths") {
    /*
       a-b-c
     */
    val nodeA = createLabeledNode("A")
    val nodeB = createLabeledNode("B")
    val nodeC = createLabeledNode("C")
    relate(nodeA, nodeB)
    relate(nodeB, nodeC)

    val result = executeWithAllPlanners("match p = shortestpath((a:A)-[r]->(n)) return nodes(p) as nodes").columnAs[List[Node]]("nodes").toSet
    result should equal(Set(List(nodeA, nodeB)))
  }

  test("two bound nodes pointing to one") {
    val a = createNode("A")
    val b = createNode("B")
    val x1 = createNode("x1")
    val x2 = createNode("x2")

    relate(a, x1, "REL", "AX1")
    relate(a, x2, "REL", "AX2")

    relate(b, x1, "REL", "BX1")
    relate(b, x2, "REL", "BX2")

    val result = executeWithAllPlanners( """
MATCH (a {name:'A'}), (b {name:'B'})
MATCH a-[rA]->x<-[rB]->b
return x""")

    result.columnAs("x").toList should equal (List(x2, x1))
  }

  test("three bound nodes pointing to one") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val x1 = createNode("x1")
    val x2 = createNode("x2")

    relate(a, x1, "REL", "AX1")
    relate(a, x2, "REL", "AX2")

    relate(b, x1, "REL", "BX1")
    relate(b, x2, "REL", "BX2")

    relate(c, x1, "REL", "CX1")
    relate(c, x2, "REL", "CX2")

    val result = executeWithAllPlanners( """
MATCH (a {name:'A'}), (b {name:'B'}), (c {name:'C'})
match a-[rA]->x, b-[rB]->x, c-[rC]->x
return x""")

    result.columnAs("x").toList should equal (List(x2, x1))
  }

  test("three bound nodes pointing to one with a bunch of extra connections") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    val d = createNode("d")
    val e = createNode("e")
    val f = createNode("f")
    val g = createNode("g")
    val h = createNode("h")
    val i = createNode("i")
    val j = createNode("j")
    val k = createNode("k")

    relate(a, d)
    relate(a, e)
    relate(a, f)
    relate(a, g)
    relate(a, i)

    relate(b, d)
    relate(b, e)
    relate(b, f)
    relate(b, h)
    relate(b, k)

    relate(c, d)
    relate(c, e)
    relate(c, h)
    relate(c, g)
    relate(c, j)

    val result = executeWithAllPlanners( """
MATCH (a {name:'a'}), (b {name:'b'}), (c {name:'c'})
match a-->x, b-->x, c-->x
return x""")

    result.columnAs("x").toList should equal (List(e, d))
  }

  test("should split optional mandatory cleverly") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")

    relate(a, b, "knows", "rAB")
    relate(b, c, "knows", "rBC")

    val result = executeWithAllPlanners( """
match (a {name:'A'})
optional match (a)-[r1:knows]->(friend)-[r2:knows]->(foaf)
return foaf""")

    result.toList should equal (List(Map("foaf" -> c)))
  }

  test("should handle optional paths") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = executeWithAllPlanners( """
match (a {name:'A'}), (x) where x.name in ['B', 'C']
optional match p = a --> x
return x, p""")

    graph.inTx {
      assert(Set(
        Map("x" -> b, "p" -> PathImpl(a, r, b)),
        Map("x" -> c, "p" -> null)
      ) === result.toSet)
    }
  }

  test("should handle optional paths from graph algo") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = executeWithAllPlanners( """
match (a {name:'A'}), (x) where x.name in ['B', 'C']
optional match p = shortestPath(a -[*]-> x)
return x, p""").toSet

    graph.inTx(assert(Set(
      Map("x" -> b, "p" -> PathImpl(a, r, b)),
      Map("x" -> c, "p" -> null)
    ) === result))
  }

  test("should handle optional paths from a combo") {
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "X")

    val result = executeWithAllPlanners( """
match (a {name:'A'})
optional match p = a-->b-[*]->c
return p""")

    assert(Set(
      Map("p" -> null)
    ) === result.toSet)
  }

  test("should handle optional paths from a combo with MATCH") {
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "X")

    val result = executeWithAllPlanners( """
match (a {name:'A'})
optional match p = a-->b-[*]->c
return p""")

    assert(Set(
      Map("p" -> null)
    ) === result.toSet)
  }

  test("should handle optional paths from var length path") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = executeWithAllPlanners( """
match (a {name:'A'}), (x) where x.name in ['B', 'C']
optional match p = (a)-[r*]->(x)
return r, x, p""")

    assert(Set(
      Map("r" -> Seq(r), "x" -> b, "p" -> PathImpl(a, r, b)),
      Map("r" -> null, "x" -> c, "p" -> null)
    ) === result.toSet)
  }

  test("should return an iterable with all relationships from a var length") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, c)

    val result = executeWithAllPlanners( """
match (a) where id(a) = 0
match a-[r*2]->c
return r""")

    result.toList should equal (List(Map("r" -> List(r1, r2))))
  }

  test("should handle all shortest paths") {
    createDiamond()

    val result = executeWithAllPlanners( """
match (a), (d) where id(a) = 0 and id(d) = 3
match p = allShortestPaths( a-[*]->d )
return p""")

    result.toList.size should equal (2)
  }

  test("should collect leafs") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    val rab = relate(a, b)
    val rac = relate(a, c)
    val rcd = relate(c, d)

    val result = executeWithAllPlanners( """
match p = root-[*]->leaf
where id(root) = 0 and not(leaf-->())
return p, leaf""")

    assert(Set(
      Map("leaf" -> b, "p" -> PathImpl(a, rab, b)),
      Map("leaf" -> d, "p" -> PathImpl(a, rac, c, rcd, d))
    ) === result.toSet)
  }

  test("should exclude connected nodes") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("B")
    relate(a, b)

    val result = executeWithAllPlanners("""
MATCH (a {name:'A'}), (other {name:'B'})
WHERE NOT a-->other
RETURN other""")

    result.toList should equal (List(Map("other" -> c)))
  }

  test("should not throw exception when stuff is missing") {
    val a = createNode()
    val b = createNode("Mark")
    relate(a, b)
    val result = executeWithAllPlanners( """
MATCH n-->x0
OPTIONAL MATCH x0-->x1
WHERE x1.foo = 'bar'
RETURN x0.name""")
    result.toList should equal (List(Map("x0.name" -> "Mark")))
  }

  test("should solve an optional match even when the optional match is highly selective") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    relate(a, b)
    relate(a, c)
    val result = executeWithAllPlanners( s"""
MATCH a-->b
WHERE id(b) = ${b.getId}
OPTIONAL MATCH a-->c
WHERE id(c) = ${c.getId}
RETURN a.name""")
    result.toList should equal (List(Map("a.name" -> "A")))
  }

  test("should find nodes both directions") {
    val n = createNode()
    val a = createNode()
    relate(a, n, "Admin")
    val result = executeWithAllPlanners( "match (n) -[:Admin]- (b) where id(n) = 0 return id(n), id(b)")
    result.toSet should equal (Set(Map("id(n)" -> 0, "id(b)" -> 1)))
  }

  test("should get all nodes") {
    val a = createNode()
    val b = createNode()

    val result = executeWithAllPlanners("match n return n")
    result.columnAs[Node]("n").toSet should equal (Set(a, b))
  }

  test("should allow comparisons of nodes") {
    val a = createNode()
    val b = createNode()

    val result = executeWithAllPlanners("MATCH a, b where a <> b return a,b")
    result.toSet should equal (Set(Map("a" -> b, "b" -> a), Map("b" -> b, "a" -> a)))
  }

  test("should solve selfreferencing pattern") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = executeWithAllPlanners("match a-->b, b-->b return b")

    result shouldBe 'isEmpty
  }

  test("should solve self referencing pattern2") {
    val a = createNode()
    val b = createNode()

    val r = relate(a, a)
    relate(a, b)


    val result = executeWithAllPlanners("match a-[r]->a return r")
    result.toList should equal (List(Map("r" -> r)))
  }

  test("relationship predicate with multiple rel types") {
    val a = createNode()
    val b = createNode()
    val x = createNode()

    relate(a, x, "A")
    relate(b, x, "B")

    val result = executeWithAllPlanners("match a where a-[:A|:B]->() return a").toSet

    result should equal (Set(Map("a" -> a), Map("a" -> b)))
  }

  test("relationship predicate") {
    val a = createNode()
    val x = createNode()

    relate(a, x, "A")

    val result = executeWithAllPlanners("match a where a-[:A]->() return a").toList

    result should equal (List(Map("a" -> a)))
  }

  test("nullable var length path should work") {
    createNode()
    val b = createNode()

    val result = executeWithAllPlanners("""
match (a), (b) where id(a) = 0 and id(b) = 1
optional match a-[r*]-b where r is null and a <> b
return b
      """).toList

    result should equal (List(Map("b" -> b)))
  }

  test("listing rel types multiple times should not give multiple returns") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "REL")

    val result = executeWithAllPlanners("match (a)-[:REL|:REL]->(b) return b").toList

    result should equal (List(Map("b" -> b)))
  }

  test("different results on ordered aggregation with limit") {
    val root = createNode()
    val n1 = createNode("x" -> 1)
    val n2 = createNode("x" -> 2)
    val m1 = createNode()
    val m2 = createNode()

    relate(root, n1, m1)
    relate(root, n2, m2)

    val q = "match a-->n-->m where id(a) = 0 return n.x, count(*) order by n.x"

    val resultWithoutLimit = executeWithAllPlanners(q)
    val resultWithLimit = executeWithAllPlanners(q + " limit 1000")

    resultWithoutLimit.toList should equal (resultWithLimit.toList)
  }

  test("should be able to handle single node patterns") {
    // given
    val n = createNode("foo" -> "bar")

    // when
    val result = executeWithAllPlanners("match n where n.foo = 'bar' return n")

    // then
    result.toList should equal (List(Map("n" -> n)))
  }

  test("issue 479") {
    createNode()

    val q = "match (n) where id(n) = 0 optional match (n)-->(x) where x-->() return x"

    executeWithAllPlanners(q).toList should equal (List(Map("x" -> null)))
  }

  test("issue 479 has relationship to specific node") {
    createNode()

    val q = "match (n) where id(n) = 0 optional match (n)-[:FRIEND]->(x) where not n-[:BLOCK]->x return x"

    executeWithAllPlanners(q).toList should equal (List(Map("x" -> null)))
  }

  test("length on filter") {
    val q = "match (n) optional match (n)-[r]->(m) return length(filter(x in collect(r) WHERE x <> null)) as cn"

    executeWithAllPlanners(q).toList should equal (List(Map("cn" -> 0)))
  }

  test("path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = executeWithAllPlanners("match p=b<--a return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("shortest Path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = executeWithAllPlanners("match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath(b<-[*]-a) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("no match in optional match should produce null values") {
    val result = executeWithAllPlanners("OPTIONAL MATCH n RETURN n")

    result.toList should equal (List(Map("n" ->  null)))
  }

  test("should preserve the original matched values if optional match matches nothing") {
    val n = createNode()
    val result = executeWithAllPlanners("MATCH n OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n, x")

    result.toList should equal (List(Map("n" -> n, "x" -> null)))
  }

  test("empty collect should not contain null") {
    val n = createNode()
    val result = executeWithAllPlanners("MATCH n OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n, collect(x)")

    result.toList should equal (List(Map("n" -> n, "collect(x)" -> List())))
  }

  test("can rewrite has property") {
    val a = createNode()
    val r1 = createNode("foo" -> "bar")
    val r2 = createNode()
    val b = createNode()

    relate(a, r1)
    relate(a, r2)
    relate(r1, b)
    relate(r2, b)

    val result = executeWithAllPlanners("MATCH a-->r-->b WHERE id(a) = 0 AND has(r.foo) RETURN r")

    result.toList should equal (List(Map("r" -> r1)))
  }

  test("can handle paths with multiple unnamed nodes") {
    createNode()
    val result = executeWithAllPlanners("MATCH a<--()<--b-->()-->c WHERE id(a) = 0 RETURN c")

    result shouldBe 'isEmpty
  }

  test("path expressions should work with on the fly predicates") {
    val refNode = createNode()
    relate(refNode, createNode("name" -> "Neo"))
    val result = executeWithAllPlanners("MATCH a-->b WHERE b-->() AND id(a) = {self} RETURN b", "self" -> refNode.getId)

    result shouldBe 'isEmpty
  }

  test("should filter nodes by label given in match") {
    // given
    val a = createNode()
    val b1 = createLabeledNode("foo")
    val b2 = createNode()

    relate(a, b1)
    relate(a, b2)

    // when
    val result = executeWithAllPlanners(s"MATCH a-->(b:foo) RETURN b")

    // THEN
    result.toList should equal (List(Map("b" -> b1)))
  }

  test("should match nodes with specified labels on both sides") {
    // given
    val r = relate(createLabeledNode("A"), createLabeledNode("B"))
    relate(createLabeledNode("B"), createLabeledNode("A"))
    relate(createLabeledNode("B"), createLabeledNode("B"))
    relate(createLabeledNode("A"), createLabeledNode("A"))

    // when
    val result = executeWithAllPlanners(s"MATCH (a:A)-[r]->(b:B) RETURN r")

    // THEN
    result.toSet should equal (Set(Map("r" -> r)))
  }

  test("should match nodes with many labels specified on it") {
    // given
    val n = createLabeledNode("A","B","C")
    createLabeledNode("A","B")
    createLabeledNode("A","C")
    createLabeledNode("B","C")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")

    // when
    val result = executeWithAllPlanners(s"MATCH (a:A:B:C) RETURN a")

    // THEN
    result.toList should equal (List(Map("a" -> n)))
  }

  test("should be able to tell if a label is on a node or not") {
    // given
    createNode()
    createLabeledNode("Foo")

    // when
    val result = executeWithAllPlanners(s"MATCH (n) RETURN (n:Foo)")

    // THEN
    result.toSet should equal (Set(Map("(n:Foo)" -> true), Map("(n:Foo)" -> false)))
  }

  test("should use predicates in the correct place") {
    // given
    val m = executeWithRulePlanner( """create
                        (advertiser {name:"advertiser1"}),
                        (thing      {name:"Color"}),
                        (red        {name:"red"}),
                        (p1         {name:"product1"}),
                        (p2         {name:"product4"}),
                        (advertiser)-[:adv_has_product]->(p1),
                        (advertiser)-[:adv_has_product]->(p2),
                        (thing)-[:aa_has_value]->(red),
                        (p1)   -[:ap_has_value]->(red),
                        (p2)   -[:ap_has_value]->(red)
                        return advertiser, thing""").toList.head

    val advertiser = m("advertiser").asInstanceOf[Node]
    val thing = m("thing").asInstanceOf[Node]

    // when
    val result = executeWithAllPlanners(
      """MATCH (advertiser) -[:adv_has_product] ->(out) -[:ap_has_value] -> red <-[:aa_has_value]- (a)
       WHERE id(advertiser) = {1} AND id(a) = {2}
       AND red.name = 'red' and out.name = 'product1'
       RETURN out.name""", "1" -> advertiser.getId, "2" -> thing.getId)

    // then
    result.toList should equal (List(Map("out.name" -> "product1")))
  }

  test("should be able to use index hints") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    // when
    val result = executeWithAllPlanners("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name = 'Jacob' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription.toString should include("IndexSeek")
  }

  test("should be able to use index hints with STARTS WITH predicates") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    // when
    val result = executeWithAllPlanners("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name STARTS WITH 'Jac' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription.toString should include("IndexSeek")
  }

  test("should be able to use index hints with inequality/range predicates") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    // when
    val result = executeWithAllPlanners("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name > 'Jac' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
    result.executionPlanDescription.toString should include("IndexSeek")
  }

  test("should be able to use label as start point") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    // when
    val result = executeWithAllPlanners("MATCH (n:Person)-->() WHERE n.name = 'Jacob' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should not see updates created by itself") {
    createNode()

    val result = executeWithRulePlanner("match n create ()")
    assertStats(result, nodesCreated = 1)
  }

  test("id in where leads to empty result") {
    // when
    val result = executeWithAllPlanners("MATCH n WHERE id(n)=1337 RETURN n")

    // then DOESN'T THROW EXCEPTION
    result shouldBe 'isEmpty
  }

  test("should handle two unconnected patterns") {
    // given a node with two related nodes
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(a, c)

    // when asked for a cartesian product of the same match twice
    val result = executeWithAllPlanners("match a-->b match c-->d return a,b,c,d")

    // then we should find 2 x 2 = 4 result matches

    result.toSet shouldEqual
      Set(
        Map("a" -> a, "b" -> b, "c" -> a, "d" -> b),
        Map("a" -> a, "b" -> b, "c" -> a, "d" -> c),
        Map("a" -> a, "b" -> c, "c" -> a, "d" -> b),
        Map("a" -> a, "b" -> c, "c" -> a, "d" -> c))
  }

  test("should be able to set properties with a literal map twice in the same transaction") {
    val node = createLabeledNode("FOO")

    graph.inTx {
      executeWithRulePlanner("MATCH (n:FOO) SET n = { first: 'value' }")
      executeWithRulePlanner("MATCH (n:FOO) SET n = { second: 'value' }")
    }

    graph.inTx {
      node.getProperty("first", null) should equal (null)
      node.getProperty("second") should equal ("value")
    }
  }

  test("should not fail if asking for a non existent node id with WHERE") {
    executeWithAllPlanners("match (n) where id(n) in [0,1] return n").toList
    // should not throw an exception
  }

  test("non optional patterns should not contain nulls") {
    // given
    val h = createNode()
    val g = createNode()
    val t = createNode()
    val b = createNode()

    relate(h, g)
    relate(h, t)
    relate(h, b)
    relate(g, t)
    relate(g, b)
    relate(t, b)

    val result = profile("MATCH h-[r1]-n-[r2]-g-[r3]-o-[r4]-h, n-[r]-o where id(h) = 1 and id(g) = 2 RETURN o")

    // then
    assert(!result.columnAs[Node]("o").contains(null), "Result should not contain nulls")
  }


  test("should handle queries that cant be index solved because expressions lack dependencies") {
    // given
    val a = createLabeledNode(Map("property" -> 42), "Label")
    val b = createLabeledNode(Map("property" -> 42), "Label")
    createLabeledNode(Map("property" -> 666), "Label")
    createLabeledNode(Map("property" -> 666), "Label")
    val e = createLabeledNode(Map("property" -> 1), "Label")
    relate(a, b)
    relate(a, e)
    graph.createIndex("Label", "property")

    // when
    val result = executeWithAllPlanners("match (a:Label)-->(b:Label) where a.property = b.property return a, b")

    // then does not throw exceptions
    result.toList should equal (List(Map("a" -> a, "b" -> b)))
  }

  test("should handle queries that cant be index solved because expressions lack dependencies with two disjoin patterns") {
    // given
    val a = createLabeledNode(Map("property" -> 42), "Label")
    val b = createLabeledNode(Map("property" -> 42), "Label")
    val e = createLabeledNode(Map("property" -> 1), "Label")
    graph.createIndex("Label", "property")

    // when
    val result = executeWithAllPlanners("match (a:Label), (b:Label) where a.property = b.property return *")

    // then does not throw exceptions
    assert(result.toSet === Set(
      Map("a" -> a, "b" -> a),
      Map("a" -> a, "b" -> b),
      Map("a" -> b, "b" -> b),
      Map("a" -> b, "b" -> a),
      Map("a" -> e, "b" -> e)
    ))
  }

  test("should use the index for property existence queries (with has) for cost when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) USING INDEX n:User(email) WHERE has(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should use the index for property existence queries (with exists) for cost when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) USING INDEX n:User(email) WHERE exists(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should use the index for property existence queries (with IS NOT NULL) for cost when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) USING INDEX n:User(email) WHERE n.email IS NOT NULL RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  private def setupIndexScanTest(): Seq[Node] = {
    for (i <- 1 to 100) {
      createLabeledNode(Map("name" -> ("Joe Soap " + i)), "User")
    }
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")
    graph.createIndex("User", "name")
    Seq(n, m, p)
  }

  test("should use the index for property existence queries when cardinality prefers it") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) WHERE has(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes(0)), Map("n" -> nodes(1))))
    result.executionPlanDescription().toString should include("NodeIndexScan")
  }

  test("should not use the index for property existence queries when cardinality does not prefer it") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) WHERE has(n.name) RETURN n")

    // then
    result.toList.length should equal(100)
    result.executionPlanDescription().toString should include("NodeByLabelScan")
  }

  test("should not use the index for property existence queries when property value predicate exists") {
    // given
    val nodes = setupIndexScanTest()

    // when
    val result = executeWithCostPlannerOnly("MATCH (n:User) WHERE has(n.email) AND n.email = 'me@mine' RETURN n")

    // then
    result.toList should equal(List(Map("n" -> nodes(0))))
    result.executionPlanDescription().toString should include("NodeIndexSeek")
    result.executionPlanDescription().toString should not include("NodeIndexScan")
  }

  test("should use the index for property existence queries for rule when asked for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = eengine.execute("CYPHER planner=rule MATCH (n:User) USING INDEX n:User(email) WHERE has(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should include("SchemaIndex")
  }

  test("should not use the index for property existence queries for rule when not asking for it") {
    // given
    val n = createLabeledNode(Map("email" -> "me@mine"), "User")
    val m = createLabeledNode(Map("email" -> "you@yours"), "User")
    val p = createLabeledNode(Map("emailx" -> "youtoo@yours"), "User")
    graph.createIndex("User", "email")

    // when
    val result = eengine.execute("CYPHER planner=rule MATCH (n:User) WHERE has(n.email) RETURN n")

    // then
    result.toList should equal(List(Map("n" -> n), Map("n" -> m)))
    result.executionPlanDescription().toString should not include("SchemaIndex")
  }

  test("should handle cyclic patterns") {
    // given
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "A")
    relate(b, a, "B")
    relate(b, c, "B")

    // when
    val result = executeWithAllPlanners("match (a)-[r1:A]->(x)-[r2:B]->(a) return a.name")

    // then does not throw exceptions
    assert(result.toList === List(
      Map("a.name" -> "a")
    ))
  }

  test("should handle cyclic patterns (broken up into two paths)") {
    // given
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "A")
    relate(b, a, "B")
    relate(b, c, "B")

    // when
    val result = executeWithAllPlanners("match (a)-[:A]->(b), (b)-[:B]->(a) return a.name")

    // then does not throw exceptions
    assert(result.toList === List(
      Map("a.name" -> "a")
    ))
  }

  test("should match fixed-size var length pattern") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val result = executeWithAllPlanners("match (a)-[r*1..1]->(b) return r")
    result.toList should equal (List(Map("r" -> List(r))))
  }

  test("should only evaluate non-deterministic predicates after pattern is matched") {
    // given
    graph.inTx {
      (0 to 100) foreach {
        x => createNode()
      }
    }

    // when
    val count = executeScalar[Long]("match (a) where rand() < .5 return count(*)")

    // should give us a number in the middle, not all or nothing
    count should not equal 0
    count should not equal 100
  }

  test("should not find any matches when a node in a pattern is null") {
    // given empty db

    // when
    val result = executeWithAllPlanners("optional match (a) with a match (a)-->(b) return b")

    // then
    result.toList should be(empty)
  }

  test("should not find node in the match if there is a filter on the optional match") {
    // given
    val a = createNode()
    val b = createNode()
    relate(a, b)

    // when
    val result = executeWithAllPlanners("optional match (a:Person) with a match (a)-->(b) return b").columnAs[Node]("b")

    // then
    result.toList should be(empty)
  }

  test("optional match starting from a null node returns null") {
    // given empty db

    // when
    val result = executeWithAllPlanners("optional match (a) with a optional match (a)-->(b) return b")

    // then
    result.toList should equal (List(Map("b"->null)))
  }

  test("optional match returns null") {
    // given empty db

    // when
    val result = executeWithAllPlanners("optional match (a) return a")

    // then
    result.toList should equal (List(Map("a" -> null)))
  }

  test("match p = (a) return p") {
    // given a single node
    val node = createNode()

    // when
    val result = executeWithAllPlanners("match p = (a) return p")

    // should give us a number in the middle, not all or nothing
    result.toList should equal (List(Map("p"->new PathImpl(node))))
  }

  test("match p = (a)-[r*0..]->(b) return p") {
    // given a single node
    val node = createNode()

    // when
    val result = executeWithAllPlanners("match p = (a)-[r*0..]->(b) return p")

    // should give us a single, empty path starting at one end
    result.toList should equal (List(Map("p"-> new PathImpl(node))))
  }

  test("MATCH n RETURN n.prop AS m, count(n) AS count") {
    // given a single node
    createNode("prop" -> "42")

    // when
    val result = executeWithAllPlanners("MATCH n RETURN n.prop AS n, count(n) AS count")

    // should give us a single, empty path starting at one end
    result.toList should equal (List(Map("n" -> "42", "count" -> 1)))
  }

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    // given two disconnected rels
    val rel1 = relate(createNode(), createNode())
    val rel2 = relate(createNode(), createNode())

    // when
    val result = executeWithAllPlanners("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel")

    // should give us all rels
    val actual = relsById(result.columnAs[Relationship]("rel").toList)
    val expected = relsById(Seq(rel1, rel2))

    result.columns should equal(List("rel"))
    actual should equal(expected)
  }

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2, count(*) AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    // given two disconnected rels
    val rel1 = relate(createNode(), createNode())
    val rel2 = relate(createNode(), createNode())

    // when
    val result = executeWithAllPlanners("MATCH (u)-[r1]->(v) WITH r1 AS r2, count(*) AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel")

    // should give us all rels
    val actual = relsById(result.columnAs[Relationship]("rel").toList)
    val expected = relsById(Seq(rel1, rel2))

    result.columns should equal(List("rel"))
    actual should equal(expected)
  }

  test("MATCH (a)-[r]->(b) WITH a, r, b, count(*) AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel") {
    // given two disconnected rels
    val rel1 = relate(createNode(), createNode())
    val rel2 = relate(createNode(), createNode())

    // when
    val result = executeWithAllPlanners("MATCH (a)-[r]->(b) WITH a, r, b, count(*) AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel")

    // should give us all rels
    val actual = relsById(result.columnAs[Relationship]("rel").toList)
    val expected = relsById(Seq(rel1, rel2))

    result.columns should equal(List("rel"))
    actual should equal(expected)
  }

  test("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a2" -> node1, "r" -> relationship, "b2" -> node2)))
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)-[r]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)-[r]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a1" -> node1, "r" -> relationship, "b2" -> node2)))
  }

  test("MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:X)-[r]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2)

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:X)-[r]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should be(empty)
  }

  test("MATCH (a1:X:Y)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:Y)-[r]->(b2) RETURN a1, r, b2") {
    val node1 = graph.inTx({
      val node = createNode()
      node.addLabel(DynamicLabel.label("X"))
      node.addLabel(DynamicLabel.label("Y"))
      node
    })
    val node2 = createNode()
    relate(node1, node2)

    // when
    val result = executeWithAllPlanners("MATCH (a1:X:Y)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:Y)-[r]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual shouldNot be(empty)
  }

  test("MATCH (a1)-[r:X]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2, "X")

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r:X]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should be(empty)
  }

  test("MATCH (a1)-[r:Y]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2, "Y")

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r:Y]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual shouldNot be(empty)
  }

  // todo: broken for rule planner
  test("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second") {
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2, "Y")
    relate(node2, node3, "Y")

    // when
    val result = executeWithCostPlannerOnly("MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")

    val actual = result.toSet

    actual should equal(Set(
      Map("first" -> node1, "second" -> node3)
    ))
  }

  test("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS first, b AS second LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second") {
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2, "Y")
    relate(node2, node3, "Y")

    // when
    val result = executeWithAllPlanners("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS first, b AS second LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")

    val actual = result.toList

    actual should equal(List(
      Map("first" -> node1, "second" -> node3)
    ))
  }

  test("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS second, b AS first LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second") {
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2, "Y")
    relate(node2, node3, "Y")

    // when
    val result = executeWithAllPlanners("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS second, b AS first LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")

    val actual = result.toList

    actual should be(empty)
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a1" -> node1, "r" -> relationship, "b2" -> null)))
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithAllPlanners("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a1" -> node1, "r" -> relationship, "b2" -> null, "a2" -> null)))
  }

  test("MATCH n WITH n.prop AS n2 RETURN n2.prop") {
    // Given a single node
    createNode("prop" -> "42")

    // then
    intercept[CypherTypeException](executeWithAllPlanners("MATCH n WITH n.prop AS n2 RETURN n2.prop"))
  }

  test("MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4") {
    createNode("bar" -> 1)
    createNode("bar" -> 3)
    createNode("bar" -> 2)

    // when
    val result = executeWithAllPlanners("MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4")
    result.toList should equal(List(
      Map("x" -> 3),
      Map("x" -> 2),
      Map("x" -> 1)
    ))
  }

  test("MATCH a RETURN count(a) > 0") {
    val result = executeWithAllPlanners("MATCH a RETURN count(a) > 0")
    result.toList should equal(List(
      Map("count(a) > 0" -> false)
    ))
  }

  test("MATCH (a:Artist)-[:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *") {
    val a = createLabeledNode("Artist")
    val b = createLabeledNode("Artist")
    val c = createLabeledNode("Artist")

    relate(a, b, "WORKED_WITH", Map("year" -> 1987))
    relate(b, c, "WORKED_WITH", Map("year" -> 1988))

    val result = executeWithAllPlanners("MATCH (a:Artist)-[:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *")
    result.toList should equal(List(
      Map("a" -> b, "b" -> c)
    ))
  }

  private def relsById(in: Seq[Relationship]): Seq[Relationship] = in.sortBy(_.getId)

  // todo: broken for rule planner
  test("should return shortest paths when only one side is bound") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val r1 = relate(a, b)

    val result = executeWithAllPlanners("match (a:A) match p = shortestPath( a-[*]->(b:B) ) return p").toList.head("p").asInstanceOf[Path]

    graph.inTx {
      result.startNode() should equal(a)
      result.endNode() should equal(b)
      result.length() should equal(1)
      result.lastRelationship() should equal (r1)
    }
  }

  test("should not break when using pattern expressions and order by") {
    val query =
      """
        |    MATCH (liker)
        |    RETURN (liker)-[]-() AS isNew
        |    ORDER BY liker.time
      """.stripMargin

    executeWithAllPlanners(query).toList
  }

  test("issue #2907, varlength pattern should check label on endnode") {

    val a = createLabeledNode("LABEL")
    val b = createLabeledNode("LABEL")
    val c = createLabeledNode("LABEL")

    relate(a, b,  "r")
    relate(b, c,  "r")

    val query = s"""MATCH (a), (b)
                   |WHERE id(a) = ${a.getId} AND
                   |      (a)-[:r]->(b:LABEL) OR
                   |      (a)-[:r*]->(b:MISSING_LABEL)
                   |RETURN DISTINCT b""".stripMargin


    //WHEN
    val result = executeWithAllPlanners(query)

    //THEN
    result.toList should equal (List(Map("b" -> b)))
  }

  test("make sure that we are handling arguments in leaf plans") {

    val a = createLabeledNode("LABEL")
    val b = createLabeledNode("LABEL")
    val c = createLabeledNode("LABEL")

    relate(a, b,  "r")
    relate(b, c,  "r")

    val query = s"""MATCH (a), (b) WHERE (id(a) = ${a.getId} OR (a)-[:r]->(b:MISSING_LABEL)) AND ((a)-[:r]->(b:LABEL) OR (a)-[:r]->(b:MISSING_LABEL)) RETURN b""".stripMargin

    //WHEN
    val result = executeWithAllPlanners(query)

    //THEN
    result.toList should equal (List(Map("b" -> b)))
  }

  test("issue #2907 should only check label on end node") {

    val a = createLabeledNode("BLUE")
    val b = createLabeledNode("RED")
    val c = createLabeledNode("GREEN")
    val d = createLabeledNode("YELLOW")

    relate(a, b,  "r")
    relate(b, c,  "r")
    relate(b, d,  "r")

    val query = s"""MATCH (a:BLUE)-[r*]->(b:GREEN) RETURN count(r)""".stripMargin


    //WHEN
    val result = executeScalar[Long](query)

    //THEN
    result should equal (1)
  }

  test("Should be able to run delete/merge query multiple times") {
    //GIVEN
    createLabeledNode("User")

    val query = """MATCH (:User)
                  |MERGE (project:Project)
                  |MERGE (user)-[:HAS_PROJECT]->(project)
                  |WITH project
                  |    // delete the current relations to be able to replace them with new ones
                  |OPTIONAL MATCH (project)-[hasFolder:HAS_FOLDER]->(:Folder)
                  |OPTIONAL MATCH (project)-[:HAS_FOLDER]->(folder:Folder)
                  |DELETE folder, hasFolder
                  |WITH project
                  |   // add the new relations and objects
                  |FOREACH (el in[{name:"Dir1"}, {name:"Dir2"}] |
                  |  MERGE (folder:Folder{ name: el.name })
                  |  MERGE (project)–[:HAS_FOLDER]->(folder))
                  |RETURN DISTINCT project""".stripMargin

    //WHEN
    val first = eengine.execute(query).toList
    val second = eengine.execute(query).toList
    val check = executeWithAllPlanners("MATCH (f:Folder) RETURN f.name").toSet

    //THEN
    first should equal(second)
    check should equal(Set(Map("f.name" -> "Dir1"), Map("f.name" -> "Dir2")))
  }

  test("Should be able to run delete/merge query multiple times, match on property") {
    //GIVEN
    createLabeledNode("User")


    val query = """MATCH (:User)
                  |MERGE (project:Project)
                  |MERGE (user)-[:HAS_PROJECT]->(project)
                  |WITH project
                  |    // delete the current relations to be able to replace them with new ones
                  |OPTIONAL MATCH (project)-[hasFolder:HAS_FOLDER]->({name: "Dir2"})
                  |OPTIONAL MATCH (project)-[hasFolder2:HAS_FOLDER]->({name: "Dir1"})
                  |OPTIONAL MATCH (project)-[:HAS_FOLDER]->(folder {name: "Dir1"})
                  |DELETE folder, hasFolder, hasFolder2
                  |WITH project
                  |   // add the new relations and objects
                  |FOREACH (el in[{name:"Dir1"}, {name:"Dir2"}] |
                  |  MERGE (folder:Folder{ name: el.name })
                  |  MERGE (project)–[:HAS_FOLDER]->(folder))
                  |RETURN DISTINCT project""".stripMargin

    //WHEN
    val first = eengine.execute(query).toList
    val second = eengine.execute(query).toList
    val check = executeWithAllPlanners("MATCH (f:Folder) RETURN f.name").toSet

    //THEN
    first should equal(second)
    check should equal(Set(Map("f.name" -> "Dir1"), Map("f.name" -> "Dir2")))
  }

  test("index hints should work in optional match") {
    //GIVEN
    val subnet = createLabeledNode("Subnet")
    createLabeledNode("Subnet")//extra dangling subnet
    val host = createLabeledNode(Map("name" -> "host"), "Host")

    relate(subnet, host)

    graph.createIndex("Host", "name")

    val query =
      """MATCH (subnet: Subnet)
        |OPTIONAL MATCH (subnet)-->(host:Host)
        |USING INDEX host:Host(name)
        |WHERE host.name = 'host'
        |RETURN host""".stripMargin

    //WHEN
    val result = profile(query)

    //THEN
    result.toList should equal (List(Map("host" -> host), Map("host" -> null)))
  }

  test("Undirected paths should be properly handled") {
    //GIVEN
    val node1 = createLabeledNode("Movie")
    val node2 = createNode()
    val rel = relate(node2, node1)

    val query =
      """profile match p = (n:Movie)--(m) return p limit 1""".stripMargin

    graph.inTx {
      val res = executeWithAllPlanners(query).toList
      val path = res.head("p").asInstanceOf[Path]
      path.startNode should equal(node1)
      path.endNode should equal(node2)
    }
  }

  test("named paths should work properly with WITH") {
    val a = createNode()
    val query = """MATCH p = (a)
                  |WITH p
                  |RETURN p
                  | """.stripMargin

    val result = executeWithAllPlanners(query).toList
    result should equal(List(Map("p" -> PathImpl(a))))
  }

  test("Named paths with directed followed by undirected relationships") {
    //GIVEN
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    val twoToOne = relate(node2, node1)
    val threeToTwo = relate(node3, node2)
    val query =
      """match p = (n)-->(m)--(o) return p""".stripMargin

    //WHEN
    val res = executeWithAllPlanners(query).toList

    //THEN
    graph.inTx {
      val path = res.head("p").asInstanceOf[Path]
      path.startNode should equal(node3)
      path.endNode should equal(node1)

      path.nodes().asScala.toList should equal(Seq(node3, node2, node1))
      path.relationships().asScala.toList should equal(Seq(threeToTwo, twoToOne))
    }
  }

  test("Named paths with directed followed by multiple undirected relationships") {
    //GIVEN
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    val node4 = createNode()
    val twoToOne = relate(node2, node1)
    val threeToTwo = relate(node3, node2)
    val fourToThree = relate(node4, node3)
    val query =
      """match path = (n)-->(m)--(o)--(p) return path""".stripMargin

    //WHEN
    val res = executeWithAllPlanners(query).toList

    //THEN
    graph.inTx {
      val path = res.head("path").asInstanceOf[Path]
      path.startNode should equal(node4)
      path.endNode should equal(node1)

      path.nodes().asScala.toList should equal(Seq(node4, node3, node2, node1))
      path.relationships().asScala.toList should equal(Seq(fourToThree, threeToTwo, twoToOne))
    }
  }

  test("should handle cartesian products even when same argument exists on both sides") {
    val node1 = createNode()
    val node2 = createNode()
    val r = relate(node1, node2)

    val query = """WITH [{0}, {1}] AS x, count(*) as y
                  |MATCH (n) WHERE ID(n) IN x
                  |MATCH (m) WHERE ID(m) IN x
                  |MATCH paths = allShortestPaths((n)-[*..1]-(m))
                  |RETURN paths""".stripMargin

    val result = executeWithAllPlanners(query, "0" -> node1.getId, "1" -> node2.getId)
    graph.inTx(
      result.toSet should equal(Set(Map("paths" -> new PathImpl(node1, r, node2)), Map("paths" -> new PathImpl(node2, r, node1))))
    )
  }

  test("should handle paths of containing undirected varlength") {
    // given
    val db1 = createLabeledNode("Start")
    val db2 = createLabeledNode("End")
    val mid = createNode()
    val other = createNode()
    relate(mid, db1, "CONNECTED_TO")
    relate(mid, db2, "CONNECTED_TO")
    relate(mid, db2, "CONNECTED_TO")
    relate(mid, other, "CONNECTED_TO")
    relate(mid, other, "CONNECTED_TO")

    // when
    val query = "MATCH topRoute = (db1:Start)<-[:CONNECTED_TO]-()-[:CONNECTED_TO*3..3]-(db2:End) RETURN topRoute"

    executeWithAllPlanners(query).toList should have size 4
  }

  test("should return empty result when there are no relationship with the given id") {
    executeWithAllPlanners("MATCH ()-[r]->() WHERE id(r) = 42 RETURN r") shouldBe empty
    executeWithAllPlanners("MATCH ()<-[r]-() WHERE id(r) = 42 RETURN r") shouldBe empty
    executeWithAllPlanners("MATCH ()-[r]-() WHERE id(r) = 42 RETURN r") shouldBe empty
  }

  test("should use NodeByIdSeek for id array in identifier") {
    // given
    val a = createNode().getId
    val b = createNode().getId
    val c = createNode().getId
    val d = createNode().getId
    1.to(1000).foreach(_ => createNode())

    // when
    val result = executeWithCostPlannerOnly(s"profile WITH [$a,$b,$d] AS arr MATCH (n) WHERE id(n) IN arr return count(*)")

    // then
    result.toList should equal(List(Map("count(*)" -> 3)))
  }

  test("should return null as value for non-existent property") {
    createNode(Map("foo" -> 1))

    val query = "MATCH a RETURN a.bar"

    val result = executeWithAllPlanners(query).toList
    result should equal(List(Map("a.bar" -> null)))
  }

  test("should return property value for matched node") {
    val props = Map("prop" -> 1)
    createNode(props)

    val query = "MATCH a RETURN a.prop"

    val result = executeWithAllPlanners(query).toComparableResult
    result should equal(List(asResult(props, "a")))
  }

  test("should return property value for matched relationship") {
    relate(createNode(), createNode(), "prop" -> 1)

    val query = "MATCH a-[r]->b RETURN r.prop"

    val result = executeWithAllPlanners(query).toComparableResult
    result should equal(List(asResult(Map("prop" -> 1), "r")))
  }

  test("should return property value for matched node and relationship") {
    val nodeProp = Map("nodeProp" -> 1)
    relate(createNode(nodeProp), createNode(), "relProp" -> 2)

    val query = "MATCH a-[r]->b RETURN a.nodeProp, r.relProp"

    val result = executeWithAllPlanners(query).toComparableResult
    result should equal(List(asResult(Map("nodeProp" -> 1), "a") ++ asResult(Map("relProp" -> 2), "r")))
  }

  test("should be able to project both nodes and relationships") {
    val a = createNode()
    val r = relate(a, createNode())

    val query = "MATCH a-[r]->b RETURN a AS FOO, r AS BAR"

    val result = executeWithAllPlanners(query).toComparableResult
    result should equal(List(Map("FOO" -> a, "BAR" -> r)))
  }

  test("should return null as value for non-existent relationship property") {
    relate(createNode(), createNode(), "prop" -> 1)

    val query = "MATCH a-[r]->b RETURN r.foo"

    val result = executeWithAllPlanners(query).toComparableResult
    result should equal(List(Map("r.foo" -> null)))
  }

  test("should return multiple property values for matched node") {
    val props = Map[String, Any](
      "name" -> "Philip J. Fry",
      "age" -> 2046,
      "seasons" -> Array(1, 2, 3, 4, 5, 6, 7))

    createNode(props)

    val query = "MATCH a RETURN a.name, a.age, a.seasons"

    val result = executeWithAllPlanners(query).toComparableResult
    result should equal(List(asResult(props, "a")))
  }

  test("adding a property and a literal is supported in new runtime") {
    val props = Map("prop" -> 1)
    createNode(props)
    val result = executeWithAllPlanners("MATCH a RETURN a.prop + 1 AS FOO").toComparableResult

    result should equal(List(Map("FOO" -> 2)))
  }

  test("adding arrays is supported in new runtime") {
    val props = Map("prop1" -> Array(1,2,3), "prop2" -> Array(4, 5))
    createNode(props)
    val result = executeWithAllPlanners("MATCH a RETURN a.prop1 + a.prop2 AS FOO").toComparableResult

    result should equal(List(Map("FOO" -> List(1, 2, 3, 4, 5))))
  }

  test("should type var length identifiers correctly as collection of relationships") {
    createNode()
    val r = relate(createNode(), createNode())

    val result = executeWithAllPlanners("match ()-[r*0..1]-() return last(r) as l").toList

    result should equal(List(Map("l" -> null), Map("l" -> null), Map("l" -> r), Map("l" -> null), Map("l" -> r)))
  }

  test("should correctly handle nulls in var length expand") {
    val node = createLabeledNode("A")
    createLabeledNode("B")

    val query =
      """match (a:A)
        |optional match (a)-[r1:FOO]->(b:B)
        |optional match (b)<-[r2:BAR*]-(c:B)
        |return a, b, c""".stripMargin

    val result = executeWithAllPlanners(query).toList

    result should equal(List(Map("a" -> node, "b" -> null, "c" -> null)))
  }

  test("should handle varlength paths of size 0..0") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(b, c)

    val query =
      """match (a)-[*0..0]->(b)
        |return a, b""".stripMargin

    val result = executeWithAllPlanners(query).toSet

    result should equal(Set(
      Map("a" -> a, "b" -> a),
      Map("a" -> b, "b" -> b),
      Map("a" -> c, "b" -> c)
    ))
  }

  test("properly handle collections of nodes and relationships") {
    val node1 = createNode()
    val node2 = createNode()
    val rel = relate(node1, node2)
    val result = executeWithAllPlanners("match (n)-[r]->(m) return [n, r, m] as r").toComparableResult

    result should equal(Seq(Map("r" -> Seq(node1, rel, node2))))
  }

  test("properly handle maps of nodes and relationships") {
    val node1 = createNode()
    val node2 = createNode()
    val rel = relate(node1, node2)
    val result = executeWithAllPlanners("match (n)-[r]->(m) return {node1: n, rel: r, node2: m} as m").toComparableResult

    result should equal(Seq(Map("m" -> Map("node1" -> node1, "rel" -> rel, "node2" -> node2))))
  }

  test("matching existing path with two nodes should respect relationship direction") {
    val a = createNode("prop" -> "a")
    val b = createNode("prop" -> "b")
    val rel = relate(a, b)

    val result = executeWithAllPlanners("MATCH p=({prop: 'a'})-->({prop: 'b'}) RETURN p").toList

    result shouldBe List(Map("p" -> PathImpl(a, rel, b)))
  }

  test("matching non-existing path with two nodes should respect relationship direction") {
    val a = createNode("prop" -> "a")
    val b = createNode("prop" -> "b")
    relate(a, b)

    val result = executeWithAllPlanners("MATCH p=({prop: 'a'})<--({prop: 'b'}) RETURN p").toList

    result shouldBe empty
  }

  test("issue 4692 path matching should respect relationship directions") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(b, a)

    val result = executeWithAllPlanners("MATCH p=(n)-->(k)<--(n) RETURN p").toList

    result shouldBe empty
  }

  test("matching path with single <--> relationship should respect other relationship directions") {
    val a = createNode()
    val b = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, a)

    val results = executeWithAllPlanners("MATCH p=(n)<-->(k)<--(n) RETURN p").toList

    results should contain theSameElementsAs List(
      Map("p" -> PathImpl(a, r2, b, r1, a)),
      Map("p" -> PathImpl(b, r1, a, r2, b)))
  }

  test("matching path with only <--> relationships should work") {
    val a = createNode()
    val b = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, a)

    val results = executeWithAllPlanners("MATCH p=(n)<-->(k)<-->(n) RETURN p").toList

    results should contain theSameElementsAs List(
      Map("p" -> PathImpl(a, r2, b, r1, a)),
      Map("p" -> PathImpl(a, r1, b, r2, a)),
      Map("p" -> PathImpl(b, r1, a, r2, b)),
      Map("p" -> PathImpl(b, r2, a, r1, b)))
  }

  test("columns should be in the provided order") {
    val result = executeWithAllPlanners("MATCH p,o,n,t,u,s RETURN p,o,n,t,u,s")

    result.columns should equal(List("p", "o", "n", "t", "u", "s"))
  }

  test("should be able to match on nodes with MANY labels") {
    //given
    val start = createLabeledNode('A' to 'M' map(_.toString):_* )
    val end = createLabeledNode('U' to 'Z' map(_.toString):_* )
    relate(start, end, "REL")

    //when
    val result = executeWithAllPlanners("match (n:A:B:C:D:E:F:G:H:I:J:K:L:M)-[:REL]->(m:Z:Y:X:W:V:U) return n,m")

    //then
    result.toList should equal(List(Map("n" -> start, "m" -> end)))
  }

  test("should be able to do varlength matches of sizes larger that 15 hops") {
    //given
    //({prop: "bar"})-[:R]->({prop: "bar"})…-[:R]->({prop: "foo"})
    val start = createNode(Map("prop" -> "start"))
    val end = createNode(Map("prop" -> "end"))
    val nodes = start +: (for (i <- 1 to 15) yield createNode(Map("prop" -> "bar"))) :+ end
    nodes.sliding(2).foreach {
      case Seq(node1, node2) => relate(node1, node2, "R")
    }

    val result = executeWithAllPlanners("MATCH (n {prop: 'start'})-[:R*]->(m {prop: 'end'}) RETURN m")

    result.toList should equal(List(Map("m" -> end)))
  }

  test("aliasing node names should not change estimations but it should simply introduce a projection") {
    val b = createLabeledNode("B")
    (0 to 10).foreach { i =>
      val a = createLabeledNode("A")
      relate(a, b)
    }

    val resultNoAlias = graph.execute("MATCH (a:A) with a SKIP 0 MATCH (a)-[]->(b:B) return a, b")
    resultNoAlias.asScala.toList.size should equal(11)
    val resultWithAlias = graph.execute("MATCH (a:A) with a as n SKIP 0 MATCH (n)-[]->(b:B) return n, b")
    resultWithAlias.asScala.toList.size should equal(11)

    var descriptionNoAlias = resultNoAlias.getExecutionPlanDescription
    var descriptionWithAlias = resultWithAlias.getExecutionPlanDescription
    descriptionWithAlias.getArguments.get("EstimatedRows") should equal(descriptionNoAlias.getArguments.get("EstimatedRows"))
    while (descriptionWithAlias.getChildren.isEmpty) {
      descriptionWithAlias = single(descriptionWithAlias.getChildren)
      if ( descriptionWithAlias.getName != "Projection" ) {
        descriptionNoAlias = single(descriptionNoAlias.getChildren)
        descriptionWithAlias.getArguments.get("EstimatedRows") should equal(descriptionNoAlias.getArguments.get("EstimatedRows"))
      }
    }

    resultNoAlias.close()
    resultWithAlias.close()
  }

  /**
   * Append identifier to keys and transform value arrays to lists
   */
  private def asResult(data: Map[String, Any], id: String) =
    data.map {
      case (k, v) => (s"$id.$k", v)
    }.mapValues {
      case v: Array[_] => v.toList
      case v => v
    }
}
