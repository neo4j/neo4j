/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.graphdb._
import org.junit.Assert._
import org.neo4j.cypher.internal.PathImpl

class MatchAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport {

  test("should be able to use params in pattern matching predicates") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2, "A", Map("foo" -> "bar"))

    val result = execute("match a-[r]->b where r.foo =~ {param} return b", "param" -> "bar")

    result.columnAs("b").toList should equal (List(n2))
  }

  test("should filter out based on node prop name") {
    val name = "Andres"
    val start: Node = createNode()
    val a1: Node = createNode(Map("name" -> "Someone Else"))
    val a2: Node = createNode(Map("name" -> name))
    relate(start, a1, "x")
    relate(start, a2, "x")

    val result = execute(
      s"MATCH (start)-[rel:x]-(a) WHERE a.name = '$name' return a"
    )
    result.columnAs[Node]("a").toList should equal (List(a2))
  }

  test("should filter based on rel prop name") {
    val start: Node = createNode()
    val a: Node = createNode()
    val b: Node = createNode()
    relate(start, a, "KNOWS", Map("name" -> "monkey"))
    relate(start, b, "KNOWS", Map("name" -> "woot"))

    val result = execute(
      s"match (node)-[r:KNOWS]->(a) WHERE r.name = 'monkey' RETURN a"
    )
    result.columnAs[Node]("a").toList should equal(List(a))
  }

  test("should get neighbours") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val result = execute(
      s"match (n1)-[rel:KNOWS]->(n2) RETURN n1, n2"
    )

    result.toList should equal(List(Map("n1" -> n1, "n2" -> n2)))
  }

  test("should get two related nodes") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val result = execute(
      s"match (start)-[rel:KNOWS]->(x) return x"
    )

    result.toList should equal(List(Map("x" -> n2), Map("x" -> n3)))
  }

  test("should get related to related to") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val result = execute("match n-->a-->b RETURN b").toList

    result.toList should equal(List(Map("b" -> n3)))
  }

  test("should handle comparison between node properties") {
    //start n = node(1,4) match (n) --> (x) where n.animal = x.animal return n,x
    val n1 = createNode(Map("animal" -> "monkey"))
    val n2 = createNode(Map("animal" -> "cow"))
    val n3 = createNode(Map("animal" -> "monkey"))
    val n4 = createNode(Map("animal" -> "cow"))

    relate(n1, n2, "A")
    relate(n1, n3, "A")
    relate(n4, n2, "A")
    relate(n4, n3, "A")

    val result = execute(
      """match (n)-[rel]->(x)
        where n.animal = x.animal
        return n, x"""
    )

    result.toList should equal(List(
      Map("n" -> n1, "x" -> n3),
      Map("n" -> n4, "x" -> n2)))

    result.columns should equal(List("n", "x"))
  }

  test("comparing numbers should work nicely") {
    val n1 = createNode(Map("x" -> 50))
    val n2 = createNode(Map("x" -> 50l))
    val n3 = createNode(Map("x" -> 50f))
    val n4 = createNode(Map("x" -> 50d))
    val n5 = createNode(Map("x" -> 50.toByte))

    val result = execute(
      s"match n where n.x < 100 return n"
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2, n3, n4, n5))
  }

  test("comparing string and chars should work nicely") {
    val n1 = createNode(Map("x" -> "Anders"))
    val n2 = createNode(Map("x" -> 'C'))
    createNode(Map("x" -> "Zzing"))
    createNode(Map("x" -> 'Ä'))

    val result = execute(
      s"match n where n.x < 'Z' AND n.x < 'z' return n"
    )

    result.columnAs("n").toList should equal(List(n1, n2))
  }

  test("should return two subgraphs with bound undirected relationship") {
    val a = createNode("a")
    val b = createNode("b")
    relate(a, b, "rel", "r")

    val result = execute("start r=rel(0) match a-[r]-b return a,b")

    result.toList should equal(List(Map("a" -> a, "b" -> b), Map("a" -> b, "b" -> a)))
  }

  test("should return two subgraphs with bound undirected relationship and optional relationship") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "rel", "r")
    relate(b, c, "rel", "r2")

    val result = execute("start r=rel(0) match (a)-[r]-(b) optional match (b)-[r2]-(c) where r<>r2 return a,b,c")
    result.toList should equal(List(Map("a" -> a, "b" -> b, "c" -> c), Map("a" -> b, "b" -> a, "c" -> null)))
  }

  test("magic rel type works as expected") {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = execute(
      "start n = node(0) match (n)-[r]->(x) where type(r) = 'KNOWS' return x"
    )

    result.columnAs[Node]("x").toList should equal(List(node("B")))
  }

  test("should walk alternative relationships") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("A" -> "HATES" -> "C")
    relate("A" -> "WONDERS" -> "C")

    val result = execute(
      "match (n)-[r]->(x) where type(r) = 'KNOWS' OR type(r) = 'HATES' return r"
    )

    result.columnAs("r").toList should equal(List(r1, r2))
  }

  test("should return a simple path") {
    createNodes("A", "B")
    val r = relate("A" -> "KNOWS" -> "B")

    val result = execute("start a = node(0) match p=a-->b return p")

    result.columnAs[Path]("p").toList should equal(List(PathImpl(node("A"), r, node("B"))))
  }

  test("should return a three node path") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start a = node(0) match p = a-[rel1]->b-[rel2]->c return p")

    result.columnAs("p").toList should equal(List(PathImpl(node("A"), r1, node("B"), r2, node("C"))))
  }

  test("should not return anything because path length doesnt match") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("match p = n-->x where length(p) = 10 return x")

    result shouldBe 'isEmpty
  }

  test("should pass the path length test") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("match p = n-->x where length(p)=1 return x")

    result shouldBe 'nonEmpty
  }


  test("should be able to filter on path nodes") {
    val a = createNode(Map("foo" -> "bar"))
    val b = createNode(Map("foo" -> "bar"))
    val c = createNode(Map("foo" -> "bar"))
    val d = createNode(Map("foo" -> "bar"))

    relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val result = execute("match p = pA-[:rel*3..3]->pB WHERE all(i in nodes(p) where i.foo = 'bar') return pB")

    result.columnAs("pB").toList should equal(List(d))
  }

  test("should return relationships") {
    val a = createNode(Map("foo" -> "bar"))
    val b = createNode(Map("foo" -> "bar"))
    val c = createNode(Map("foo" -> "bar"))

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("match p = a-[:rel*2..2]->b return RELATIONSHIPS(p)")

    result.columnAs[Node]("RELATIONSHIPS(p)").toList.head should equal(List(r1, r2))
  }

  test("should return a var length path") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("match p=(n {name:'A'})-[:KNOWS*1..2]->x return p")

    graph.inTx {
      result.columnAs("p").toList should equal(List(
        PathImpl(node("A"), r1, node("B")),
        PathImpl(node("A"), r1, node("B"), r2, node("C"))))
    }
  }

  test("a var length path of length zero") {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val result = execute("match p=a-[*0..1]->b return a,b, length(p) as l")

    result.toSet should equal(
      Set(
        Map("a" -> a, "b" -> a, "l" -> 0),
        Map("a" -> b, "b" -> b, "l" -> 0),
        Map("a" -> a, "b" -> b, "l" -> 1)))
  }

  test("a named var length path of length zero") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "FRIEND" -> "C")

    val result = execute("match p=(a {name:'A'})-[:KNOWS*0..1]->b-[:FRIEND*0..1]->c return p,a,b,c")

    graph.inTx {
      result.columnAs[Path]("p").toList should equal(
        List(
          PathImpl(node("A")),
          PathImpl(node("A"), r1, node("B")),
          PathImpl(node("A"), r1, node("B"), r2, node("C"))
        ))
    }
  }

  test("test zero length var len path in the middle") {
    createNodes("A", "B", "C", "D", "E")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "FRIEND" -> "C")


    val result = execute("match (a {name:'A'})-[:CONTAINS*0..1]->b-[:FRIEND*0..1]->c return a,b,c")

    result.toSet should equal(
      Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C")))
      )
  }

  test("should return a var length path without minimal length") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start n = node(0) match p=n-[:KNOWS*..2]->x return p")

    result.columnAs[Path]("p").toList should equal(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ))
  }

  test("should return a var length path with unbound max") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start n = node(0) match p=n-[:KNOWS*..]->x return p")

    result.columnAs[Path]("p").toList should equal(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ))
  }


  test("should handle bound nodes not part of the pattern") {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("MATCH (a {name:'A'}),(c {name:'C'}) match a-->b return a,b,c").toList

    result should equal (List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))))
  }

  test("should return shortest path") {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val result = execute("start a = node(0), b = node(1) match p = shortestPath(a-[*..15]-b) return p").
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
    execute("start a = node(0), b = node(1) match p = shortestPath(a-[*]-b) return p").toList
  }

  test("should not traverse same relationship twice in shortest path") {
    // given
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    // when
    val result = execute("MATCH (a{name:'A'}), (b{name:'B'}) MATCH p=allShortestPaths((a)-[:KNOWS|KNOWS*]->(b)) RETURN p").
      toList

    // then
    graph.inTx {
      result.size should equal (1)
    }
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

    val result = execute( """
MATCH (a {name:'A'}), (b {name:'B'})
MATCH a-[rA]->x<-[rB]->b
return x""")

    result.columnAs("x").toList should equal (List(x1, x2))
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

    val result = execute( """
MATCH (a {name:'A'}), (b {name:'B'}), (c {name:'C'})
match a-[rA]->x, b-[rB]->x, c-[rC]->x
return x""")

    result.columnAs("x").toList should equal (List(x1, x2))
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

    val result = execute( """
MATCH (a {name:'a'}), (b {name:'b'}), (c {name:'c'})
match a-->x, b-->x, c-->x
return x""")

    result.columnAs("x").toList should equal (List(d, e))
  }

  test("should split optional mandatory cleverly") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")

    relate(a, b, "knows", "rAB")
    relate(b, c, "knows", "rBC")

    val result = execute( """
start a  = node(0)
optional match (a)-[r1:knows]->(friend)-[r2:knows]->(foaf)
return foaf""")

    result.toList should equal (List(Map("foaf" -> c)))
  }

  test("should handle optional paths") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = execute( """
start a  = node(0), x = node(1,2)
optional match p = a --> x
return x, p""")

    assert(List(
      Map("x" -> b, "p" -> PathImpl(a, r, b)),
      Map("x" -> c, "p" -> null)
    ) === result.toList)
  }

  test("should handle optional paths from graph algo") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = execute( """
start a  = node(0), x = node(1,2)
optional match p = shortestPath(a -[*]-> x)
return x, p""").toList

    graph.inTx(assert(List(
      Map("x" -> b, "p" -> PathImpl(a, r, b)),
      Map("x" -> c, "p" -> null)
    ) === result))
  }

  test("should handle optional paths from a combo") {
    val a = createNode("A")
    val b = createNode("B")
    relate(a, b, "X")

    val result = execute( """
start a  = node(0)
optional match p = a-->b-[*]->c
return p""")

    assert(List(
      Map("p" -> null)
    ) === result.toList)
  }

  test("should handle optional paths from var length path") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = execute( """
start a = node(0), x = node(1,2)
optional match p = (a)-[r*]->(x)
return r, x, p""")

    assert(List(
      Map("r" -> Seq(r), "x" -> b, "p" -> PathImpl(a, r, b)),
      Map("r" -> null, "x" -> c, "p" -> null)
    ) === result.toList)
  }

  test("should return an iterable with all relationships from a var length") {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, c)

    val result = execute( """
start a  = node(0)
match a-[r*2]->c
return r""")

    result.toList should equal (List(Map("r" -> List(r1, r2))))
  }

  test("should handle all shortest paths") {
    createDiamond()

    val result = execute( """
start a  = node(0), d = node(3)
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

    val result = execute( """
start root  = node(0)
match p = root-[*]->leaf
where not(leaf-->())
return p, leaf""")

    assert(List(
      Map("leaf" -> b, "p" -> PathImpl(a, rab, b)),
      Map("leaf" -> d, "p" -> PathImpl(a, rac, c, rcd, d))
    ) === result.toList)
  }

  test("should exclude connected nodes") {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("B")
    relate(a, b)

    val result = execute( """
MATCH (a {name:'A'}), (other {name:'B'})
WHERE NOT a-->other
RETURN other""")

    result.toList should equal (List(Map("other" -> c)))
  }

  test("should not throw exception when stuff is missing") {
    val a = createNode()
    val b = createNode("Mark")
    relate(a, b)
    val result = execute( """
MATCH n-->x0
OPTIONAL MATCH x0-->x1
WHERE x1.foo = 'bar'
RETURN x0.name""")
    result.toList should equal (List(Map("x0.name" -> "Mark")))
  }

  test("should find nodes both directions") {
    val n = createNode()
    val a = createNode()
    relate(a, n, "Admin")
    val result = execute( """start n = node(0) match (n) -[:Admin]- (b) return id(n), id(b)""")
    result.toList should equal (List(Map("id(n)" -> 0, "id(b)" -> 1)))
  }


  test("should get all nodes") {
    val a = createNode()
    val b = createNode()

    val result = execute("match n return n")
    result.columnAs[Node]("n").toList should equal (List(a, b))
  }

  test("should allow comparisons of nodes") {
    val a = createNode()
    val b = createNode()

    val result = execute("MATCH a, b where a <> b return a,b")
    result.toSet should equal (Set(Map("a" -> b, "b" -> a), Map("b" -> b, "a" -> a)))
  }

  test("should solve selfreferencing pattern") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = execute("match a-->b, b-->b return b")

    result shouldBe 'isEmpty
  }

  test("should solve selfreferencing pattern2") {
    val a = createNode()
    val b = createNode()

    val r = relate(a, a)
    relate(a, b)

    val result = execute("match a-[r]->a return r")
    result.toList should equal (List(Map("r" -> r)))
  }

  test("relationship predicate with multiple rel types") {
    val a = createNode()
    val b = createNode()
    val x = createNode()

    relate(a, x, "A")
    relate(b, x, "B")

    val result = execute("match a where a-[:A|:B]->() return a").toList

    result should equal (List(Map("a" -> a), Map("a" -> b)))
  }

  test("nullable var length path should work") {
    createNode()
    val b = createNode()

    val result = execute("start a=node(0), b=node(1) optional match a-[r*]-b where r is null and a <> b return b").toList

    result should equal (List(Map("b" -> b)))
  }

  test("listing rel types multiple times should not give multiple returns") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "REL")

    val result = execute("match (a)-[:REL|:REL]->(b) return b").toList

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

    val q = "start a=node(0) match a-->n-->m return n.x, count(*) order by n.x"

    val resultWithoutLimit = execute(q)
    val resultWithLimit = execute(q + " limit 1000")

    resultWithoutLimit.toList should equal (resultWithLimit.toList)
  }

  test("should be able to handle single node patterns") {
    //GIVEN
    val n = createNode("foo" -> "bar")

    //WHEN
    val result = execute("match n where n.foo = 'bar' return n")

    //THEN
    result.toList should equal (List(Map("n" -> n)))
  }


  test("issue 479") {
    createNode()

    val q = "start n = node(0) optional match (n)-->(x) where x-->() return x"

    execute(q).toList should equal (List(Map("x" -> null)))
  }

  test("issue 479 has relationship to specific node") {
    createNode()

    val q = "start n = node(0) optional match (n)-[:FRIEND]->(x) where not n-[:BLOCK]->x return x"

    execute(q).toList should equal (List(Map("x" -> null)))
  }

  test("length on filter") {
    val q = "start n=node(*) optional match (n)-[r]->(m) return length(filter(x in collect(r) WHERE x <> null)) as cn"

    executeScalar[Long](q) should equal (0)
  }

  test("path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = execute("match p=b<--a return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("shortest Path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = execute("START a=node(0), b=node(1) match p=shortestPath(b<-[*]-a) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("empty collect should not contain null") {
    val n = createNode()
    val result = execute("MATCH n OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n, collect(x)")

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

    val result = execute("START a=node(0) MATCH a-->r-->b WHERE has(r.foo) RETURN r")

    result.toList should equal (List(Map("r" -> r1)))
  }

  test("can handle paths with multiple unnamed nodes") {
    createNode()
    val result = execute("START a=node(0) MATCH a<--()<--b-->()-->c RETURN c")

    result shouldBe 'isEmpty
  }

  test("path expressions should work with on the fly predicates") {
    val refNode = createNode()
    relate(refNode, createNode("name" -> "Neo"))
    val result = execute("START a=node({self}) MATCH a-->b WHERE b-->() RETURN b", "self" -> refNode)

    result shouldBe 'isEmpty
  }

  test("should filter nodes by label given in match") {
    // GIVEN
    val a = createNode()
    val b1 = createLabeledNode("foo")
    val b2 = createNode()

    relate(a, b1)
    relate(a, b2)

    // WHEN
    val result = execute(s"MATCH a-->(b:foo) RETURN b")

    // THEN
    result.toList should equal (List(Map("b" -> b1)))
  }

  test("should filter nodes by label given in match even if nodes are start nodes") {
    // GIVEN
    val a1 = createLabeledNode("bar")
    val a2 = createLabeledNode("baz")
    val b = createLabeledNode("foo")

    relate(a1, b)
    relate(a2, b)

    // WHEN
    val result = execute("START a=node(0,1), b=node(2) MATCH (a:bar) --> (b:foo) RETURN a")

    // THEN
    result.toList should equal (List(Map("a" -> a1)))
  }


  test("should use predicates in the correct place") {
    //GIVEN
    val m = execute( """create
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

    //WHEN
    val result = execute(
      """START advertiser = node({1}), a = node({2})
       MATCH (advertiser) -[:adv_has_product] ->(out) -[:ap_has_value] -> red <-[:aa_has_value]- (a)
       WHERE red.name = 'red' and out.name = 'product1'
       RETURN out.name""", "1" -> advertiser, "2" -> thing)

    //THEN
    result.toList should equal (List(Map("out.name" -> "product1")))
  }

  test("should be able to use index hints") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = execute("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name = 'Jacob' RETURN n")

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should be able to use label as start point") {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    //WHEN
    val result = execute("MATCH (n:Person)-->() WHERE n.name = 'Jacob' RETURN n")

    //THEN
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should not see updates created by itself") {
    createNode()

    val result = execute("match n create ()")
    assertStats(result, nodesCreated = 1)
  }

  test("id in where leads to empty result") {
    //WHEN
    val result = execute("MATCH n WHERE id(n)=1337 RETURN n")

    //THEN DOESN'T THROW EXCEPTION
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
    val result = execute("match a-->b match c-->d return a,b,c,d")

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
      execute("MATCH (n:FOO) SET n = { first: 'value' }")
      execute("MATCH (n:FOO) SET n = { second: 'value' }")
    }

    graph.inTx {
      node.getProperty("first", null) should equal (null)
      node.getProperty("second") should equal ("value")
    }
  }

  test("should not fail if asking for a non existent node id with WHERE") {
    execute("match (n) where id(n) in [0,1] return n").toList
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

    val result = profile("START h=node(1),g=node(2) MATCH h-[r1]-n-[r2]-g-[r3]-o-[r4]-h, n-[r]-o RETURN o")

    // then
    assert(!result.columnAs[Node]("o").exists(_ == null), "Result should not contain nulls")
  }


  test("should handle queries that cant be index solved because expressions lack dependencies") {
    // Given
    val a = createLabeledNode(Map("property" -> 42), "Label")
    val b = createLabeledNode(Map("property" -> 42), "Label")
    createLabeledNode(Map("property" -> 666), "Label")
    createLabeledNode(Map("property" -> 666), "Label")
    val e = createLabeledNode(Map("property" -> 1), "Label")
    relate(a, b)
    relate(a, e)
    graph.createIndex("Label", "property")

    // when
    val result = execute("match (a:Label)-->(b:Label) where a.property = b.property return a, b")

    // then does not throw exceptions
    result.toList should equal (List(Map("a" -> a, "b" -> b)))
  }

  test("should handle queries that cant be index solved because expressions lack dependencies with two disjoin patterns") {
    // Given
    val a = createLabeledNode(Map("property" -> 42), "Label")
    val b = createLabeledNode(Map("property" -> 42), "Label")
    val e = createLabeledNode(Map("property" -> 1), "Label")
    graph.createIndex("Label", "property")

    // when
    val result = execute("match (a:Label), (b:Label) where a.property = b.property return *")

    // then does not throw exceptions
    assert(result.toSet === Set(
      Map("a" -> a, "b" -> a),
      Map("a" -> a, "b" -> b),
      Map("a" -> b, "b" -> b),
      Map("a" -> b, "b" -> a),
      Map("a" -> e, "b" -> e)
    ))
  }
}
