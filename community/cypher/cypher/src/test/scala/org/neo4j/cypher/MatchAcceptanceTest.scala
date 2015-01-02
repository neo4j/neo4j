/**
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

import org.neo4j.cypher.internal.PathImpl
import org.neo4j.graphdb._

class MatchAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("should be able to use multiple MATCH clauses to do a cartesian product") {
    createNode("value" -> 1)
    createNode("value" -> 2)
    createNode("value" -> 3)

    val result = executeWithNewPlanner("MATCH n, m RETURN n.value AS n, m.value AS m")
    result.map(row => row("n") -> row("m")).toSet should equal(
      Set(1 -> 1, 1 -> 2, 1 -> 3, 2 -> 1, 2 -> 2, 2 -> 3, 3 -> 1, 3 -> 2, 3 -> 3)
    )
  }

  test("should be able to use params in pattern matching predicates") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2, "A", Map("foo" -> "bar"))

    val result = executeWithNewPlanner("match a-[r]->b where r.foo =~ {param} return b", "param" -> "bar")

    result.columnAs("b").toList should equal (List(n2))
  }

  test("should filter out based on node prop name") {
    val name = "Andres"
    val start: Node = createNode()
    val a1: Node = createNode(Map("name" -> "Someone Else"))
    val a2: Node = createNode(Map("name" -> name))
    relate(start, a1, "x")
    relate(start, a2, "x")

    val result = executeWithNewPlanner(
      s"MATCH (start)-[rel:x]-(a) WHERE a.name = '$name' return a"
    )
    result.columnAs[Node]("a").toList should equal (List(a2))
  }

  test("should honour the column name for RETURN items") {
    val start: Node = createNode(Map("name" -> "Someone Else"))
    val result = executeWithNewPlanner(s"MATCH a WITH a.name AS a RETURN a")
    result.columnAs[String]("a").toList should equal (List("Someone Else"))
  }

  test("should filter based on rel prop name") {
    val start: Node = createNode()
    val a: Node = createNode()
    val b: Node = createNode()
    relate(start, a, "KNOWS", Map("name" -> "monkey"))
    relate(start, b, "KNOWS", Map("name" -> "woot"))

    val result = executeWithNewPlanner(
      s"match (node)-[r:KNOWS]->(a) WHERE r.name = 'monkey' RETURN a"
    )
    result.columnAs[Node]("a").toList should equal(List(a))
  }

  test("should cope with shadowed variables") {
    val node = createNode("name" -> "Foo")
    val result = executeWithNewPlanner(s"MATCH n WITH n.name AS n RETURN n")
    result.columnAs[String]("n").toList should equal(List("Foo"))
  }

  test("should get neighbours") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val result = executeWithNewPlanner(
      s"match (n1)-[rel:KNOWS]->(n2) RETURN n1, n2"
    )

    result.toSet should equal(Set(Map("n1" -> n1, "n2" -> n2)))
  }

  test("should get two related nodes") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val result = executeWithNewPlanner(
      s"match (start)-[rel:KNOWS]->(x) return x"
    )

    result.toSet should equal(Set(Map("x" -> n2), Map("x" -> n3)))
  }

  test("should get related to related to") {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val result = executeWithNewPlanner("match n-->a-->b RETURN b").toList

    result.toSet should equal(Set(Map("b" -> n3)))
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

    val result = executeWithNewPlanner(
      """match (n)-[rel]->(x)
        where n.animal = x.animal
        return n, x"""
    )

    result.toSet should equal(Set(
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

    val result = executeWithNewPlanner(
      s"match n where n.x < 100 return n"
    )

    result.columnAs[Node]("n").toList should equal(List(n1, n2, n3, n4, n5))
  }

  test("comparing string and chars should work nicely") {
    val n1 = createNode(Map("x" -> "Anders"))
    val n2 = createNode(Map("x" -> 'C'))
    createNode(Map("x" -> "Zzing"))
    createNode(Map("x" -> 'Ä'))

    val result = executeWithNewPlanner(
      s"match n where n.x < 'Z' AND n.x < 'z' return n"
    )

    result.columnAs("n").toList should equal(List(n1, n2))
  }

  test("should return two subgraphs with bound undirected relationship") {
    val a = createNode("a")
    val b = createNode("b")
    relate(a, b, "rel", "r")

    val result = executeWithNewPlanner("match a-[r {name: 'r'}]-b return a,b")

    result.toSet should equal(Set(Map("a" -> a, "b" -> b), Map("a" -> b, "b" -> a)))
  }

  test("should return two subgraphs with bound undirected relationship and optional relationship") {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "rel", "r")
    relate(b, c, "rel", "r2")

    val result = executeWithNewPlanner("match (a)-[r {name:'r'}]-(b) optional match (b)-[r2]-(c) where r<>r2 return a,b,c")
    result.toSet should equal(Set(Map("a" -> a, "b" -> b, "c" -> c), Map("a" -> b, "b" -> a, "c" -> null)))
  }

  test("magic rel type works as expected") {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = executeWithNewPlanner(
      "match (n {name:'A'})-[r]->(x) where type(r) = 'KNOWS' return x"
    )

    result.columnAs[Node]("x").toList should equal(List(node("B")))
  }

  test("should walk alternative relationships") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("A" -> "HATES" -> "C")
    relate("A" -> "WONDERS" -> "C")

    val result = executeWithNewPlanner(
      "match (n)-[r]->(x) where type(r) = 'KNOWS' OR type(r) = 'HATES' return r"
    )

    result.columnAs("r").toList should equal(List(r1, r2))
  }

  test("should handle OR in the WHERE clause") {
    val a = createNode(Map("p1" -> 12))
    val b = createNode(Map("p2" -> 13))
    createNode()

    val result = executeWithNewPlanner("match (n) where n.p1 = 12 or n.p2 = 13 return n")

    result.columnAs("n").toList should equal(List(a, b))
  }

  test("should return a simple path") {
    createNodes("A", "B")
    val r = relate("A" -> "KNOWS" -> "B")

    val result = executeWithNewPlanner("match p=(a {name:'A'})-->b return p")

    result.columnAs[Path]("p").toList should equal(List(PathImpl(node("A"), r, node("B"))))
  }

  test("should return a three node path") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = executeWithNewPlanner("match p = (a {name:'A'})-[rel1]->b-[rel2]->c return p")

    result.columnAs("p").toList should equal(List(PathImpl(node("A"), r1, node("B"), r2, node("C"))))
  }

  test("should not return anything because path length does not match") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = executeWithNewPlanner("match p = n-->x where length(p) = 10 return x")

    result shouldBe 'isEmpty
  }

  test("should pass the path length test") {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = executeWithNewPlanner("match p = n-->x where length(p)=1 return x")

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

  test("should return relationships by fetching them from the path - starting from the end") {
    val a = createNode()
    val b = createNode()
    val c = createLabeledNode("End")

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("match p = a-[:rel*2..2]->(b:End) return RELATIONSHIPS(p)")

    result.columnAs[Node]("RELATIONSHIPS(p)").toList.head should equal(List(r1, r2))
  }

  test("should return relationships by fetching them from the path") {
    val a = createLabeledNode("Start")
    val b = createNode()
    val c = createNode()

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("match p = (a:Start)-[:rel*2..2]->b return RELATIONSHIPS(p)")

    result.columnAs[Node]("RELATIONSHIPS(p)").toList.head should equal(List(r1, r2))
  }

  test("should return relationships by collecting them as a list - wrong way") {
    val a = createNode()
    val b = createNode()
    val c = createLabeledNode("End")

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("match a-[r:rel*2..2]->(b:End) return r")

    result.columnAs[List[Relationship]]("r").toList.head should equal(List(r1, r2))
  }

  test("should return relationships by collecting them as a list - undirected") {
    val a = createLabeledNode("End")
    val b = createNode()
    val c = createLabeledNode("End")

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("match a-[r:rel*2..2]-(b:End) return r")

    val relationships = result.columnAs[List[Relationship]]("r").toSet
    relationships should equal(Set(List(r1, r2), List(r2, r1)))
  }

  test("should return relationships by collecting them as a list") {
    val a = createLabeledNode("Start")
    val b = createNode()
    val c = createNode()

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("match (a:Start)-[r:rel*2..2]->b return r")

    result.columnAs[List[Relationship]]("r").toList.head should equal(List(r1, r2))
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

  test("simple var length acceptance test") {
    createNodes("A", "B", "C", "D")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "CONTAINS" -> "C")
    relate("C" -> "CONTAINS" -> "D")


    val result = execute("match (a {name:'A'})-[*]->x return x")

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

    val result = execute("match p=(n {name:'A'})-[:KNOWS*..2]->x return p")

    result.columnAs[Path]("p").toList should equal(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ))
  }

  test("should return a var length path with unbound max") {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("match p=(n {name:'A'})-[:KNOWS*..]->x return p")

    result.columnAs[Path]("p").toList should equal(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ))
  }


  test("should handle bound nodes not part of the pattern") {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")

    val result = executeWithNewPlanner("MATCH (a {name:'A'}),(c {name:'C'}) match a-->b return a,b,c").toSet

    result should equal (Set(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))))
  }

  test("should return shortest path") {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val result = executeWithNewPlanner("match p = shortestPath((a {name:'A'})-[*..15]-(b {name:'B'})) return p").
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
    executeWithNewPlanner("match p = shortestPath((a {name:'A'})-[*]-(b {name:'B'})) return p").toList
  }

  test("should not traverse same relationship twice in shortest path") {
    // given
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    // when
    val result = executeWithNewPlanner("MATCH (a{name:'A'}), (b{name:'B'}) MATCH p=allShortestPaths((a)-[:KNOWS|KNOWS*]->(b)) RETURN p").
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

    val result = executeWithNewPlanner( """
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

    val result = execute( """
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

    val result = execute( """
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

    val result = executeWithNewPlanner( """
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

    val result = executeWithNewPlanner( """
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

    val result = executeWithNewPlanner( """
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

    val result = execute( """
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

    val result = execute( """
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

    val result = execute( """
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

    val result = execute( """
match (a) where id(a) = 0
match a-[r*2]->c
return r""")

    result.toList should equal (List(Map("r" -> List(r1, r2))))
  }

  test("should handle all shortest paths") {
    createDiamond()

    val result = executeWithNewPlanner( """
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

    val result = execute( """
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

    val result = executeWithNewPlanner("""
MATCH (a {name:'A'}), (other {name:'B'})
WHERE NOT a-->other
RETURN other""")

    result.toList should equal (List(Map("other" -> c)))
  }

  test("should not throw exception when stuff is missing") {
    val a = createNode()
    val b = createNode("Mark")
    relate(a, b)
    val result = executeWithNewPlanner( """
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
    val result = executeWithNewPlanner( s"""
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
    val result = executeWithNewPlanner( "match (n) -[:Admin]- (b) where id(n) = 0 return id(n), id(b)")
    result.toSet should equal (Set(Map("id(n)" -> 0, "id(b)" -> 1)))
  }


  test("should get all nodes") {
    val a = createNode()
    val b = createNode()

    val result = executeWithNewPlanner("match n return n")
    result.columnAs[Node]("n").toSet should equal (Set(a, b))
  }

  test("should allow comparisons of nodes") {
    val a = createNode()
    val b = createNode()

    val result = executeWithNewPlanner("MATCH a, b where a <> b return a,b")
    result.toSet should equal (Set(Map("a" -> b, "b" -> a), Map("b" -> b, "a" -> a)))
  }

  //TODO cyclic patterns has been disabled in ronja
  test("should solve selfreferencing pattern") {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = execute("match a-->b, b-->b return b")

    result shouldBe 'isEmpty
  }

  //TODO cyclic patterns has been disabled in ronja
  test("should solve self referencing pattern2") {
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

    val result = executeWithNewPlanner("match a where a-[:A|:B]->() return a").toSet

    result should equal (Set(Map("a" -> a), Map("a" -> b)))
  }

  test("relationship predicate") {
    val a = createNode()
    val x = createNode()

    relate(a, x, "A")

    val result = executeWithNewPlanner("match a where a-[:A]->() return a").toList

    result should equal (List(Map("a" -> a)))
  }

  test("nullable var length path should work") {
    createNode()
    val b = createNode()

    val result = execute("""
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

    val result = executeWithNewPlanner("match (a)-[:REL|:REL]->(b) return b").toList

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

    val resultWithoutLimit = executeWithNewPlanner(q)
    val resultWithLimit = executeWithNewPlanner(q + " limit 1000")

    resultWithoutLimit.toList should equal (resultWithLimit.toList)
  }

  test("should be able to handle single node patterns") {
    // given
    val n = createNode("foo" -> "bar")

    // when
    val result = executeWithNewPlanner("match n where n.foo = 'bar' return n")

    // then
    result.toList should equal (List(Map("n" -> n)))
  }

  test("issue 479") {
    createNode()

    val q = "match (n) where id(n) = 0 optional match (n)-->(x) where x-->() return x"

    executeWithNewPlanner(q).toList should equal (List(Map("x" -> null)))
  }

  test("issue 479 has relationship to specific node") {
    createNode()

    val q = "match (n) where id(n) = 0 optional match (n)-[:FRIEND]->(x) where not n-[:BLOCK]->x return x"

    executeWithNewPlanner(q).toList should equal (List(Map("x" -> null)))
  }

  test("length on filter") {
    val q = "match (n) optional match (n)-[r]->(m) return length(filter(x in collect(r) WHERE x <> null)) as cn"

    executeWithNewPlanner(q).toList should equal (List(Map("cn" -> 0)))
  }

  test("path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = executeWithNewPlanner("match p=b<--a return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("shortest Path Direction Respected") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = executeWithNewPlanner("match (a), (b) where id(a) = 0 and id(b) = 1 match p=shortestPath(b<-[*]-a) return p").toList.head("p").asInstanceOf[Path]

    result.startNode() should equal (b)
    result.endNode() should equal (a)
  }

  test("no match in optional match should produce null values") {
    val result = executeWithNewPlanner("OPTIONAL MATCH n RETURN n")

    result.toList should equal (List(Map("n" ->  null)))
  }

  test("should preserve the original matched values if optional match matches nothing") {
    val n = createNode()
    val result = executeWithNewPlanner("MATCH n OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n, x")

    result.toList should equal (List(Map("n" -> n, "x" -> null)))
  }

  test("empty collect should not contain null") {
    val n = createNode()
    val result = executeWithNewPlanner("MATCH n OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n, collect(x)")

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

    val result = executeWithNewPlanner("MATCH a-->r-->b WHERE id(a) = 0 AND has(r.foo) RETURN r")

    result.toList should equal (List(Map("r" -> r1)))
  }

  test("can handle paths with multiple unnamed nodes") {
    createNode()
    val result = executeWithNewPlanner("MATCH a<--()<--b-->()-->c WHERE id(a) = 0 RETURN c")

    result shouldBe 'isEmpty
  }

  test("path expressions should work with on the fly predicates") {
    val refNode = createNode()
    relate(refNode, createNode("name" -> "Neo"))
    val result = executeWithNewPlanner("MATCH a-->b WHERE b-->() AND id(a) = {self} RETURN b", "self" -> refNode.getId)

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
    val result = executeWithNewPlanner(s"MATCH a-->(b:foo) RETURN b")

    // THEN
    result.toList should equal (List(Map("b" -> b1)))
  }

  test("should use predicates in the correct place") {
    // given
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

    // when
    val result = executeWithNewPlanner(
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
    val result = executeWithNewPlanner("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name = 'Jacob' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should be able to use label as start point") {
    // given
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    // when
    val result = executeWithNewPlanner("MATCH (n:Person)-->() WHERE n.name = 'Jacob' RETURN n")

    // then
    result.toList should equal (List(Map("n" -> jake)))
  }

  test("should not see updates created by itself") {
    createNode()

    val result = execute("match n create ()")
    assertStats(result, nodesCreated = 1)
  }

  test("id in where leads to empty result") {
    // when
    val result = executeWithNewPlanner("MATCH n WHERE id(n)=1337 RETURN n")

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
    val result = executeWithNewPlanner("match a-->b match c-->d return a,b,c,d")

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
    executeWithNewPlanner("match (n) where id(n) in [0,1] return n").toList
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
    val result = executeWithNewPlanner("match (a:Label)-->(b:Label) where a.property = b.property return a, b")

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
    val result = executeWithNewPlanner("match (a:Label), (b:Label) where a.property = b.property return *")

    // then does not throw exceptions
    assert(result.toSet === Set(
      Map("a" -> a, "b" -> a),
      Map("a" -> a, "b" -> b),
      Map("a" -> b, "b" -> b),
      Map("a" -> b, "b" -> a),
      Map("a" -> e, "b" -> e)
    ))
  }

  //TODO cyclic patterns has been disabled in ronja
  test("should handle cyclic patterns") {
    // given
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "A")
    relate(b, a, "B")
    relate(b, c, "B")

    // when
    val result = execute("match (a)-[r1:A]->(x)-[r2:B]->(a) return a.name")

    // then does not throw exceptions
    assert(result.toList === List(
      Map("a.name" -> "a")
    ))
  }

  //TODO cyclic patterns has been disabled in ronja
  test("should handle cyclic patterns (broken up into two paths)") {
    // given
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "A")
    relate(b, a, "B")
    relate(b, c, "B")

    // when
    val result = execute("match (a)-[:A]->(b), (b)-[:B]->(a) return a.name")

    // then does not throw exceptions
    assert(result.toList === List(
      Map("a.name" -> "a")
    ))
  }

  test("should match fixed-size var length pattern") {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val result = execute("match (a)-[r*1..1]->(b) return r")
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
    val result = executeWithNewPlanner("optional match (a) with a match (a)-->(b) return b")

    // should give us a number in the middle, not all or nothing
    result.toList should be(empty)
  }

  test("should not find node in the match if there is a filter on the optional match") {
    // given
    val a = createNode()
    val b = createNode()
    relate(a, b)

    // when
    val result = executeWithNewPlanner("optional match (a:Person) with a match (a)-->(b) return b").columnAs[Node]("b")

    // should give us a number in the middle, not all or nothing
    result.toList should be(empty)
  }

  test("optional match starting from a null node returns null") {
    // given empty db

    // when
    val result = executeWithNewPlanner("optional match (a) with a optional match (a)-->(b) return b")

    // should give us a number in the middle, not all or nothing
    result.toList should equal (List(Map("b"->null)))
  }

  test("match p = (a) return p") {
    // given a single node
    val node = createNode()

    // when
    val result = executeWithNewPlanner("match p = (a) return p")

    // should give us a number in the middle, not all or nothing
    result.toList should equal (List(Map("p"->new PathImpl(node))))
  }

  test("match p = (a)-[r*0..]->(b) return p") {
    // given a single node
    val node = createNode()

    // when
    val result = execute("match p = (a)-[r*0..]->(b) return p")

    // should give us a single, empty path starting at one end
    result.toList should equal (List(Map("p"-> new PathImpl(node))))
  }

  test("MATCH n RETURN n.prop AS m, count(n) AS count") {
    // given a single node
    createNode("prop" -> "42")

    // when
    val result = executeWithNewPlanner("MATCH n RETURN n.prop AS n, count(n) AS count")

    // should give us a single, empty path starting at one end
    result.toList should equal (List(Map("n" -> "42", "count" -> 1)))
  }

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    // given two disconnected rels
    val rel1 = relate(createNode(), createNode())
    val rel2 = relate(createNode(), createNode())

    // when
    val result = executeWithNewPlanner("MATCH (u)-[r1]->(v) WITH r1 AS r2 MATCH (a)-[r2]->(b) RETURN r2 AS rel")

    // should give us all rels
    val actual = relsById(result.columnAs[Relationship]("rel").toList)
    val expected = relsById(Seq(rel1, rel2))

    result.columns should equal(List("rel"))
    actual should equal(expected)
  }

  test("MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel") {
    // given two disconnected rels
    val rel1 = relate(createNode(), createNode())
    val rel2 = relate(createNode(), createNode())

    // when
    val result = executeWithNewPlanner("MATCH (u)-[r1]->(v) WITH r1 AS r2, rand() AS c ORDER BY c MATCH (a)-[r2]->(b) RETURN r2 AS rel")

    // should give us all rels
    val actual = relsById(result.columnAs[Relationship]("rel").toList)
    val expected = relsById(Seq(rel1, rel2))

    result.columns should equal(List("rel"))
    actual should equal(expected)
  }

  test("MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel") {
    // given two disconnected rels
    val rel1 = relate(createNode(), createNode())
    val rel2 = relate(createNode(), createNode())

    // when
    val result = executeWithNewPlanner("MATCH (a)-[r]->(b) WITH a, r, b, rand() AS c ORDER BY c MATCH (a)-[r]->(b) RETURN r AS rel")

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
    val result = executeWithNewPlanner("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a2" -> node1, "r" -> relationship, "b2" -> node2)))
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)-[r]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithNewPlanner("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)-[r]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a1" -> node1, "r" -> relationship, "b2" -> node2)))
  }

  test("MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:X)-[r]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2)

    // when
    val result = executeWithNewPlanner("MATCH (a1)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:X)-[r]->(b2) RETURN a1, r, b2")

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
    val result = executeWithNewPlanner("MATCH (a1:X:Y)-[r]->() WITH r, a1 LIMIT 1 MATCH (a1:Y)-[r]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual shouldNot be(empty)
  }

  test("MATCH (a1)-[r:X]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2, "X")

    // when
    val result = executeWithNewPlanner("MATCH (a1)-[r:X]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should be(empty)
  }

  test("MATCH (a1)-[r:Y]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    relate(node1, node2, "Y")

    // when
    val result = executeWithNewPlanner("MATCH (a1)-[r:Y]->() WITH r, a1 LIMIT 1 MATCH (a1)-[r:Y]->(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual shouldNot be(empty)
  }

  //TODO this doesn't work in rule planner
  ignore("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second") {
    val node1 = createNode()
    val node2 = createNode()
    val node3 = createNode()
    relate(node1, node2, "Y")
    relate(node2, node3, "Y")

    // when
    val result = execute("MATCH ()-[r1]->()-[r2]->() WITH [r1, r2] AS rs LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")

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
    val result = execute("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS first, b AS second LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")

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
    val result = execute("MATCH (a)-[r1]->()-[r2]->(b) WITH [r1, r2] AS rs, a AS second, b AS first LIMIT 1 MATCH (first)-[rs*]->(second) RETURN first, second")

    val actual = result.toList

    actual should be(empty)
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithNewPlanner("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a1" -> node1, "r" -> relationship, "b2" -> null)))
  }

  test("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2") {
    val node1 = createNode()
    val node2 = createNode()
    val relationship = relate(node1, node2)

    // when
    val result = executeWithNewPlanner("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2, a2")

    // should give us all rels
    val actual = result.toList

    actual should equal(List(Map("a1" -> node1, "r" -> relationship, "b2" -> null, "a2" -> null)))
  }

  test("MATCH n WITH n.prop AS n2 RETURN n2.prop") {
    // Given a single node
    createNode("prop" -> "42")

    // then
    intercept[CypherTypeException](executeWithNewPlanner("MATCH n WITH n.prop AS n2 RETURN n2.prop"))
  }

  test("MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4") {
    createNode("bar" -> 1)
    createNode("bar" -> 3)
    createNode("bar" -> 2)

    // when
    val result = executeWithNewPlanner("MATCH foo RETURN foo.bar AS x ORDER BY x DESC LIMIT 4")
    result.toList should equal(List(
      Map("x" -> 3),
      Map("x" -> 2),
      Map("x" -> 1)
    ))
  }

  test("MATCH a RETURN count(a) > 0") {
    val result = executeWithNewPlanner("MATCH a RETURN count(a) > 0")
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

    val result = execute("MATCH (a:Artist)-[:WORKED_WITH* { year: 1988 }]->(b:Artist) RETURN *")
    result.toList should equal(List(
      Map("a" -> b, "b" -> c)
    ))
  }

  private def relsById(in: Seq[Relationship]): Seq[Relationship] = in.sortBy(_.getId)

  test("should return shortest paths when only one side is bound") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val r1 = relate(a, b)

    val result = executeWithNewPlanner("match (a:A) match p = shortestPath( a-[*]->(b:B) ) return p").toList.head("p").asInstanceOf[Path]

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

    executeWithNewPlanner(query).toList
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
    val result = executeWithNewPlanner(query)

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
    val result = executeWithNewPlanner(query)

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
    val check = executeWithNewPlanner("MATCH (f:Folder) RETURN f.name").toSet

    //THEN
    first should equal(second)
    check should equal(Set(Map("f.name" -> "Dir1"), Map("f.name" -> "Dir2")))
  }
}
