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

import org.hamcrest.CoreMatchers._
import org.neo4j.graphdb._
import org.neo4j.kernel.TopLevelTransaction
import org.neo4j.test.ImpermanentGraphDatabase
import org.junit.Assert._
import scala.collection.JavaConverters._
import org.junit.{Ignore, Test}
import util.Random
import java.util.concurrent.TimeUnit
import org.neo4j.cypher.internal.PathImpl
import org.neo4j.graphdb.factory.{GraphDatabaseSettings, GraphDatabaseFactory}
import org.scalautils.LegacyTripleEquals

class ExecutionEngineTest extends ExecutionEngineHelper with StatisticsChecker with LegacyTripleEquals {

  @Ignore
  @Test def assignToPathInsideForeachShouldWork() {
    execute(
"""start n=node(0)
foreach(x in [1,2,3] |
  create p = ({foo:x})-[:X]->()
  foreach( i in p |
    set i.touched = true))""")
  }

  @Test def shouldGetRelationshipById() {
    val n = createNode()
    val r = relate(n, createNode(), "KNOWS")

    val result = execute("start r=rel(0) return r")
    assertEquals(List(r), result.columnAs[Relationship]("r").toList)
  }

  @Test def shouldFilterOnGreaterThan() {
    val n = createNode()
    val result = execute("start node=node(0) where 0<1 return node")

    assertEquals(List(n), result.columnAs[Node]("node").toList)
  }

  @Test def shouldFilterOnRegexp() {
    val n1 = createNode(Map("name" -> "Andres"))
    val n2 = createNode(Map("name" -> "Jim"))

    val result = execute(
      s"start node=node(${n1.getId}, ${n2.getId}) where node.name =~ 'And.*' return node"
    )
    assertEquals(List(n1), result.columnAs[Node]("node").toList)
  }

  @Test def shouldBeAbleToUseParamsInPatternMatchingPredicates() {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2, "A", Map("foo" -> "bar"))

    val result = execute("start a=node(0) match a-[r]->b where r.foo =~ {param} return b", "param" -> "bar")

    assertEquals(List(n2), result.columnAs[Node]("b").toList)
  }

  @Test def shouldGetOtherNode() {
    val node: Node = createNode()

    val result = execute(s"start node=node(${node.getId}) return node")
    assertEquals(List(node), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetRelationship() {
    val node: Node = createNode()
    val rel: Relationship = relate(createNode(), node, "yo")

    val result = execute(s"start rel=rel(${rel.getId}) return rel")
    assertEquals(List(rel), result.columnAs[Relationship]("rel").toList)
  }

  @Test def shouldGetTwoNodes() {
    val node1: Node = createNode()
    val node2: Node = createNode()

    val result = execute(s"start node=node(${node1.getId}, ${node2.getId}) return node")
    assertEquals(List(node1, node2), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetNodeProperty() {
    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val result = execute(s"start node=node(${node.getId}) return node.name")
    val list = result.columnAs[String]("node.name").toList
    assertEquals(List(name), list)
  }

  @Test def shouldFilterOutBasedOnNodePropName() {
    val name = "Andres"
    val start: Node = createNode()
    val a1: Node = createNode(Map("name" -> "Someone Else"))
    val a2: Node = createNode(Map("name" -> name))
    relate(start, a1, "x")
    relate(start, a2, "x")

    val result = execute(
      s"start start=node(${start.getId}) MATCH (start)-[rel:x]-(a) WHERE a.name = '$name' return a"
    )
    assertEquals(List(a2), result.columnAs[Node]("a").toList)
  }

  @Test def shouldFilterBasedOnRelPropName() {
    val start: Node = createNode()
    val a: Node = createNode()
    val b: Node = createNode()
    relate(start, a, "KNOWS", Map("name" -> "monkey"))
    relate(start, b, "KNOWS", Map("name" -> "woot"))

    val result = execute(
      s"start node=node(${start.getId}) match (node)-[r:KNOWS]-(a) WHERE r.name = 'monkey' RETURN a"
    )
    assertEquals(List(a), result.columnAs[Node]("a").toList)
  }

  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()

    val result = execute(
      s"start n1=node(${n1.getId}), n2=node(${n2.getId}) return n1, n2"
    )

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetNeighbours() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val result = execute(
      s"start n1=node(${n1.getId}) match (n1)-[rel:KNOWS]->(n2) RETURN n1, n2"
    )

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetTwoRelatedNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val result = execute(
      s"start n=node(${n1.getId}) match (start)-[rel:KNOWS]->(x) return x"
    )

    assertEquals(List(Map("x" -> n2), Map("x" -> n3)), result.toList)
  }

  @Test def executionResultTextualOutput() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val result = execute(
      s"start node=node(${n1.getId}) match (node)-[rel:KNOWS]->(x) return x, node"
    )
    result.dumpToString()
  }

  @Test def shouldGetRelatedToRelatedTo() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val result = execute("start n = node(0) match n-->a-->b RETURN b").toList

    assertEquals(List(Map("b" -> n3)), result.toList)
  }

  @Test def shouldFindNodesByExactIndexLookup() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName($key = '$value') return n"

    assertInTx(List(Map("n" -> n)) === execute(query).toList)
  }

  @Test def shouldFindNodesByIndexQuery() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName('$key: $value') return n"

    assertInTx(List(Map("n" -> n)) === execute(query).toList)
  }

  @Test def shouldFindNodesByIndexParameters() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    indexNode(n, idxName, key, "Andres")

    val query = s"start n=node:$idxName(key = {value}) return n"

    assertInTx(List(Map("n" -> n)) === execute(query, "value" -> "Andres").toList)
  }

  @Test def shouldFindNodesByIndexWildcardQuery() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = s"start n=node:$idxName('$key:andr*') return n"

    assertInTx(List(Map("n" -> n)) === execute(query).toList)
  }

  @Test def shouldHandleOrFilters() {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}) where n.name = 'boy' OR n.name = 'girl' return n"
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldHandleXorFilters() {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}) where n.name = 'boy' XOR n.name = 'girl' return n"
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldHandleNestedAndOrFilters() {
    val n1 = createNode(Map("animal" -> "monkey", "food" -> "banana"))
    val n2 = createNode(Map("animal" -> "cow", "food" -> "grass"))
    val n3 = createNode(Map("animal" -> "cow", "food" -> "banana"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}) " +
        """where
        (n.animal = 'monkey' AND n.food = 'banana') OR
        (n.animal = 'cow' AND n.food = 'grass')
        return n
        """
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldBeAbleToOutputNullForMissingProperties() {
    createNode()
    val result = execute("start n=node(0) return n.name")
    assertEquals(List(Map("n.name" -> null)), result.toList)
  }

  @Test def shouldHandleComparisonBetweenNodeProperties() {
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
      s"start n=node(${n1.getId}, ${n4.getId})" +
        """match (n)-[rel]->(x)
        where n.animal = x.animal
        return n, x
        """
    )

    assertEquals(List(
      Map("n" -> n1, "x" -> n3),
      Map("n" -> n4, "x" -> n2)), result.toList)

    assertEquals(List("n", "x"), result.columns)
  }

  @Test def comparingNumbersShouldWorkNicely() {
    val n1 = createNode(Map("x" -> 50))
    val n2 = createNode(Map("x" -> 50l))
    val n3 = createNode(Map("x" -> 50f))
    val n4 = createNode(Map("x" -> 50d))
    val n5 = createNode(Map("x" -> 50.toByte))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}, ${n4.getId}, ${n5.getId}) where n.x < 100 return n"
    )

    assertEquals(List(n1, n2, n3, n4, n5), result.columnAs[Node]("n").toList)
  }

  @Test def comparingStringAndCharsShouldWorkNicely() {
    val n1 = createNode(Map("x" -> "Anders"))
    val n2 = createNode(Map("x" -> 'C'))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}) where n.x < 'Z' AND n.x < 'z' return n"
    )

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldBeAbleToCountNodes() {
    val a = createNode()
    val b1 = createNode() //start a = (0) match (a) --> (b) return a, count(*)
    val b2 = createNode()
    relate(a, b1, "A")
    relate(a, b2, "A")

    val result = execute(
      s"start a=node(${a.getId}) match (a)-[rel]->(b) return a, count(*)"
    )

    assertEquals(List(Map("a" -> a, "count(*)" -> 2)), result.toList)
  }

  @Test def shouldReturnTwoSubgraphsWithBoundUndirectedRelationship() {
    val a = createNode("a")
    val b = createNode("b")
    relate(a, b, "rel", "r")

    val result = execute("start r=rel(0) match a-[r]-b return a,b")

    assertEquals(List(Map("a" -> a, "b" -> b), Map("a" -> b, "b" -> a)), result.toList)
  }

  @Test def shouldAcceptSkipZero() {
    val result = execute("start n=node(0) where 1 = 0 return n skip 0")

    assertEquals(List(), result.columnAs[Node]("n").toList)
  }

  @Test def shouldReturnTwoSubgraphsWithBoundUndirectedRelationshipAndOptionalRelationship() {
    val a = createNode("a")
    val b = createNode("b")
    val c = createNode("c")
    relate(a, b, "rel", "r")
    relate(b, c, "rel", "r2")

    val result = execute("start r=rel(0) match (a)-[r]-(b) optional match (b)-[r2]-(c) where r<>r2 return a,b,c")
    assertEquals(List(Map("a" -> a, "b" -> b, "c" -> c), Map("a" -> b, "b" -> a, "c" -> null)), result.toList)
  }

  @Test def shouldLimitToTwoHits() {
    createNodes("A", "B", "C", "D", "E")

    val result = execute(
      s"start n=node(${nodeIds.mkString(",")}) return n limit 2"
    )

    assertEquals("Result was not trimmed down", 2, result.size)
  }

  @Test def shouldStartTheResultFromSecondRow() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val result = execute(
      s"start n=node(${nodeIds.mkString(",")}) return n order by n.name ASC skip 2"
    )

    assertEquals(nodes.drop(2).toList, result.columnAs[Node]("n").toList)
  }

  @Test def shouldStartTheResultFromSecondRowByParam() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val query = s"start n=node(${nodeIds.mkString(",")}) return n order by n.name ASC skip {skippa}"
    val result = execute(query, "skippa" -> 2)

    assertEquals(nodes.drop(2).toList, result.columnAs[Node]("n").toList)
  }

  @Test def shouldGetStuffInTheMiddle() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val result = execute(
      s"start n=node(${nodeIds.mkString(",")}) return n order by n.name ASC skip 2 limit 2"
    )

    assertEquals(nodes.slice(2, 4).toList, result.columnAs[Node]("n").toList)
  }

  @Test def shouldGetStuffInTheMiddleByParam() {
    val nodes = createNodes("A", "B", "C", "D", "E")

    val query = s"start n=node(${nodeIds.mkString(",")}) return n order by n.name ASC skip {s} limit {l}"
    val result = execute(query, "l" -> 2, "s" -> 2)

    assertEquals(nodes.slice(2, 4).toList, result.columnAs[Node]("n").toList)
  }

  @Test def shouldSortOnAggregatedFunction() {
    createNode(Map("name" -> "andres", "division" -> "Sweden", "age" -> 33))
    createNode(Map("name" -> "michael", "division" -> "Germany", "age" -> 22))
    createNode(Map("name" -> "jim", "division" -> "England", "age" -> 55))
    createNode(Map("name" -> "anders", "division" -> "Sweden", "age" -> 35))

    val result = execute("start n=node(0,1,2,3) return n.division, max(n.age) order by max(n.age) ")

    assertEquals(List("Germany", "Sweden", "England"), result.columnAs[String]("n.division").toList)
  }

  @Test def shouldSortOnAggregatedFunctionAndNormalProperty() {
    val n1 = createNode(Map("name" -> "andres", "division" -> "Sweden"))
    val n2 = createNode(Map("name" -> "michael", "division" -> "Germany"))
    val n3 = createNode(Map("name" -> "jim", "division" -> "England"))
    val n4 = createNode(Map("name" -> "mattias", "division" -> "Sweden"))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}, ${n4.getId})" +
        """return n.division, count(*)
        order by count(*) DESC, n.division ASC
        """
    )

    assertEquals(List(
      Map("n.division" -> "Sweden", "count(*)" -> 2),
      Map("n.division" -> "England", "count(*)" -> 1),
      Map("n.division" -> "Germany", "count(*)" -> 1)), result.toList)
  }

  @Test def magicRelTypeWorksAsExpected() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = execute(
      "start n = node(0) match (n)-[r]->(x) where type(r) = 'KNOWS' return x"
    )

    assertEquals(List(node("B")), result.columnAs[Node]("x").toList)
  }

  @Test def magicRelTypeOutput() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = execute("start n = node(0) match n-[r]->x return type(r)")

    assertEquals(List("KNOWS", "HATES"), result.columnAs[String]("type(r)").toList)
  }

  @Test def shouldAggregateOnProperties() {
    val n1 = createNode(Map("x" -> 33))
    val n2 = createNode(Map("x" -> 33))
    val n3 = createNode(Map("x" -> 42))

    val result = execute(
      s"start n=node(${n1.getId}, ${n2.getId}, ${n3.getId}) return n.x, count(*)"
    )

    assertThat(result.toList.asJava, hasItems[Map[String, Any]](Map("n.x" -> 33, "count(*)" -> 2), Map("n.x" -> 42, "count(*)" -> 1)))
  }

  @Test def shouldCountNonNullValues() {
    createNode(Map("y" -> "a", "x" -> 33))
    createNode(Map("y" -> "a"))
    createNode(Map("y" -> "b", "x" -> 42))

    val result = execute("start n=node(0,1,2) return n.y, count(n.x)")

    assertThat(result.toList.asJava,
      hasItems[Map[String, Any]](
        Map("n.y" -> "a", "count(n.x)" -> 1),
        Map("n.y" -> "b", "count(n.x)" -> 1)))
  }

  @Test def shouldSumNonNullValues() {
    createNode(Map("y" -> "a", "x" -> 33))
    createNode(Map("y" -> "a"))
    createNode(Map("y" -> "a", "x" -> 42))

    val result = execute("start n = node(0,1,2) return n.y, sum(n.x)")

    assertThat(result.toList.asJava,
      hasItems[Map[String, Any]](Map("n.y" -> "a", "sum(n.x)" -> 75)))
  }

  @Test def shouldWalkAlternativeRelationships() {
    val nodes: List[Node] = createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = execute(
      "start n = node(0) match (n)-[r]->(x) where type(r) = 'KNOWS' OR type(r) = 'HATES' return x"
    )

    assertEquals(nodes.slice(1, 3), result.columnAs[Node]("x").toList)
  }

  @Test def shouldReturnASimplePath() {
    createNodes("A", "B")
    val r = relate("A" -> "KNOWS" -> "B")

    val result = execute("start a = node(0) match p=a-->b return p")

    assertEquals(List(PathImpl(node("A"), r, node("B"))), result.columnAs[Path]("p").toList)
  }

  @Test def shouldReturnAThreeNodePath() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start a = node(0) match p = a-[rel1]->b-[rel2]->c return p")

    assertEquals(List(PathImpl(node("A"), r1, node("B"), r2, node("C"))), result.columnAs[Path]("p").toList)
  }

  @Test def shouldWalkAlternativeRelationships2() {
    val nodes: List[Node] = createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")
    relate("A" -> "HATES" -> "C")

    val result = execute("start n = node(0) match (n)-[r]->(x) where type(r)='KNOWS' or type(r) = 'HATES' return x")

    assertEquals(nodes.slice(1, 3), result.columnAs[Node]("x").toList)
  }

  @Test def shouldNotReturnAnythingBecausePathLengthDoesntMatch() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("start n = node(0) match p = n-->x where length(p) = 10 return x")

    assertTrue("Result set should be empty, but it wasn't", result.isEmpty)
  }

  @Ignore
  @Test def statingAPathTwiceShouldNotBeAProblem() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("start n = node(0) match x<--n, p = n-->x return p")

    assertEquals(1, result.toSeq.length)
  }

  @Test def shouldPassThePathLengthTest() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("start n = node(0) match p = n-->x where length(p)=1 return x")

    assertTrue("Result set should not be empty, but it was", !result.isEmpty)
  }

  @Test def shouldReturnPathLength() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("start n = node(0) match p = n-->x return length(p)")

    assertEquals(List(1), result.columnAs[Int]("length(p)").toList)
  }

  @Test def shouldReturnCollectionSize() {
    val result = execute("return size([1,2,3]) as n")
    assertEquals(List(3), result.columnAs[Int]("n").toList)
  }

  @Test def shouldBeAbleToFilterOnPathNodes() {
    val a = createNode(Map("foo" -> "bar"))
    val b = createNode(Map("foo" -> "bar"))
    val c = createNode(Map("foo" -> "bar"))
    val d = createNode(Map("foo" -> "bar"))

    relate(a, b, "rel")
    relate(b, c, "rel")
    relate(c, d, "rel")

    val result = execute("start pA = node(0), pB=node(3) match p = pA-[:rel*1..5]->pB WHERE all(i in nodes(p) where i.foo = 'bar') return pB")

    assertEquals(List(d), result.columnAs[Node]("pB").toList)
  }

  @Test def shouldReturnRelationships() {
    val a = createNode(Map("foo" -> "bar"))
    val b = createNode(Map("foo" -> "bar"))
    val c = createNode(Map("foo" -> "bar"))

    val r1 = relate(a, b, "rel")
    val r2 = relate(b, c, "rel")

    val result = execute("start a = node(0) match p = a-[:rel*2..2]->b return RELATIONSHIPS(p)")

    assertEquals(List(r1, r2), result.columnAs[Node]("RELATIONSHIPS(p)").toList.head)
  }

  @Test def shouldReturnAVarLengthPath() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start n = node(0) match p=n-[:KNOWS*1..2]->x return p")

    val toList: List[Path] = result.columnAs[Path]("p").toList
    assertEquals(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ), toList)
  }

  @Test def aVarLengthPathOfLengthZero() {
    val a = createNode()
    val b = createNode()
    relate(a,b)

    val result = execute("start a=node(0) match p=a-[*0..1]->b return a,b, length(p) as l")

    assertEquals(
      Set(
        Map("a" -> a, "b" -> a, "l"->0),
        Map("a" -> a, "b" -> b, "l"->1)),
      result.toSet)
  }

  @Test def aNamedVarLengthPathOfLengthZero() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "FRIEND" -> "C")

    val result = execute("start a=node(0) match p=a-[:KNOWS*0..1]->b-[:FRIEND*0..1]->c return p,a,b,c")

    assertEquals(
      Set(
        PathImpl(node("A")),
        PathImpl(node("A"), r1, node("B")),
        PathImpl(node("A"), r1, node("B"), r2, node("C"))
      ),
      result.columnAs[Path]("p").toSet)
  }

  @Test def testZeroLengthVarLenPathInTheMiddle() {
    createNodes("A", "B", "C", "D", "E")
    relate("A" -> "CONTAINS" -> "B")
    relate("B" -> "FRIEND" -> "C")


    val result = execute("start a=node(0) match a-[:CONTAINS*0..1]->b-[:FRIEND*0..1]->c return a,b,c")

    assertEquals(
      Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B")),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))),
      result.toSet)
  }

  @Test def shouldReturnAVarLengthPathWithoutMinimalLength() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start n = node(0) match p=n-[:KNOWS*..2]->x return p")

    assertEquals(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ), result.columnAs[Path]("p").toList)
  }

  @Test def shouldReturnAVarLengthPathWithUnboundMax() {
    createNodes("A", "B", "C")
    val r1 = relate("A" -> "KNOWS" -> "B")
    val r2 = relate("B" -> "KNOWS" -> "C")

    val result = execute("start n = node(0) match p=n-[:KNOWS*..]->x return p")

    assertEquals(List(
      PathImpl(node("A"), r1, node("B")),
      PathImpl(node("A"), r1, node("B"), r2, node("C"))
    ), result.columnAs[Path]("p").toList)
  }


  @Test def shouldHandleBoundNodesNotPartOfThePattern() {
    createNodes("A", "B", "C")
    relate("A" -> "KNOWS" -> "B")

    val result = execute("start a=node(0), c = node(2) match a-->b return a,b,c").toList

    assert(List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("C"))) === result)
  }

  @Test def shouldReturnShortestPath() {
    createNodes("A", "B")
    val r1 = relate("A" -> "KNOWS" -> "B")

    val result = execute("start a = node(0), b = node(1) match p = shortestPath(a-[*..15]-b) return p").
      toList.head("p").asInstanceOf[Path]

    graph.inTx {
      val number_of_relationships_in_path = result.length()
      assert(number_of_relationships_in_path === 1)
      assert(result.startNode() === node("A"))
      assert(result.endNode() === node("B"))
      assert(result.lastRelationship() === r1)
    }
  }

  @Test def shouldReturnShortestPathUnboundLength() {
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    //Checking that we don't get an exception
    execute("start a = node(0), b = node(1) match p = shortestPath(a-[*]-b) return p").toList
  }

  @Test def shouldNotTraverseSameRelationshipTwiceInShortestPath() {
    // given
    createNodes("A", "B")
    relate("A" -> "KNOWS" -> "B")

    // when
    val result = execute("MATCH (a{name:'A'}), (b{name:'B'}) MATCH p=allShortestPaths((a)-[:KNOWS|KNOWS*]->(b)) RETURN p").
      toList

    // then
    graph.inTx {
      assert(result.size === 1, result)
    }
  }

  @Test def shouldBeAbleToTakeParamsInDifferentTypes() {
    createNodes("A", "B", "C", "D", "E")

    val query =
      """
        |start pA=node({a}), pB=node({b}), pC=node({c}), pD=node({0}), pE=node({1})
        |return pA, pB, pC, pD, pE
      """.stripMargin

    val result = execute(query,
      "a" -> Seq[Long](0),
      "b" -> 1,
      "c" -> Seq(2L).asJava,
      "0" -> Seq(3).asJava,
      "1" -> List(4)
    )

    assertEquals(1, result.toList.size)
  }

  @Test(expected = classOf[CypherTypeException]) def parameterTypeErrorShouldBeNicelyExplained() {
    createNodes("A")

    val query = "start pA=node({a}) return pA"
    execute(query, "a" -> "Andres").toList
  }

  @Test def shouldBeAbleToTakeParamsFromParsedStuff() {
    createNodes("A")

    val query = "start pA = node({a}) return pA"
    val result = execute(query, "a" -> Seq[Long](0))

    assertEquals(List(Map("pA" -> node("A"))), result.toList)
  }

  @Test def shouldBeAbleToTakeParamsForEqualityComparisons() {
    createNode(Map("name" -> "Andres"))

    val query = "start a=node(0) where a.name = {name} return a"

    assert(0 === execute(query, "name" -> "Tobias").toList.size)
    assert(1 === execute(query, "name" -> "Andres").toList.size)
  }

  @Test def shouldHandlePatternMatchingWithParameters() {
    val a = createNode()
    val b = createNode(Map("name" -> "you"))
    relate(a, b, "KNOW")

    val result = execute("start x  = node({startId}) match x-[r]-friend where friend.name = {name} return TYPE(r)", "startId" -> a, "name" -> "you")

    assert(List(Map("TYPE(r)" -> "KNOW")) === result.toList)
  }

  @Test def twoBoundNodesPointingToOne() {
    val a = createNode("A")
    val b = createNode("B")
    val x1 = createNode("x1")
    val x2 = createNode("x2")

    relate(a, x1, "REL", "AX1")
    relate(a, x2, "REL", "AX2")

    relate(b, x1, "REL", "BX1")
    relate(b, x2, "REL", "BX2")

    val result = execute( """
start a  = node({A}), b = node({B})
match a-[rA]->x<-[rB]->b
return x""", "A" -> a, "B" -> b)

    assert(List(x1, x2) === result.columnAs[Node]("x").toList)
  }


  @Test def threeBoundNodesPointingToOne() {
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
start a  = node({A}), b = node({B}), c = node({C})
match a-[rA]->x, b-[rB]->x, c-[rC]->x
return x""", "A" -> a, "B" -> b, "C" -> c)

    assert(List(x1, x2) === result.columnAs[Node]("x").toList)
  }

  @Test def threeBoundNodesPointingToOneWithABunchOfExtraConnections() {
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
start a  = node({A}), b = node({B}), c = node({C})
match a-->x, b-->x, c-->x
return x""", "A" -> a, "B" -> b, "C" -> c)

    assert(List(d, e) === result.columnAs[Node]("x").toList)
  }

  @Test def shouldSplitOptionalMandatoryCleverly() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")

    relate(a, b, "knows", "rAB")
    relate(b, c, "knows", "rBC")

    val result = execute( """
start a  = node(0)
optional match (a)-[r1:knows]->(friend)-[r2:knows]->(foaf)
return foaf""")

    assert(List(Map("foaf" -> c)) === result.toList)
  }

  @Test(expected = classOf[ParameterNotFoundException]) def shouldComplainWhenMissingParams() {
    execute("start pA=node({a}) return pA").toList
  }

  @Test def shouldSupportSortAndDistinct() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")

    val result = execute( """
start a  = node(0,1,2,0)
return distinct a
order by a.name""")

    assert(List(a, b, c) === result.columnAs[Node]("a").toList)
  }

  @Test def shouldHandleAggregationOnFunctions() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    relate(a, b, "X")
    relate(a, c, "X")

    val result = execute( """
start a  = node(0)
match p = a -[*]-> b
return b, avg(length(p))""")

    assert(Set(b, c) === result.columnAs[Node]("b").toSet)
  }

  @Test def shouldHandleOptionalPaths() {
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

  @Test def shouldHandleOptionalPathsFromGraphAlgo() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = execute( """
start a  = node(0), x = node(1,2)
optional match p = shortestPath(a -[*]-> x)
return x, p""").toList

    assertInTx(List(
      Map("x" -> b, "p" -> PathImpl(a, r, b)),
      Map("x" -> c, "p" -> null)
    ) === result)
  }

  @Test def shouldHandleOptionalPathsFromACombo() {
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

  @Test def shouldHandleOptionalPathsFromVarLengthPath() {
    val a = createNode("A")
    val b = createNode("B")
    val c = createNode("C")
    val r = relate(a, b, "X")

    val result = execute("""
start a = node(0), x = node(1,2)
optional match p = (a)-[r*]->(x)
return r, x, p""")

    assert(List(
      Map("r" -> Seq(r), "x" -> b, "p" -> PathImpl(a, r, b)),
      Map("r" -> null, "x" -> c, "p" -> null)
    ) === result.toList)
  }

  @Test def shouldSupportMultipleRegexes() {
    val a = createNode(Map("name" -> "Andreas"))

    val result = execute( """
start a  = node(0)
where a.name =~ 'And.*' AND a.name =~ 'And.*'
return a""")

    assert(List(a) === result.columnAs[Node]("a").toList)
  }

  @Test def shouldSupportColumnRenaming() {
    val a = createNode(Map("name" -> "Andreas"))

    val result = execute("start a = node(0) return a as OneLove")

    assert(List(a) === result.columnAs[Node]("OneLove").toList)
  }

  @Test def shouldSupportColumnRenamingForAggregatesAsWell() {
    createNode(Map("name" -> "Andreas"))

    val result = execute( """
start a  = node(0)
return count(*) as OneLove""")

    assert(List(1) === result.columnAs[Node]("OneLove").toList)
  }

  @Test(expected = classOf[SyntaxException]) def shouldNotSupportSortingOnThingsAfterDistinctHasRemovedIt() {
    createNode("name" -> "A", "age" -> 13)
    createNode("name" -> "B", "age" -> 12)
    createNode("name" -> "C", "age" -> 11)

    val result = execute( """
start a  = node(1,2,3,1)
return distinct a.name
order by a.age""")

    result.toList
  }

  @Test def shouldSupportOrderingByAPropertyAfterBeingDistinctified() {
    val a = createNode("name" -> "A")
    val b = createNode("name" -> "B")
    val c = createNode("name" -> "C")

    relate(a, b)
    relate(a, c)

    val result = execute( """
start a  = node(0)
match a-->b
return distinct b
order by b.name""")

    assert(List(b, c) === result.columnAs[Node]("b").toList)
  }

  @Test def shouldBeAbleToRunCoalesce() {
    createNode("name" -> "A")

    val result = execute( """
start a  = node(0)
return coalesce(a.title, a.name)""")

    assert(List(Map("coalesce(a.title, a.name)" -> "A")) === result.toList)
  }

  @Test def shouldReturnAnInterableWithAllRelationshipsFromAVarLength() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val r1 = relate(a, b)
    val r2 = relate(b, c)

    val result = execute( """
start a  = node(0)
match a-[r*2]->c
return r""")

    assert(List(Map("r" -> List(r1, r2))) === result.toList)
  }

  @Test def shouldHandleAllShortestPaths() {
    createDiamond()

    val result = execute( """
start a  = node(0), d = node(3)
match p = allShortestPaths( a-[*]->d )
return p""")

    assert(2 === result.toList.size)
  }

  @Test def shouldCollectLeafs() {
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

  @Ignore
  @Test def shouldCollectLeafsAndDoOtherMatchingAsWell() {
    val root = createNode()
    val leaf = createNode()
    val stuff1 = createNode()
    val stuff2 = createNode()
    relate(root, leaf)
    relate(leaf, stuff1)
    relate(leaf, stuff2)

    val result = execute( """
start root = node(0)
match allLeafPaths( root-->leaf ), leaf <-- stuff
return leaf, stuff
                                  """)

    assert(List(
      Map("leaf" -> leaf, "stuff" -> stuff1),
      Map("leaf" -> leaf, "stuff" -> stuff2)
    ) === result.toList)
  }

  @Test def shouldExcludeConnectedNodes() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)

    val result = execute( """
start a  = node(0), other = node(1,2)
where not(a-->other)
return other""")

    assert(List(Map("other" -> c)) === result.toList)
  }

  @Test def shouldHandleCheckingThatANodeDoesNotHaveAProp() {
    val a = createNode()

    val result = execute("start a=node(0) where not has(a.propertyDoesntExist) return a")
    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldHandleAggregationAndSortingOnSomeOverlappingColumns() {
    createNode("COL1" -> "A", "COL2" -> "A", "num" -> 1)
    createNode("COL1" -> "B", "COL2" -> "B", "num" -> 2)

    val result = execute( """
start a  = node(0,1)
return a.COL1, a.COL2, avg(a.num)
order by a.COL1""")

    assert(List(
      Map("a.COL1" -> "A", "a.COL2" -> "A", "avg(a.num)" -> 1),
      Map("a.COL1" -> "B", "a.COL2" -> "B", "avg(a.num)" -> 2)
    ) === result.toList)
  }

  @Test def shouldAllowAllPredicateOnArrayProperty() {
    val a = createNode("array" -> Array(1, 2, 3, 4))

    val result = execute("start a = node(0) where any(x in a.array where x = 2) return a")

    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldAllowStringComparisonsInArray() {
    val a = createNode("array" -> Array("Cypher duck", "Gremlin orange", "I like the snow"))

    val result = execute("start a = node(0) where single(x in a.array where x =~ '.*the.*') return a")

    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldBeAbleToCompareWithTrue() {
    val a = createNode("first" -> true)

    val result = execute("start a = node(0) where a.first = true return a")

    assert(List(Map("a" -> a)) === result.toList)
  }

  @Test def shouldNotThrowExceptionWhenStuffIsMissing() {
    val a = createNode()
    val b = createNode("Mark")
    relate(a, b)
    val result = execute("""START n = node(0)
MATCH n-->x0
OPTIONAL MATCH x0-->x1
WHERE x1.foo = 'bar'
RETURN x0.name""")
    assert(List(Map("x0.name"->"Mark")) === result.toList)
  }

  @Test def shouldFindNodesBothDirections() {
    val n = createNode()
    val a = createNode()
    relate(a, n, "Admin")
    val result = execute( """start n = node(0) match (n) -[:Admin]- (b) return id(n), id(b)""")
    assert(List(Map("id(n)" -> 0, "id(b)" -> 1)) === result.toList)
  }

  @Test def shouldToStringArraysPrettily() {
    createNode("foo" -> Array("one", "two"))

    val result = executeLazy( """start n = node(0) return n.foo""")


    val string = result.dumpToString()

    assertThat(string, containsString( """["one","two"]"""))
  }

  @Test def shouldAllowOrderingOnAggregateFunction() {
    createNode()

    val result = execute("start n = node(0) match (n)-[:KNOWS]-(c) return n, count(c) as cnt order by cnt")
    assert(List() === result.toList)
  }

  @Test def shouldNotAllowOrderingOnNodes() {
    createNode()
    createNode()

    intercept[SyntaxException](execute("start n = node(0,1) return n order by n").toList)
  }

  @Test def shouldIgnoreNodesInParameters() {
    val x = createNode()
    val a = createNode()
    relate(x, a, "X")

    val result = execute("start c = node(0) match (n)--(c) return n")
    assert(1 === result.size)
  }

  @Test def shouldReturnDifferentResultsWithDifferentParams() {
    val refNode = createNode()
    val a = createNode()

    val b = createNode()
    relate(a, b)

    relate(refNode, a, "X")

    assert(1 === execute("start a = node({a}) match a-->b return b", "a" -> a).size)
    assert(0 === execute("start a = node({a}) match a-->b return b", "a" -> b).size)
  }

  @Test def shouldHandleParametersNamedAsIdentifiers() {
    createNode("bar" -> "Andres")

    val result = execute("start foo=node(0) where foo.bar = {foo} return foo.bar", "foo" -> "Andres")
    assert(List(Map("foo.bar" -> "Andres")) === result.toList)
  }

  @Test def shouldHandleRelationshipIndexQuery() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)
    indexRel(r, "relIdx", "key", "value")

    assertInTx(List(Map("r" -> r)) === execute("start r=relationship:relIdx(key='value') return r").toList)
  }

  @Test def shouldHandleComparisonsWithDifferentTypes() {
    createNode("belt" -> 13)

    val result = execute("start n = node(0) where n.belt = 'white' OR n.belt = false return n")
    assert(List() === result.toList)
  }

  @Test def shouldGetAllNodes() {
    val a = createNode()
    val b = createNode()

    val result = execute("start n=node(*) return n")
    assert(List(a, b) === result.columnAs[Node]("n").toList)
  }

  @Test def shouldAllowComparisonsOfNodes() {
    val a = createNode()
    val b = createNode()

    val result = execute("start a=node(0,1),b=node(1,0) where a <> b return a,b")
    assert(Set(Map("a" -> b, "b" -> a), Map("b" -> b, "a" -> a)) === result.toSet)
  }

  @Test def arithmeticsPrecedenceTest() {
    val result = execute("return 12/4*3-2*4")
    assert(List(Map("12/4*3-2*4" -> 1)) === result.toList)
  }

  @Test def arithmeticsPrecedenceWithParenthesisTest() {
    val result = execute("return 12/4*(3-2*4)")
    assert(List(Map("12/4*(3-2*4)" -> -15)) === result.toList)
  }

  @Test def shouldAllowAddition() {
    createNode("age" -> 36)

    val result = execute("start a=node(0) return a.age + 5 as newAge")
    assert(List(Map("newAge" -> 41)) === result.toList)
  }

  @Test def shouldSolveSelfreferencingPattern() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = execute("start a=node(0) match a-->b, b-->b return b")
    assert(List() === result.toList)
  }

  @Test def shouldSolveSelfreferencingPattern2() {
    val a = createNode()
    val b = createNode()

    val r = relate(a, a)
    relate(a, b)

    val result = execute("start a=node(0) match a-[r]->a return r")
    assert(List(Map("r" -> r)) === result.toList)
  }

  @Test def absFunction() {
    createNode()
    val result = execute("start a=node(0) return abs(-1)")
    assert(List(Map("abs(-1)" -> 1)) === result.toList)
  }

  @Test def shouldBeAbleToDoDistinctOnUnboundNode() {
    createNode()

    val result = execute("start a=node(0) optional match a-->b return count(distinct b)")
    assert(List(Map("count(distinct b)" -> 0)) === result.toList)
  }

  @Test def shouldBeAbleToDoDistinctOnNull() {
    createNode()

    val result = execute("start a=node(0) return count(distinct a.foo)")
    assert(List(Map("count(distinct a.foo)" -> 0)) === result.toList)
  }

  @Test def exposesIssue198() {
    createNode()

    execute("start a=node(*) return a, count(*) order by COUNT(*)").toList
  }

  @Test def shouldAggregateOnArrayValues() {
    createNode("color" -> Array("red"))
    createNode("color" -> Array("blue"))
    createNode("color" -> Array("red"))

    val result = execute("start a=node(0,1,2) return distinct a.color, count(*)").toList
    result.foreach { x =>
      val c = x("a.color").asInstanceOf[Array[_]]

      c.toList match {
        case List("red")  => assert(2 === x("count(*)"))
        case List("blue") => assert(1 === x("count(*)"))
        case _            => fail("wut?")
      }
    }
  }

  @Test def functions_should_return_null_if_they_get_path_containing_unbound() {
    createNode()

    val result = execute("start a=node(0) optional match p=a-[r]->() return length(nodes(p)), id(r), type(r), nodes(p), rels(p)").toList

    assert(List(Map("length(nodes(p))" -> null, "id(r)" -> null, "type(r)" -> null, "nodes(p)" -> null, "rels(p)" -> null)) === result)
  }

  @Test def functions_should_return_null_if_they_get_path_containing_null() {
    createNode()

    val result = execute("start a=node(0) optional match p=a-[r]->() return length(rels(p)), id(r), type(r), nodes(p), rels(p)").toList

    assert(List(Map("length(rels(p))" -> null, "id(r)" -> null, "type(r)" -> null, "nodes(p)" -> null, "rels(p)" -> null)) === result)
  }

  @Test def aggregates_in_aggregates_should_fail() {
    createNode()

    intercept[SyntaxException](execute("start a=node(0) return count(count(*))").toList)
  }

  @Test def aggregates_inside_normal_functions_should_work() {
    createNode()

    val result = execute("start a=node(0) return length(collect(a))").toList
    assert(List(Map("length(collect(a))" -> 1)) === result)
  }

  @Test def aggregates_should_be_possible_to_use_with_arithmetics() {
    createNode()

    val result = execute("start a=node(0) return count(*) * 10").toList
    assert(List(Map("count(*) * 10" -> 10)) === result)
  }

  @Test def aggregates_should_be_possible_to_order_by_arithmetics() {
    createNode()
    createNode()
    createNode()

    val result = execute("start a=node(0),b=node(1,2) return count(a) * 10 + count(b) * 5 as X order by X").toList
    assert(List(Map("X" -> 30)) === result)
  }

  @Test def tests_that_filterfunction_works_as_expected() {
    val a = createNode("foo" -> 1)
    val b = createNode("foo" -> 3)
    relate(a, b, "rel", Map("foo" -> 2))

    val result = execute("start a=node(0) match p=a-->() return filter(x in nodes(p) WHERE x.foo > 2) as n").toList

    val resultingCollection = result.head("n").asInstanceOf[Seq[_]].toList

    assert(List(b) == resultingCollection)
  }

  @Test def expose_problem_with_aliasing() {
    createNode("nisse")
    execute("start n = node(0) return n.name, count(*) as foo order by n.name")
  }

  @Test def start_with_node_and_relationship() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)
    val result = execute("start a=node(0), r=relationship(0) return a,r").toList

    assert(List(Map("a" -> a, "r" -> r)) === result)
  }

  @Test def relationship_predicate_with_multiple_rel_types() {
    val a = createNode()
    val b = createNode()
    val x = createNode()

    relate(a, x, "A")
    relate(b, x, "B")

    val result = execute("start a=node(0,1) where a-[:A|:B]->() return a").toList

    assert(List(Map("a" -> a), Map("a" -> b)) === result)
  }

  @Test def nullable_var_length_path_should_work() {
    createNode()
    val b = createNode()

    val result = execute("start a=node(0), b=node(1) optional match a-[r*]-b where r is null and a <> b return b").toList

    assert(List(Map("b" -> b)) === result)
  }

  @Test def first_piped_query_woot() {
    val a = createNode("foo" -> 42)
    createNode("foo" -> 49)

    val q = "start x=node(0, 1) with x WHERE x.foo = 42 return x"
    val result = execute(q)

    assert(List(Map("x" -> a)) === result.toList)
  }

  @Test def second_piped_query_woot() {
    createNode()
    val q = "start x=node(0) with count(*) as apa WHERE apa = 1 RETURN apa"
    val result = execute(q)

    assert(List(Map("apa" -> 1)) === result.toList)
  }

  @Test def listing_rel_types_multiple_times_should_not_give_multiple_returns() {
    val a = createNode()
    val b = createNode()
    relate(a, b, "REL")

    val result = execute("start a=node(0) match a-[:REL|:REL]-b return b").toList

    assert(List(Map("b" -> b)) === result)
  }

  @Test def should_throw_on_missing_indexes() {
    intercept[MissingIndexException](execute("start a=node:missingIndex(key='value') return a").toList)
    intercept[MissingIndexException](execute("start a=node:missingIndex('value') return a").toList)
    intercept[MissingIndexException](execute("start a=relationship:missingIndex(key='value') return a").toList)
    intercept[MissingIndexException](execute("start a=relationship:missingIndex('value') return a").toList)
  }

  @Test def distinct_on_nullable_values() {
    createNode("name" -> "Florescu")
    createNode()
    createNode()

    val result = execute("start a=node(0,1,2) return distinct a.name").toList

    assert(result === List(Map("a.name" -> "Florescu"), Map("a.name" -> null)))
  }

  @Test def createEngineWithSpecifiedParserVersion() {
    val db = new ImpermanentGraphDatabase(Map[String, String]("cypher_parser_version" -> "1.9").asJava)
    val engine = new ExecutionEngine(db)

    try {
      // This syntax is valid today, but should give an exception in 1.5
      engine.execute("create a")
    } catch {
      case x: SyntaxException =>
      case _: Throwable => fail("expected exception")
    } finally {
      db.shutdown()
    }
  }

  @Test def different_results_on_ordered_aggregation_with_limit() {
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

    assert(resultWithLimit.toList === resultWithoutLimit.toList)
  }

  @Test def return_all_identifiers() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val q = "start a=node(0) match p=a-->b return *"

    val result = execute(q).toList
    val first = result.head
    assert(first.keys === Set("a", "b", "p"))
    assert(first("p").asInstanceOf[Path] == PathImpl(a, r, b))
  }

  @Test def issue_446() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "age" -> 24)
    relate(a, c, "age" -> 38)
    relate(a, d, "age" -> 12)

    val q = "start n = node(0) match n-[f]->() with n, max(f.age) as age match n-[f]->m where f.age = age return m"

    assert(execute(q).toList === List(Map("m" -> c)))
  }

  @Test def issue_432() {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val q = "start n = node(0) match p = n-[*1..]->m return p, last(nodes(p)) order by length(nodes(p)) asc"

    assert(execute(q).size === 1)
  }

  @Test def issue_479() {
    createNode()

    val q = "start n = node(0) optional match (n)-->(x) where x-->() return x"

    assert(execute(q).toList === List(Map("x" -> null)))
  }

  @Test def issue_479_has_relationship_to_specific_node() {
    createNode()

    val q = "start n = node(0) optional match (n)-[:FRIEND]->(x) where not n-[:BLOCK]->x return x"

    assert(execute(q).toList === List(Map("x" -> null)))
  }

  @Test def issue_508() {
    createNode()

    val q = "start n=node(0) set n.x=[1,2,3] return length(n.x)"

    assert(execute(q).toList === List(Map("length(n.x)" -> 3)))
  }

  @Test def length_on_filter() {
    val q = "start n=node(*) optional match (n)-[r]->(m) return length(filter(x in collect(r) WHERE x <> null)) as cn"

    assert(executeScalar[Long](q) === 0)
  }

  @Test def long_or_double() {
    val result = execute("return 1, 1.5").toList.head

    assert(result("1").getClass === classOf[java.lang.Long])
    assert(result("1.5").getClass === classOf[java.lang.Double])
  }

  @Test def square_function_returns_decimals() {
    val result = execute("return sqrt(12.96)").toList

    assert(result === List(Map("sqrt(12.96)"->3.6)))
  }

  @Test def maths_inside_aggregation() {
    val andres = createNode("name"->"Andres")
    val michael = createNode("name"->"Michael")
    val peter = createNode("name"->"Peter")
    val bread = createNode("type"->"Bread")
    val veg = createNode("type"->"Veggies")
    val meat = createNode("type"->"Meat")

    relate(andres, bread, "ATE", Map("times"->10))
    relate(andres, veg, "ATE", Map("times"->8))

    relate(michael, veg, "ATE", Map("times"->4))
    relate(michael, bread, "ATE", Map("times"->6))
    relate(michael, meat, "ATE", Map("times"->9))

    relate(peter, veg, "ATE", Map("times"->7))
    relate(peter, bread, "ATE", Map("times"->7))
    relate(peter, meat, "ATE", Map("times"->4))

    execute(
      """    start me=node(1)
    match me-[r1:ATE]->food<-[r2:ATE]-you

    with me,count(distinct r1) as H1,count(distinct r2) as H2,you
    match me-[r1:ATE]->food<-[r2:ATE]-you

    return me,you,sum((1-ABS(r1.times/H1-r2.times/H2))*(r1.times+r2.times)/(H1+H2))""").dumpToString()
  }

  @Test def zero_matching_subgraphs_yield_correct_count_star() {
    val result = execute("start n=node(*) where 1 = 0 return count(*)").toList
    assert(List(Map("count(*)" -> 0)) === result)
  }

  @Test def should_return_paths_in_1_9() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    val d = createNode()
    relate(a, b, "X")
    relate(a, c, "X")
    relate(a, d, "X")

    val result = execute("cypher 1.9 start n = node(0) return n-->()").columnAs[List[Path]]("n-->()").toList.flatMap(p => p.map(_.endNode()))

    assert(result === List(b, c, d))
  }

  @Test def should_return_shortest_paths_if_using_a_ridiculously_unhip_cypher() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    val result = execute("cypher 1.9 start a=node(0), c=node(2) return shortestPath(a-[*]->c)").columnAs[List[Path]]("shortestPath(a-[*]->c)").toList.head.head
    assertEquals(result.endNode(), c)
    assertEquals(result.startNode(), a)
    assertEquals(result.length(), 2)
  }

  @Test def should_return_shortest_path() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a, b)
    relate(b, c)

    val result = execute("start a=node(0), c=node(2) return shortestPath(a-[*]->c)").columnAs[Path]("shortestPath(a-[*]->c)").toList.head
    assertEquals(result.endNode(), c)
    assertEquals(result.startNode(), a)
    assertEquals(result.length(), 2)
  }

  @Test
  def array_prop_output() {
    createNode("foo"->Array(1,2,3))
    val result = executeLazy("start n = node(0) return n").dumpToString()
    assertThat(result, containsString("[1,2,3]"))
  }

  @Test
  def var_length_expression_on_1_9() {
    val a = createNode()
    val b = createNode()
    val r = relate(a, b)

    val resultPath = execute("CYPHER 1.9 START a=node(0), b=node(1) RETURN a-[*]->b as path")
      .toList.head("path").asInstanceOf[List[Path]].head

    assert(resultPath.startNode() === a)
    assert(resultPath.endNode() === b)
    assert(resultPath.lastRelationship() === r)
  }

  @Test
  def var_length_predicate() {
    val a = createNode()
    val b = createNode()
    relate(a, b)

    val resultPath = execute("START a=node(0), b=node(1) RETURN a-[*]->b as path")
      .toList.head("path").asInstanceOf[Seq[_]]

    assert(resultPath.size === 1)
  }

  @Test
  def optional_expression_used_to_be_supported() {
    graph.inTx {
      val a = createNode()
      val b = createNode()
      val r = relate(a, b)

      val result = execute("CYPHER 1.9 start a=node(0) match a-->b RETURN a-[?]->b").toList
      assert(result === List(Map("a-[?]->b" -> List(PathImpl(a, r, b)))))
    }
  }

  @Test
  def pattern_expression_deep_in_function_call_in_1_9() {
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a,b)
    relate(a,c)

    graph.inTx {
      execute("CYPHER 1.9 start a=node(0) foreach(n in extract(p in a-->() | last(p)) | set n.touched = true) return a-->()").size
    }
  }

  @Test
  def with_should_not_forget_original_type() {
    val result = execute("create (a{x:8}) with a.x as foo return sum(foo)")

    assert(result.toList === List(Map("sum(foo)" -> 8)))
  }

  @Test
  def with_should_not_forget_parameters() {
    graph.inTx(graph.index().forNodes("test"))
    val id = "bar"
    val result = execute("start n=node:test(name={id}) with count(*) as c where c=0 create (x{name:{id}}) return c, x", "id" -> id).toList

    assert(result.size === 1)
    assert(result(0)("c").asInstanceOf[Long] === 0)
    assertInTx(result(0)("x").asInstanceOf[Node].getProperty("name") === id)
  }

  @Test
  def with_should_not_forget_parameters2() {
    val a = createNode()
    val id = a.getId
    val result = execute("start n=node({id}) with n set n.foo={id} return n", "id" -> id).toList

    assert(result.size === 1)
    assertInTx(result(0)("n").asInstanceOf[Node].getProperty("foo") === id)
  }

  @Test
  def pathDirectionRespected() {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = execute("start a=node(0) match p=b<--a return p").toList.head("p").asInstanceOf[Path]

    assert(result.startNode() === b)
    assert(result.endNode() === a)
  }

  @Test
  def shortestPathDirectionRespected() {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    val result = execute("START a=node(0), b=node(1) match p=shortestPath(b<-[*]-a) return p").toList.head("p").asInstanceOf[Path]

    assert(result.startNode() === b)
    assert(result.endNode() === a)
  }

  @Test
  def should_be_able_to_return_predicate_result() {
    createNode()
    val result = execute("START a=node(0) return id(a) = 0, a is null").toList
    assert(result === List(Map("id(a) = 0" -> true, "a is null" -> false)))
  }

  @Test def literal_collection() {
    val result = execute("return length([[],[]]+[[]]) as l").toList
    assert(result === List(Map("l" -> 3)))
  }

  @Test
  def shouldAllowArrayComparison() {
    val node = createNode("lotteryNumbers" -> Array(42, 87))

    val result = execute("start n = node(0) where n.lotteryNumbers = [42, 87] return n")

    assert(result.toList === List(Map("n" -> node)))
  }

  @Test
  def shouldSupportArrayOfArrayOfPrimitivesAsParameterForInKeyword() {
    val node = createNode("lotteryNumbers" -> Array(42, 87))

    val result = execute("start n = node(0) where n.lotteryNumbers in [[42, 87], [13], [42]] return n")

    assert(result.toList === List(Map("n" -> node)))
  }

  @Test
  def array_property_should_be_accessible_as_collection() {
    createNode()
    val result = execute("START n=node(0) SET n.array = [1,2,3,4,5] RETURN tail(tail(n.array))").
      toList.
      head("tail(tail(n.array))").
      asInstanceOf[Iterable[_]]

    assert(result.toList === List(3,4,5))
  }

  @Test
  def empty_collect_should_not_contain_null() {
    val n = createNode()
    val result = execute("START n=node(0) OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n, collect(x)")

    assert(result.toList === List(Map("n" -> n, "collect(x)" -> List())))
  }

  @Test
  def params_should_survive_with() {
    val n = createNode()
    val result = execute("START n=node(0) WITH collect(n) as coll where length(coll)={id} RETURN coll", "id"->1)

    assert(result.toList === List(Map("coll" -> List(n))))
  }

  @Test
  def nodes_named_r_should_not_pose_a_problem() {
    val a = createNode()
    val r = createNode("foo"->"bar")
    val b = createNode()

    relate(a,r)
    relate(r,b)

    val result = execute("START a=node(0) MATCH a-->r-->b WHERE r.foo = 'bar' RETURN b")

    assert(result.toList === List(Map("b" -> b)))
  }

  @Test
  def can_rewrite_has_property() {
    val a = createNode()
    val r = createNode("foo"->"bar")
    val b = createNode()

    relate(a,r)
    relate(r,b)

    val result = execute("START a=node(0) MATCH a-->r-->b WHERE has(r.foo) RETURN b")

    assert(result.toList === List(Map("b" -> b)))
  }

  @Test
  def can_use_identifiers_created_inside_the_foreach() {
    createNode()
    val result = execute("start n=node(0) foreach (x in [1,2,3] | create (a { name: 'foo'})  set a.id = x)")

    assert(result.toList === List())
  }

  @Test
  def can_alias_and_aggregate() {
    val a = createNode()
    val result = execute("start n = node(0) return sum(ID(n)), n as m")

    assert(result.toList === List(Map("sum(ID(n))"->0, "m"->a)))
  }

  @Test
  def can_handle_paths_with_multiple_unnamed_nodes() {
    createNode()
    val result = execute("START a=node(0) MATCH a<--()<--b-->()-->c RETURN c")

    assert(result.toList === List())
  }

  @Test
  def getting_top_x_when_we_have_less_than_x_left() {
    val r = new Random(1337)
    val nodes = (0 to 15).map(x => createNode("count" -> x)).sortBy(x => r.nextInt(100))

    val result = execute("START a=node({nodes}) RETURN a.count ORDER BY a.count SKIP 10 LIMIT 10", "nodes" -> nodes)

    assert(result.toList === List(
      Map("a.count" -> 10),
      Map("a.count" -> 11),
      Map("a.count" -> 12),
      Map("a.count" -> 13),
      Map("a.count" -> 14),
      Map("a.count" -> 15)
    ))
  }

  @Test
  def extract_string_from_node_collection() {
    createNode("name"->"a")

    val result = execute("""START n = node(0) with collect(n) as nodes return head(extract(x in nodes | x.name)) + "test" as test """)

    assert(result.toList === List(Map("test" -> "atest")))
  }

  @Test
  def substring_with_default_length() {
    val result = execute("return substring('0123456789', 1) as s")

    assert(result.toList === List(Map("s" -> "123456789")))
  }

  @Test
  def filtering_in_match_should_not_fail() {
    val n = createNode()
    relate(n, createNode("name" -> "Neo"))
    val result = execute("START n = node(0) MATCH n-->me WHERE me.name IN ['Neo'] RETURN me.name")

    assert(result.toList === List(Map("me.name"->"Neo")))
  }

  @Test
  def unexpected_traversal_state_should_never_be_hit() {
    val a = createNode()
    val b = createNode()
    val c = createNode()

    relate(a, b)
    relate(b, c)

    val result = execute("START n=node({a}), m=node({b}) MATCH n-[r]->m RETURN *", "a"->a, "b"->c)

    assert(result.toList === List())
  }

  @Test def path_expressions_should_work_with_on_the_fly_predicates() {
    val refNode = createNode()
    relate(refNode, createNode("name" -> "Neo"))
    val result = execute("START a=node({self}) MATCH a-->b WHERE b-->() RETURN b", "self"->refNode)

    assert(result.toList === List())
  }

  @Test def syntax_errors_should_not_leave_dangling_transactions() {

    val engine = new ExecutionEngine(graph)

    intercept[Throwable](engine.execute("BABY START SMILING, YOU KNOW THE SUN IS SHINING."))

    // Until we have a clean cut way where statement context is injected into cypher,
    // I don't know a non-hairy way to tell if this was done correctly, so here goes:
    val tx  = graph.beginTx()
    val isTopLevelTx = tx.getClass === classOf[TopLevelTransaction]
    tx.close()

    assert(isTopLevelTx)
  }

  @Test def should_add_label_to_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a :foo RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_add_multiple_labels_to_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a :foo:bar RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_set_label_on_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a:foo RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_set_multiple_labels_on_node() {
    val a = createNode()
    val result = execute("""START a=node(0) SET a:foo:bar RETURN a""")

    assert(result.toList === List(Map("a" -> a)))
  }

  @Test def should_filter_nodes_by_single_label() {
    // GIVEN
    val a = createLabeledNode("foo")
    val b = createLabeledNode("foo", "bar")
    createNode()

    // WHEN
    val result = execute("""START n=node(0, 1, 2) WHERE n:foo RETURN n""")

    // THEN
    assert(result.toList === List(Map("n" -> a), Map("n" -> b)))
  }

  @Test def should_filter_nodes_by_single_negated_label() {
    // GIVEN
    createLabeledNode("foo")
    createLabeledNode("foo", "bar")
    val c = createNode()

    // WHEN
    val result = execute("""START n=node(0, 1, 2) WHERE not(n:foo) RETURN n""")

    // THEN
    assert(result.toList === List(Map("n" -> c)))
  }

  @Test def should_filter_nodes_by_multiple_labels() {
    // GIVEN
    createLabeledNode("foo")
    val b = createLabeledNode("foo", "bar")
    createNode()

    // WHEN
    val result = execute("""START n=node(0, 1, 2) WHERE n:foo:bar RETURN n""")

    // THEN
    assert(result.toList === List(Map("n" -> b)))
  }

  @Test def should_filter_nodes_by_label_given_in_match() {
    // GIVEN
    val a = createNode()
    val b1 = createLabeledNode("foo")
    val b2 = createNode()

    relate(a, b1)
    relate(a, b2)

    // WHEN
    val result = execute(s"START a=node(${nodeId(a)}) OPTIONAL MATCH a-->(b:foo) RETURN b")

    // THEN
    assert(result.toList === List(Map("b" -> b1)))
  }

  @Test def should_filter_nodes_by_label_given_in_match_even_if_nodes_are_start_nodes() {
    // GIVEN
    val a1 = createLabeledNode("bar")
    val a2 = createLabeledNode("baz")
    val b = createLabeledNode("foo")

    relate(a1, b)
    relate(a2, b)

    // WHEN
    val result = execute( "START a=node(0,1), b=node(2) MATCH (a:bar) --> (b:foo) RETURN a")

    // THEN
    assert(result.toList === List(Map("a" -> a1)))
  }

  @Test def should_handle_path_predicates_with_labels() {
    // GIVEN
    val a = createNode()

    val b1 = createLabeledNode("A")
    val b2 = createLabeledNode("B")
    val b3 = createLabeledNode("C")

    relate(a, b1)
    relate(a, b2)
    relate(a, b3)

    // WHEN
    val result = execute("START n = node(0) RETURN n-->(:A)")

    val x = result.toList.head("n-->(:A)").asInstanceOf[Seq[_]]

    assert(x.size === 1)
  }

  @Test def should_create_index() {
    // GIVEN
    val labelName = "Person"
    val propertyKeys = Seq("name")

    // WHEN
    execute(s"""CREATE INDEX ON :$labelName(${propertyKeys.reduce(_ ++ "," ++ _)})""")

    // THEN
    graph.inTx {
      val indexDefinitions = graph.schema().getIndexes(DynamicLabel.label(labelName)).asScala.toSet
      assert(1 === indexDefinitions.size)

      val actual = indexDefinitions.head.getPropertyKeys.asScala.toSeq
      assert(propertyKeys == actual)
    }
  }

  @Test def union_ftw() {
    createNode()

    // WHEN
    val result = execute("START n=node(0) RETURN 1 as x UNION ALL START n=node(0) RETURN 2 as x")

    // THEN
    assert(result.toList === List(Map("x" -> 1), Map("x" -> 2)))
  }

  @Test def union_distinct() {
    createNode()

    // WHEN
    val result = execute("START n=node(0) RETURN 1 as x UNION START n=node(0) RETURN 1 as x")

    // THEN
    assert(result.toList === List(Map("x" -> 1)))
  }

  @Test
  def sort_columns_do_not_leak() {
    //GIVEN
    val result = execute("start n=node(*) return * order by id(n)")

    //THEN
    assert(result.columns === List("n"))
  }

  @Test
  def read_only_database_can_process_has_label_predicates() {
    //GIVEN
    val engine = createReadOnlyEngine()

    //WHEN
    val result = engine.execute("MATCH (n) WHERE n:NonExistingLabel RETURN n")

    //THEN
    assert(result.toList === List())
  }

  private def createReadOnlyEngine(): ExecutionEngine = {
    val old = new GraphDatabaseFactory().newEmbeddedDatabase("target/readonly")
    old.shutdown()
    val db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder("target/readonly")
            .setConfig( GraphDatabaseSettings.read_only, "true" )
            .newGraphDatabase();
    new ExecutionEngine(db)
  }

  def should_use_predicates_in_the_correct_place() {
    //GIVEN
    val m = execute( """create
                        advertiser = {name:"advertiser1"},
                        thing      = {name:"Color"},
                        red        = {name:"red"},
                        p1         = {name:"product1"},
                        p2         = {name:"product4"},
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
    assert(result.toList === List(Map("out.name" -> "product1")))
  }

  @Test
  def should_not_create_when_match_exists() {
    //GIVEN
    val a = createNode()
    val b = createNode()
    relate(a,b,"FOO")

    //WHEN
    val result = execute(
      """START a=node(0), b=node(1)
         WHERE not (a)-[:FOO]->(b)
         CREATE (a)-[new:FOO]->(b)
         RETURN new""")

    //THEN
    assert(result.size === 0)
    assert(result.queryStatistics().relationshipsCreated === 0)
  }

  @Test
  def test550() {
    createNode()

    //WHEN
    val result = execute(
      """START p=node(0)
        WITH p
        START a=node(0)
        MATCH a-->b
        RETURN *""")

    //THEN DOESN'T THROW EXCEPTION
    assert(result.toList === List())
  }

  @Test
  def should_be_able_to_use_index_hints() {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    graph.createIndex("Person", "name")

    //WHEN
    val result = execute("MATCH (n:Person)-->() USING INDEX n:Person(name) WHERE n.name = 'Jacob' RETURN n")

    //THEN
    assert(result.toList === List(Map("n"->jake)))
  }

  @Test
  def should_be_Able_to_use_label_as_start_point() {
    //GIVEN
    val andres = createLabeledNode(Map("name" -> "Andres"), "Person")
    val jake = createLabeledNode(Map("name" -> "Jacob"), "Person")
    relate(andres, createNode())
    relate(jake, createNode())

    //WHEN
    val result = execute("MATCH (n:Person)-->() WHERE n.name = 'Jacob' RETURN n")

    //THEN
    assert(result.toList === List(Map("n"->jake)))
  }

  @Test
  def should_allow_expression_alias_in_order_by_with_distinct() {
    createNode()

    //WHEN
    val result = execute(
      """START n=node(*)
        RETURN distinct ID(n) as id
        ORDER BY id DESC""")

    //THEN DOESN'T THROW EXCEPTION
    assert(result.toList === List(Map("id" -> 0)))
  }

  @Test
  def shouldProduceProfileWhenUsingLimit() {
    // GIVEN
    createNode()
    createNode()
    createNode()
    val result = profile("""START n=node(*) RETURN n LIMIT 1""")

    // WHEN
    result.toList

    // THEN PASS
    result.executionPlanDescription()
  }

  @Test
  def should_be_able_to_handle_single_node_patterns() {
    //GIVEN
    val n = createNode("foo" -> "bar")

    //WHEN
    val result = execute("start n = node(0) match n where n.foo = 'bar' return n")

    //THEN
    assert(result.toList === List(Map("n" -> n)))
  }

  @Test
  def should_be_able_to_handle_single_node_path_patterns() {
    //GIVEN
    val n = createNode("foo" -> "bar")

    //WHEN
    val result = execute("start n = node(0) match p = n return p")

    //THEN
    assert(result.toList === List(Map("p" -> PathImpl(n))))
  }

  @Test
  def should_handle_multiple_aggregates_on_the_same_node() {
    //WHEN
    val a = createNode()
    val result = execute("start n=node(*) return count(n), collect(n)")

    //THEN
    assert(result.toList === List(Map("count(n)" -> 1, "collect(n)" -> Seq(a))))
  }

  @Test
  def should_be_able_to_coalesce_nodes() {
    val n = createNode("n")
    val m = createNode("m")
    relate(n,m,"link")
    val result = execute("start n = node(0) with coalesce(n,n) as n match n--() return n")

    assert(result.toList === List(Map("n" -> n)))
  }

  @Test
  def multiple_start_points_should_still_honor_predicates() {
    val e = createNode()
    val p1 = createNode("value"->567)
    val p2 = createNode("value"->0)
    relate(p1,e)
    relate(p2,e)

    indexNode(p1, "stuff", "key", "value")
    indexNode(p2, "stuff", "key", "value")

    val result = execute("start p1=node:stuff('key:*'), p2=node:stuff('key:*') match (p1)--(e), (p2)--(e) where p1.value = 0 and p2.value = 0 AND p1 <> p2 return p1,p2,e")
    assert(result.toList === List())
  }

  @Test
  def should_be_able_to_prettify_queries() {
    val query = "match (n)-->(x) return n"

    assert(engine.prettify(query) === String.format("MATCH (n)-->(x)%nRETURN n"))
  }

  @Test
  def should_not_see_updates_created_by_itself() {

    timeOutIn(5, TimeUnit.SECONDS) {
      val result = execute("start n=node(*) create ()")
      assert(result.toList === List())
    }
  }

  @Test
  def doctest_gone_wild() {
    // given
    execute("CREATE (n:Actor {name:'Tom Hanks'})")

    // when
    val result = execute("""MATCH (actor:Actor)
                               WHERE actor.name = "Tom Hanks"
                               CREATE (movie:Movie {title:'Sleepless in Seattle'})
                               CREATE (actor)-[:ACTED_IN]->(movie)""")

    // then
    assertStats(result, nodesCreated = 1, propertiesSet = 1, labelsAdded = 1, relationshipsCreated = 1)
  }

  @Test
  def columns_should_not_change_when_using_order_by_and_distinct() {
    val n = createNode()
    val result = execute("start n=node(*) return distinct n order by id(n)")

    assert(result.toList === List(Map("n" -> n)))
  }

  @Test
  def should_iterate_all_node_id_sets_from_start_during_matching() {
    // given
    val nodes: List[Node] =
      execute("CREATE (a)-[:EDGE]->(b), (b)<-[:EDGE]-(c), (a)-[:EDGE]->(c) RETURN [a, b, c] AS nodes")
      .columnAs[List[Node]]("nodes").next().sortBy(_.getId)

    val nodeIds = s"node(${nodes.map(_.getId).mkString(",")})"

    // when
    val result = execute(s"START src=$nodeIds, dst=$nodeIds MATCH src-[r:EDGE]-dst RETURN r")

    // then
    val relationships: List[Relationship] = result.columnAs[Relationship]("r").toList

    assert( 6 === relationships.size )
  }

  @Test
  def allow_queries_with_only_return() {
    val result = execute("RETURN 'Andres'").toList

    assert(result === List(Map("'Andres'"->"Andres")))
  }

  @Test
  def id_in_where_leads_to_empty_result() {
    //WHEN
    val result = execute("MATCH n WHERE id(n)=1337 RETURN n")

    //THEN DOESN'T THROW EXCEPTION
    assert(result.toList === List())
  }

  @Test
  def merge_should_support_single_parameter() {
    //WHEN
    val result = execute("MERGE (n:User {foo: {single_param}})", ("single_param", 42))

    //THEN DOESN'T THROW EXCEPTION
    assert(result.toList === List())
  }

  @Test
  def merge_should_not_support_map_parameters_for_defining_properties() {
    intercept[SyntaxException](execute("MERGE (n:User {merge_map})", ("merge_map", Map("email" -> "test"))))
  }

  @Test
  def should_handle_two_unconnected_patterns() {
    // given a node with two related nodes
    val a = createNode()
    val b = createNode()
    val c = createNode()
    relate(a,b)
    relate(a,c)

    // when asked for a cartesian product of the same match twice
    val result = execute("match a-->b with a,b match c-->d return a,b,c,d")

    // then we should find 2 x 2 = 4 result matches
    assert(result.toSet === Set(
      Map("a" -> a, "b" -> b, "c" -> a, "d" -> b),
      Map("a" -> a, "b" -> b, "c" -> a, "d" -> c),
      Map("a" -> a, "b" -> c, "c" -> a, "d" -> b),
      Map("a" -> a, "b" -> c, "c" -> a, "d" -> c)))
  }

  @Test
  def should_allow_distinct_followed_by_order_by() {
    // given a database with one node
    createNode()

    // then shouldn't throw
    execute("START x=node(0) RETURN DISTINCT x as otherName ORDER BY x.name ")
  }

  def should_not_hang() {
    // given
    createNode()
    createNode()

    // when
    timeOutIn(2, TimeUnit.SECONDS) {
      execute("START a=node(0), b=node(1) " +
        "MATCH x-->a, x-->b " +
        "WHERE x.foo > 2 AND x.prop IN ['val'] " +
        "RETURN x")
    }
    // then
  }

  @Test
  def should_return_null_on_all_comparisons_against_null() {
    // given

    // when
    val result = execute("return 1 > null as A, 1 < null as B, 1 <= null as C, 1 >= null as D, null <= null as E, null >= null as F")

    // then
    assert(result.toList === List(Map("A" -> null, "B" -> null, "C" -> null, "D" -> null, "E" -> null, "F" -> null)))
  }

  @Test
  def should_propagate_null_through_math_funcs() {
    val result = execute("return 1 + (2 - (3 * (4 / (5 ^ (6 % null))))) as A")
    assert(result.toList === List(Map("A" -> null)))
  }

  @Test
  def should_be_able_to_set_properties_with_a_literal_map_twice_in_the_same_transaction() {
    val node = createLabeledNode("FOO")

    graph.inTx {
      execute("MATCH (n:FOO) SET n = { first: 'value' }")
      execute("MATCH (n:FOO) SET n = { second: 'value' }")
    }

    graph.inTx {
      assert(node.getProperty("first", null) === null)
      assert(node.getProperty("second") === "value")
    }
  }

  @Test
  def should_be_able_to_index_into_nested_literal_lists() {
    execute("RETURN [[1]][0][0]").toList
    // should not throw an exception
  }

  @Test
  def should_not_fail_if_asking_for_a_non_existent_node_id_with_WHERE() {
    execute("match (n) where id(n) in [0,1] return n").toList
    // should not throw an exception
  }

  @Test
  def non_optional_patterns_should_not_contain_nulls() {
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

  @Test
  def should_be_able_to_coerce_collections_to_predicates() {
    val n = createLabeledNode(Map("coll" -> Array(1, 2, 3), "bool" -> true), "LABEL")

    val foundNode = execute("match (n:LABEL) where n.coll and n.bool return n").columnAs[Node]("n").next()

    assert(foundNode === n)
  }

  @Test
  def query_should_work() {
    assert(executeScalar[Int]("WITH 1 AS x RETURN 1 + x") === 2)
  }

  @Test
  def should_be_able_to_mix_key_expressions_with_aggregate_expressions() {
    // Given
    createNode("Foo")

    // when
    val result = executeScalar[Map[String, Any]]("match (n) return { name: n.name, count: count(*) }")

    // then
    assert(result("name") === "Foo")
    assert(result("count") === 1)
  }

  @Test
  def should_handle_queries_that_cant_be_index_solved_because_expressions_lack_dependencies() {
    // Given
    val a = createLabeledNode(Map("property"->42), "Label")
    val b = createLabeledNode(Map("property"->42), "Label")
    val c = createLabeledNode(Map("property"->666), "Label")
    val d = createLabeledNode(Map("property"->666), "Label")
    val e = createLabeledNode(Map("property"->1), "Label")
    relate(a,b)
    relate(a,e)
    graph.createIndex("Label", "property")

    // when
    val result = execute("match (a:Label)-->(b:Label) where a.property = b.property return a, b")

    // then does not throw exceptions
    assert(result.toList === List(Map("a" -> a, "b" -> b)))
  }

  @Test
  def should_handle_queries_that_cant_be_index_solved_because_expressions_lack_dependencies_with_two_disjoin_patterns() {
    // Given
    val a = createLabeledNode(Map("property"->42), "Label")
    val b = createLabeledNode(Map("property"->42), "Label")
    val e = createLabeledNode(Map("property"->1), "Label")
    graph.createIndex("Label", "property")

    // when
    val result = execute("match (a:Label), (b:Label) where a.property = b.property return *")

    // then does not throw exceptions
    assert(result.toSet === Set(
      Map("a"->a, "b"->a),
      Map("a"->a, "b"->b),
      Map("a"->b, "b"->b),
      Map("a"->b, "b"->a),
      Map("a"->e, "b"->e)
    ))
  }

  @Test
  def should_not_mind_rewriting_NOT_queries() {
    val result = execute(" create (a {x: 1}) return a.x is not null as A, a.y is null as B, a.x is not null as C, a.y is not null as D")
    assert(result.toList === List(Map("A" -> true, "B" -> true, "C" -> true, "D" -> false)))
  }

  @Test
  def should_not_mind_profiling_union_queries() {
    val result = profile("return 1 as A union return 2 as A")
    assert(result.toList === List(Map("A" -> 1), Map("A" -> 2)))
  }

  @Test
  def should_not_mind_profiling_merge_queries() {
    val result = profile("merge (a {x: 1}) return a.x as A")
    assert(result.toList.head("A") === 1)
  }

  @Test
  def should_not_mind_profiling_optional_match_queries() {
    createLabeledNode(Map("x" -> 1), "Label")
    val result = profile("match (a:Label {x: 1}) optional match (a)-[:REL]->(b) return a.x as A, b.x as B").toList.head
    assert(result("A") === 1)
    assert(result("B") === null)
  }

  @Test
  def should_not_mind_profiling_optional_match_and_with() {
    createLabeledNode(Map("x" -> 1), "Label")
    val result = profile("match (n) optional match (n)--(m) with n, m where m is null return n.x as A").toList.head
    assert(result("A") === 1)
  }
}
