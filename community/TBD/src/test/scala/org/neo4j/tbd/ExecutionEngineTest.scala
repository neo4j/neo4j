/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.tbd

import commands._
import org.junit.Assert._
import java.lang.String
import org.junit.{Ignore, Test}
import org.neo4j.graphdb.{Relationship, Direction, Node}

class ExecutionEngineTest extends ExecutionEngineTestBase {

  @Test def shouldGetReferenceNode() {
    val query = Query(
      Return(EntityOutput("node")),
      Start(NodeById("node", 0)))

    val result = execute(query)
    assertEquals(List(refNode), result.columnAs[Node]("node").toList)
  }

  @Test def shouldFilterOnGreaterThan() {
    val query = Query(
      Return(EntityOutput("node")),
      Start(NodeById("node", 0)),
      LessThan(Literal(0), Literal(1))
    )

    val result = execute(query)
    assertEquals(List(refNode), result.columnAs[Node]("node").toList)
  }

  @Test def shouldFilterOnRegexp() {
    val n1 = createNode(Map("name"->"Andres"))
    val n2 = createNode(Map("name"->"Jim"))
    val query = Query(
      Return(EntityOutput("node")),
      Start(NodeById("node", n1.getId, n2.getId)),
      RegularExpression(PropertyValue("node", "name"), "And.*")
    )

    val result = execute(query)
    assertEquals(List(n1), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetOtherNode() {
    val node: Node = createNode()

    val query = Query(
      Return(EntityOutput("node")),
      Start(NodeById("node", node.getId)))

    val result = execute(query)
    assertEquals(List(node), result.columnAs[Node]("node").toList)
  }

  @Ignore("graph-matching doesn't support using relationships as start points. revisit when it does.")
  @Test def shouldGetRelationship() {
    val node: Node = createNode()
    val rel: Relationship = relate(refNode, node, "yo")

    val query = Query(
      Return(EntityOutput("rel")),
      Start(RelationshipById("rel", rel.getId)))

    val result = execute(query)
    assertEquals(List(rel), result.columnAs[Relationship]("rel").toList)
  }

  @Test def shouldGetTwoNodes() {
    val node: Node = createNode()

    val query = Query(
      Return(EntityOutput("node")),
      Start(NodeById("node", refNode.getId, node.getId)))

    val result = execute(query)
    assertEquals(List(refNode, node), result.columnAs[Node]("node").toList)
  }

  @Test def shouldGetNodeProperty() {
    val name = "Andres"
    val node: Node = createNode(Map("name" -> name))

    val query = Query(
      Return(PropertyOutput("node", "name")),
      Start(NodeById("node", node.getId)))

    val result = execute(query)
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

    val query = Query(
      Return(EntityOutput("a")),
      Start(NodeById("start", start.getId)),
      Match(RelatedTo("start", "a", None, Some("x"), Direction.BOTH)),
      Equals(PropertyValue("a", "name"), Literal(name)))

    val result = execute(query)
    assertEquals(List(a2), result.columnAs[Node]("a").toList)
  }

  @Test def shouldFilterBasedOnRelPropName() {
    val start: Node = createNode()
    val a: Node = createNode()
    val b: Node = createNode()
    relate(start, a, "KNOWS", Map("name" -> "monkey"))
    relate(start, b, "KNOWS", Map("name" -> "woot"))

    val query = Query(
      Return(EntityOutput("a")),
      Start(NodeById("start", start.getId)),
      Match(RelatedTo("start", "a", Some("r"), Some("KNOWS"), Direction.BOTH)),
      Equals(PropertyValue("r", "name"), Literal("monkey")))

    val result = execute(query)
    assertEquals(List(a), result.columnAs[Node]("a").toList)
  }

  @Ignore("Maybe later")
  @Test def shouldOutputTheCartesianProductOfTwoNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()

    val query = Query(
      Return(EntityOutput("n1"), EntityOutput("n2")),
      Start(
        NodeById("n1", n1.getId),
        NodeById("n2", n2.getId)))

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetNeighbours() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    relate(n1, n2, "KNOWS")

    val query = Query(
      Return(EntityOutput("n1"), EntityOutput("n2")),
      Start(NodeById("n1", n1.getId)),
      Match(RelatedTo("n1", "n2", None, Some("KNOWS"), Direction.OUTGOING)))

    val result = execute(query)

    assertEquals(List(Map("n1" -> n1, "n2" -> n2)), result.toList)
  }

  @Test def shouldGetTwoRelatedNodes() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query(
      Return(EntityOutput("x")),
      Start(NodeById("start", n1.getId)),
      Match(RelatedTo("start", "x", None, Some("KNOWS"), Direction.OUTGOING)))

    val result = execute(query)

    assertEquals(List(Map("x" -> n2), Map("x" -> n3)), result.toList)
  }

  @Test def toStringTest() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n1, n3, "KNOWS")

    val query = Query(
      Return(EntityOutput("x"), EntityOutput("start")),
      Start(NodeById("start", n1.getId)),
      Match(RelatedTo("start", "x", None, Some("KNOWS"), Direction.OUTGOING)))

    val result = execute(query)

    println(result)
  }

  @Test def shouldGetRelatedToRelatedTo() {
    val n1: Node = createNode()
    val n2: Node = createNode()
    val n3: Node = createNode()
    relate(n1, n2, "KNOWS")
    relate(n2, n3, "FRIEND")

    val query = Query(
      Return(EntityOutput("b")),
      Start(NodeById("start", n1.getId)),
      Match(
        RelatedTo("start", "a", None, Some("KNOWS"), Direction.OUTGOING),
        RelatedTo("a", "b", None, Some("FRIEND"), Direction.OUTGOING)))

    val result = execute(query)

    assertEquals(List(Map("b" -> n3)), result.toList)
  }

  @Test def shouldFindNodesByIndex() {
    val n = createNode()
    val idxName = "idxName"
    val key = "key"
    val value = "andres"
    indexNode(n, idxName, key, value)

    val query = Query(
      Return(EntityOutput("n")),
      Start(NodeByIndex("n", idxName, key, value)))

    val result = execute(query)

    assertEquals(List(Map("n" -> n)), result.toList)
  }

  @Test def shouldHandleOrFilters() {
    val n1 = createNode(Map("name" -> "boy"))
    val n2 = createNode(Map("name" -> "girl"))

    val query = Query(
      Return(EntityOutput("n")),
      Start(NodeById("n", n1.getId, n2.getId)),
      Or(
        Equals(PropertyValue("n", "name"), Literal("boy")),
        Equals(PropertyValue("n", "name"), Literal("girl"))))

    val result = execute(query)

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }


  @Test def shouldHandleNestedAndOrFilters() {
    val n1 = createNode(Map("animal" -> "monkey", "food" -> "banana"))
    val n2 = createNode(Map("animal" -> "cow", "food" -> "grass"))
    val n3 = createNode(Map("animal" -> "cow", "food" -> "banana"))

    val query = Query(
      Return(EntityOutput("n")),
      Start(NodeById("n", n1.getId, n2.getId, n3.getId)),
      Or(
        And(
          Equals(PropertyValue("n", "animal"), Literal("monkey")),
          Equals(PropertyValue("n", "food"), Literal("banana"))),
        And(
          Equals(PropertyValue("n", "animal"), Literal("cow")),
          Equals(PropertyValue("n", "food"), Literal("grass")))))

    val result = execute(query)

    assertEquals(List(n1, n2), result.columnAs[Node]("n").toList)
  }

  @Test def shouldBeAbleToOutputNullForMissingProperties() {
    val query = Query(
      Return(NullablePropertyOutput("node", "name")),
      Start(NodeById("node", 0)))

    val result = execute(query)
    assertEquals(List(Map("node.name" -> null)), result.toList)
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

    val query = Query(
      Return(EntityOutput("n"), EntityOutput("x")),
      Start(NodeById("n", n1.getId, n4.getId)),
      Match(RelatedTo("n", "x", None, None, Direction.OUTGOING)),
      Equals(PropertyValue("n", "animal"), PropertyValue("x", "animal")))

    val result = execute(query).toList

    assertEquals(List(
      Map("n" -> n1, "x" -> n3),
      Map("n" -> n4, "x" -> n2)), result)
  }

  @Ignore("No implemented yet")
  @Test def shouldBeAbleToCount() {
    val a = createNode() //start a = node(0) match (a) --> (b) return a, count(*)
    val b = createNode()
    relate(refNode, a, "A")
    relate(refNode, b, "A")

    val query = Query(
      Return(EntityOutput("a")),
      Start(NodeById("a", refNode.getId)),
      Match(RelatedTo("a", "b", None, None, Direction.OUTGOING)),
      Aggregation(Count("*")))

    val result = execute(query)

    assertEquals(List(Map("a" -> refNode, "count(*)" -> 2)), result.toList)
  }
}