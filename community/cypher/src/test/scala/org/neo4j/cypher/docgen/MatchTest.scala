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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.junit.Assert._
import org.neo4j.graphdb.Node
class MatchTest extends DocumentingTestBase {
  override def indexProps: List[String] = List("name")

  def graphDescription: List[String] = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E")

  def section: String = "MATCH"

  @Test def allRelationships() {
    testQuery(
      title = "Related nodes",
      text = "The symbol `--` means related to, without regard to type or direction.",
      queryText = """start n=(%A%) match (n)--(x) return x""",
      returns = """All nodes related to A are returned""",
      (p) => assertEquals(List(node("B"), node("D"), node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def allOutgoingRelationships() {
    testQuery(
      title = "Outgoing relationships",
      text = "When the direction of a relationship is interesting, it is shown by using `-->` or `<--`, like this: ",
      queryText = """start n=(%A%) match (n)-->(x) return x""",
      returns = """All nodes that A has outgoing relationships to.""",
      (p) => assertEquals(List(node("B"), node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def allOutgoingRelationships2() {
    testQuery(
      title = "Directed relationships and identifier",
      text = "If an identifier is needed, either for filtering on properties of the relationship, or to return the relationship, " +
        "this is how you introduce the identifier.",
      queryText = """start n=(%A%) match (n)-[r]->() return r""",
      returns = """All outgoing relationships from node A.""",
      (p) => assertEquals(2, p.size)
    )
  }

  @Test def relatedNodesByRelationshipType() {
    testQuery(
      title = "Match by relationship type",
      text = "When you know the relationship type you want to match on, you can specify it by using a colon.",
      queryText = """start n=(%A%) match (n)-[:BLOCKS]->(x) return x""",
      returns = """All nodes that are BLOCKed by A.""",
      (p) => assertEquals(List(node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def relationshipsByType() {
    testQuery(
      title = "Match by relationship type and use an identifier",
      text = "If you both want to introduce an identifier to hold the relationship, and specify the relationship type you want, " +
        "just add them both, like this.",
      queryText = """start n=(%A%) match (n)-[r:BLOCKS]->() return r""",
      returns = """All +BLOCKS+ relationship going out from A.""",
      (p) => assertEquals(1, p.size)
    )
  }

  @Test def relationshipsByTypeWithSpace() {
    testQuery(
      title = "Relationship types with uncommon characters",
      text = "Sometime your database will have types with non-letter characters, or with spaces in them. Use ` to escape these.",
      queryText = """start n=(%A%) match (n)-[r:BLOCKS]->() return r""",
      returns = """All +BLOCKS+ relationship going out from A.""",
      (p) => assertEquals(1, p.size)
    )
  }

  @Test def multiStepRelationships() {
    testQuery(
      title = "Multiple relationships",
      text = "Relationships can be expressed by using multiple statements in the form of `()--()`, or they can be stringed together, " +
        "like this:",
      queryText = """start a=(%A%) match (a)-[:KNOWS]->(b)-[:KNOWS]->(c) return a,b,c""",
      returns = """The three nodes in the path.""",
      (p) => assertEquals(List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("E"))), p.toList)
    )
  }

  @Test def variableLengthPath() {
    testQuery(
      title = "Variable length relationships",
      text = "Nodes that are variable number of relationship->node hops can be found using `-[:TYPE^minHops..maxHops]->`. ",
      queryText = """start a=(%A%), x=(%E%, %B%) match a-[:KNOWS^1..3]->x return a,x""",
      returns = """The three nodes in the path.""",
      (p) => assertEquals(List(
        Map("a" -> node("A"), "x" -> node("E")),
        Map("a" -> node("A"), "x" -> node("B"))), p.toList)
    )
  }

  @Test def optionalRelationship() {
    testQuery(
      title = "Optional relationship",
      text = "If a relationship is optional, it can be marked with a question mark. This similar to how a SQL outer join " +
        "works, if the relationship is there, it is returned. If it's not, +null+ is returned in it's place. Remember that " +
        "anything hanging of an optional relation, is in turn optional, unless it is connected with a bound node some other " +
        "path.",
      queryText = """start a=(%E%) match a-[?]->x return a,x""",
      returns = """A node, and +null+, since the node has no relationships.""",
      (p) => assertEquals(List(Map("a" -> node("E"), "x" -> null)), p.toList)
    )
  }

  @Test def optionalTypedRelationship() {
    testQuery(
      title = "Optional typed and named relationship",
      text = "Just as with a normal relationship, you can decide which identifier it goes into, and what relationship type " +
        "you need.",
      queryText = """start a=(%A%) match a-[r?:LOVES]->() return a,r""",
      returns = """A node, and +null+, since the node has no relationships.""",
      (p) => assertEquals(List(Map("a" -> node("A"), "r" -> null)), p.toList)
    )
  }

  @Test def introduceNamedPath() {
    testQuery(
      title = "Named path",
      text = "If you want to return or filter on a path in your pattern graph, you can a introduce a named path.",
      queryText = """start a=(%A%) match p = a-->b return p""",
      returns = """The two paths starting from the first node.""",
      (p) => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def complexMatching() {
    testQuery(
      title = "Complex matching",
      text = "Using Cypher, you can also express more complex patterns to match on, like a diamond shape pattern.",
      queryText = """start a=(%A%)
match (a)-[:KNOWS]->(b)-[:KNOWS]->(c), (a)-[:BLOCKS]-(d)-[:KNOWS]-(c)
return a,b,c,d""",
      returns = """The four nodes in the path.""",
      p => {
        assertEquals(List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("E"), "d" -> node("C"))), p.toList)
      }
    )
  }

}