/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.junit.Assert._
import org.neo4j.graphdb.{DynamicRelationshipType, Path, Node}
import org.neo4j.cypher.CuteGraphDatabaseService.gds2cuteGds
import org.junit.Test

class MatchTest extends DocumentingTestBase {
  override def indexProps: List[String] = List("name")

  def graphDescription = List("A KNOWS B", "A BLOCKS C", "D KNOWS A", "B KNOWS E", "C KNOWS E", "B BLOCKS D")

  override val properties = Map(
    "A" -> Map("name" -> "Anders"),
    "B" -> Map("name" -> "Bossman"),
    "C" -> Map("name" -> "Cesar"),
    "D" -> Map("name" -> "David"),
    "E" -> Map("name" -> "Emil")
  )

  def section: String = "MATCH"

  @Test def allRelationships() {
    testQuery(
      title = "Related nodes",
      text = "The symbol `--` means _related to,_ without regard to type or direction.",
      queryText = """start n=node(%A%) match (n)--(x) return x""",
      returns = """All nodes related to A (Anders) are returned by the query.""",
      assertions = (p) => assertEquals(List(node("B"), node("D"), node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def allOutgoingRelationships() {
    testQuery(
      title = "Outgoing relationships",
      text = "When the direction of a relationship is interesting, it is shown by using `-->` or `<--`, like this: ",
      queryText = """start n=node(%A%) match (n)-->(x) return x""",
      returns = """All nodes that A has outgoing relationships to are returned.""",
      assertions = (p) => assertEquals(List(node("B"), node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def allOutgoingRelationships2() {
    testQuery(
      title = "Directed relationships and identifier",
      text = "If an identifier is needed, either for filtering on properties of the relationship, or to return the relationship, " +
        "this is how you introduce the identifier.",
      queryText = """start n=node(%A%) match (n)-[r]->() return r""",
      returns = """The query returns all outgoing relationships from node A.""",
      assertions = (p) => assertEquals(2, p.size)
    )
  }

  @Test def relatedNodesByRelationshipType() {
    testQuery(
      title = "Match by relationship type",
      text = "When you know the relationship type you want to match on, you can specify it by using a colon together with the relationship type.",
      queryText = """start n=node(%A%) match (n)-[:BLOCKS]->(x) return x""",
      returns = """All nodes that are BLOCKed by A are returned by this query.""",
      assertions = (p) => assertEquals(List(node("C")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def relatedNodesByMultipleRelationshipTypes() {
    testQuery(
      title = "Match by multiple relationship types",
      text = "To match on one of multiple types, you can specify this by chaining them together with the pipe symbol `|`.",
      queryText = """start n=node(%A%) match (n)-[:BLOCKS|KNOWS]->(x) return x""",
      returns = """All nodes with a +BLOCK+ or +KNOWS+ relationship to A are returned.""",
      assertions = (p) => assertEquals(List(node("C"), node("B")), p.columnAs[Node]("x").toList)
    )
  }

  @Test def relationshipsByType() {
    testQuery(
      title = "Match by relationship type and use an identifier",
      text = "If you both want to introduce an identifier to hold the relationship, and specify the relationship type you want, " +
        "just add them both, like this.",
      queryText = """start n=node(%A%) match (n)-[r:BLOCKS]->() return r""",
      returns = """All +BLOCKS+ relationships going out from A are returned.""",
      assertions = (p) => assertEquals(1, p.size)
    )
  }

  @Test def relationshipsByTypeWithSpace() {
    db.inTx(() => {
      val a = node("A")
      val b = node("A")
      a.createRelationshipTo(b, DynamicRelationshipType.withName("TYPE THAT HAS SPACE IN IT"))
    })
    testQuery(
      title = "Relationship types with uncommon characters",
      text = "Sometime your database will have types with non-letter characters, or with spaces in them. Use +`+ (backtick) to quote these.",
      queryText = """start n=node(%A%) match (n)-[r:`TYPE THAT HAS SPACE IN IT`]->() return r""",
      returns = """This query returns a relationship of a type with spaces in it.""",
      assertions = (p) => assertEquals(1, p.size)
    )
  }

  @Test def multiStepRelationships() {
    testQuery(
      title = "Multiple relationships",
      text = "Relationships can be expressed by using multiple statements in the form of `()--()`, or they can be strung together, " +
        "like this:",
      queryText = """start a=node(%A%) match (a)-[:KNOWS]->(b)-[:KNOWS]->(c) return a,b,c""",
      returns = """The three nodes in the path are returned by the query.""",
      assertions = (p) => assertEquals(List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("E"))), p.toList)
    )
  }

  @Test def variableLengthPath() {
    testQuery(
      title = "Variable length relationships",
      text = """Nodes that are a variable number of relationship->node hops away can be found using the following syntax:
`-[:TYPE*minHops..maxHops]->`.
Note that either the minimum or maximum limit can be omitted.""",
      queryText = """start a=node(%A%), x=node(%E%, %B%) match a-[:KNOWS*1..3]->x return a,x""",
      returns = "This query returns the start and end point, if there is a path between 1 and 3 relationships away.",
      assertions = (p) => assertEquals(List(
        Map("a" -> node("A"), "x" -> node("E")),
        Map("a" -> node("A"), "x" -> node("B"))), p.toList)
    )
  }

  @Test def variableLengthPathWithIterableRels() {
    testQuery(
      title = "Relationship identifier in variable length relationships",
      text = """When the connection between two nodes is of variable length, a relationship identifier becomes an iterable of relationships.""",
      queryText = """start a=node(%A%), x=node(%E%, %B%) match a-[r:KNOWS*1..3]->x return r""",
      returns = "The query returns the relationships, if there is a path between 1 and 3 relationships away.",
      assertions = (p) => assertEquals(2, p.toList.size)
    )
  }

  @Test def zeroLengthPath() {
    testQuery(
      title = "Zero length paths",
      text = "Using variable length paths that have the lower bound zero means that two identifiers can point" +
        " to the same node. If the distance between two nodes is zero, they are by definition the same node.",
      queryText = """start a=node(%A%) match p1=a-[:KNOWS*0..1]->b, p2=b-[:BLOCKS*0..1]->c return a,b,c, length(p1), length(p2)""",
      returns = "This query will return four paths, some of which have length zero.",
      assertions = p => assertEquals(Set(
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("A"), "length(p1)" -> 0, "length(p2)" -> 0),
        Map("a" -> node("A"), "b" -> node("A"), "c" -> node("C"), "length(p1)" -> 0, "length(p2)" -> 1),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("B"), "length(p1)" -> 1, "length(p2)" -> 0),
        Map("a" -> node("A"), "b" -> node("B"), "c" -> node("D"), "length(p1)" -> 1, "length(p2)" -> 1))
        , p.toSet)
    )
  }

  @Test def fixedLengthPath() {
    testQuery(
      title = "Fixed length relationships",
      text = "Elements that are a fixed number of hops away can be matched by using [*numberOfHops]. ",
      queryText = """start a=node(%D%) match p=a-[*3]->() return p""",
      returns = "The three paths that go from node D to node E",
      assertions = (p) => assert(p.toSeq.length === 3)
    )
  }

  @Test def optionalRelationship() {
    testQuery(
      title = "Optional relationship",
      text = "If a relationship is optional, it can be marked with a question mark. This is similar to how a SQL outer join " +
        "works. If the relationship is there, it is returned. If it's not, +null+ is returned in it's place. Remember that " +
        "anything hanging off an optional relationship, is in turn optional, unless it is connected with a bound node through some other " +
        "path.",
      queryText = """start a=node(%E%) match a-[?]->x return a,x""",
      returns = """A node, and +null+ are returned, since the node has no outgoing relationships.""",
      assertions = (p) => assertEquals(List(Map("a" -> node("E"), "x" -> null)), p.toList)
    )
  }

  @Test def nodePropertyFromOptionalNode() {
    testQuery(
      title = "Properties on optional elements",
      text = "Returning a property from an optional element that is +null+ will also return +null+.",
      queryText = """start a=node(%E%) match a-[?]->x return x, x.name""",
      returns = """This returns the element x (`null` in this query), and `null` as it's name.""",
      assertions = (p) => assertEquals(List(Map("x" -> null, "x.name" -> null)), p.toList)
    )
  }

  @Test def optionalTypedRelationship() {
    testQuery(
      title = "Optional typed and named relationship",
      text = "Just as with a normal relationship, you can decide which identifier it goes into, and what relationship type " +
        "you need.",
      queryText = """start a=node(%A%) match a-[r?:LOVES]->() return a,r""",
      returns = """This returns a node, and +null+, since the node has no outgoing `LOVES` relationships.""",
      assertions = (p) => assertEquals(List(Map("a" -> node("A"), "r" -> null)), p.toList)
    )
  }

  @Test def shortestPathBetweenTwoNodes() {
    testQuery(
      title = "Shortest path",
      text = "Finding a single shortest path between two nodes is as easy as using the `shortestPath` function. It's done like this:",
      queryText = """start d=node(%D%), e=node(%E%) match p = shortestPath( d-[*..15]->e ) return p""",
      returns = """This means: find a single shortest path between two nodes, as long as the path is max 15 relationships long. Inside of the parenthesis
 you define a single link of a path -- the starting node, the connecting relationship and the end node. Characteristics describing the relationship
 like relationship type, max hops and direction are all used when finding the shortest path. You can also mark the path as optional.""",
      assertions = (p) => assertEquals(3, p.toList.head("p").asInstanceOf[Path].length())
    )
  }

  @Test def allShortestPathsBetweenTwoNodes() {
    testQuery(
      title = "All shortest paths",
      text = "Finds all the shortest paths between two nodes.",
      queryText = """start d=node(%D%), e=node(%E%) match p = allShortestPaths( d-[*..15]->e ) return p""",
      returns = """This example will find the two directed paths between David and Emil.""",
      assertions = (p) => assertEquals(2, p.toList.size)
    )
  }

  @Test def complexMatching() {
    testQuery(
      title = "Complex matching",
      text = "Using Cypher, you can also express more complex patterns to match on, like a diamond shape pattern.",
      queryText = """start a=node(%A%)
match (a)-[:KNOWS]->(b)-[:KNOWS]->(c), (a)-[:BLOCKS]-(d)-[:KNOWS]-(c)
return a,b,c,d""",
      returns = """This returns the four nodes in the paths.""",
      assertions = p => assertEquals(List(Map("a" -> node("A"), "b" -> node("B"), "c" -> node("E"), "d" -> node("C"))), p.toList)
    )
  }

  @Test def introduceNamedPath() {
    testQuery(
      title = "Named path",
      text = "If you want to return or filter on a path in your pattern graph, you can a introduce a named path.",
      queryText = """start a=node(%A%) match p = a-->b return p""",
      returns = """This returns the two paths starting from the first node.""",
      assertions = (p) => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def match_on_bound_relationship() {
    testQuery(
      title = "Matching on a bound relationship",
      text = """When your pattern contains a bound relationship, and that relationship pattern doesn't specify direction,
Cypher will try to match the relationship where the connected nodes switch sides.""",
      queryText = """start r=rel(0) match a-[r]-b return a,b""",
      returns = "This returns the two connected nodes, once as the start node, and once as the end node.",
      assertions = p => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def match_mimicking_or() {
    testQuery(
      title = "Match with OR",
      text = "Strictly speaking, you can't do `OR` in your `MATCH`. It's still possible to form a query that " +
             "works a lot like `OR`.",
      queryText = "start a=node(%A%), b=node(%E%) match a-[?:KNOWS]-x-[?:KNOWS]-b return x",
      returns = "This query is saying: give me the nodes that are connected to `a`, or `b`, or both.",
      assertions = p => assertEquals(Set(node("D"), node("B"), node("C")), p.columnAs[Node]("x").toSet)
    )
  }
}
