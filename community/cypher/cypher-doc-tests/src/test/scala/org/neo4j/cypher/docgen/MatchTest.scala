/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.junit.Test
import org.neo4j.tooling.GlobalGraphOperations
import collection.JavaConverters._

class MatchTest extends DocumentingTestBase {

  def graphDescription = List(
    "Charlie:Person ACTED_IN WallStreet:Movie",
    "Martin:Person ACTED_IN WallStreet:Movie",
    "Michael:Person ACTED_IN WallStreet:Movie",
    "Martin:Person ACTED_IN TheAmericanPresident:Movie",
    "Michael:Person ACTED_IN TheAmericanPresident:Movie",
    "Oliver:Person DIRECTED WallStreet:Movie",
    "Rob:Person DIRECTED TheAmericanPresident:Movie",
    "Charlie:Person FATHER Martin:Person")

  override val properties = Map(
    "Charlie" -> Map("name" -> "Charlie Sheen"),
    "Oliver" -> Map("name" -> "Oliver Stone"),
    "Michael" -> Map("name" -> "Michael Douglas"),
    "Rob" -> Map("name" -> "Rob Reiner"),
    "Martin" -> Map("name" -> "Martin Sheen"),
    "WallStreet" -> Map("title" -> "Wall Street"),
    "TheAmericanPresident" -> Map("title" -> "The American President")
  )

  def section: String = "MATCH"

  @Test def get_all_nodes() {
    testQuery(
      title = "Get all nodes",
      text = "By just specifying a pattern with a single node and no labels, all nodes in the graph will be returned.",
      queryText = """match n return n""",
      returns = "Returns all the nodes in the database.",
      assertions = (p) => {
        val allNodes: List[Node] = GlobalGraphOperations.at(db).getAllNodes.asScala.toList

        assertEquals(allNodes, p.columnAs[Node]("n").toList)
      }
    )
  }
  @Test def get_all_labeled_nodes() {
    testQuery(
      title = "Get all nodes with a label",
      text = "Getting all nodes with a label on them is done with a single node pattern where the node has a label on it.",
      queryText = """match (movie:Movie) return movie""",
      returns = "Returns all the movies in the database.",
      assertions = (p) => assertEquals(nodes("WallStreet", "TheAmericanPresident").toSet, p.columnAs[Node]("movie").toSet)
    )
  }

  @Test def allRelationships() {
    testQuery(
      title = "Related nodes",
      text = "The symbol `--` means _related to,_ without regard to type or direction.",
      queryText = """match (director)--(movie) where director.name='Oliver Stone' return movie.title""",
      returns = """Returns all the movies directed by Oliver Stone.""",
      assertions = (p) => assertEquals(List("Wall Street"), p.columnAs[Node]("movie.title").toList)
    )
  }

  @Test def allOutgoingRelationships() {
    testQuery(
      title = "Outgoing relationships",
      text = "When the direction of a relationship is interesting, it is shown by using `-->` or `<--`, like this: ",
      queryText = """match (martin)-->(movie) where martin.name='Martin Sheen' return movie.title""",
      returns = """Returns nodes connected to Martin by outgoing relationships.""",
      assertions = (p) => assertEquals(List("Wall Street", "The American President"), p.columnAs[Node]("movie.title").toList)
    )
  }

  @Test def allOutgoingRelationships2() {
    testQuery(
      title = "Directed relationships and identifier",
      text = "If an identifier is needed, either for filtering on properties of the relationship, or to return the relationship, " +
        "this is how you introduce the identifier.",
      queryText = """match (martin)-[r]->(movie) where martin.name='Martin Sheen' return r""",
      returns = """Returns all outgoing relationships from Martin.""",
      assertions = (p) => assertEquals(2, p.size)
    )
  }

  @Test def relatedNodesByRelationshipType() {
    testQuery(
      title = "Match by relationship type",
      text = "When you know the relationship type you want to match on, you can specify it by using a colon together with the relationship type.",
      queryText = """match (wallstreet)<-[:ACTED_IN]-(actor) where wallstreet.title='Wall Street' return actor""",
      returns = """Returns nodes that +ACTED_IN+ Wall Street.""",
      assertions = (p) => assertEquals(nodes("Michael","Martin","Charlie").toSet, p.columnAs[Node]("actor").toSet)
    )
  }

  @Test def relatedNodesByMultipleRelationshipTypes() {
    testQuery(
      title = "Match by multiple relationship types",
      text = "To match on one of multiple types, you can specify this by chaining them together with the pipe symbol `|`.",
      queryText = """match (wallstreet)<-[:ACTED_IN|:DIRECTED]-(person) where wallstreet.title='Wall Street' return person""",
      returns = """Returns nodes with a +ACTED_IN+ or +DIRECTED+ relationship to Wall Street.""",
      assertions = (p) => assertEquals(nodes("Michael","Martin","Charlie","Oliver").toSet, p.columnAs[Node]("person").toSet)
    )
  }

  @Test def relationshipsByType() {
    testQuery(
      title = "Match by relationship type and use an identifier",
      text = "If you both want to introduce an identifier to hold the relationship, and specify the relationship type you want, " +
        "just add them both, like this.",
      queryText = """match (wallstreet)<-[r:ACTED_IN]-(actor) where wallstreet.title='Wall Street' return r""",
      returns = """Returns nodes that +ACTED_IN+ Wall Street.""",
      assertions = (p) => assertEquals(3, p.size)
    )
  }

  @Test def relationshipsByTypeWithSpace() {
    db.inTx {
      val a = node("Rob")
      val b = node("Charlie")
      a.createRelationshipTo(b, DynamicRelationshipType.withName("TYPE THAT HAS SPACE IN IT"))
    }
    testQuery(
      title = "Relationship types with uncommon characters",
      text = "Sometime your database will have types with non-letter characters, or with spaces in them. Use +`+ (backtick) to quote these.",
      queryText = """match (n)-[r:`TYPE THAT HAS SPACE IN IT`]->() where n.name='Rob Reiner' return r""",
      returns = """Returns a relationship of a type with spaces in it.""",
      assertions = (p) => assertEquals(1, p.size)
    )
  }

  @Test def multiStepRelationships() {
    testQuery(
      title = "Multiple relationships",
      text = "Relationships can be expressed by using multiple statements in the form of `()--()`, or they can be strung together, " +
        "like this:",
      queryText = """match (charlie)-[:ACTED_IN]->(movie)<-[:DIRECTED]->(director) where charlie.name='Charlie Sheen' return charlie,movie,director""",
      returns = """Returns the three nodes in the path.""",
      assertions = (p) => assertEquals(List(Map("charlie" -> node("Charlie"), "movie" -> node("WallStreet"), "director" -> node("Oliver"))), p.toList)
    )
  }

  @Test def variableLengthPath() {
    testQuery(
      title = "Variable length relationships",
      text = "Nodes that are a variable number of relationship->node hops away can be found using the following syntax: `-[:TYPE*minHops..maxHops]->`. " +
        "minHops and maxHops are optional and default to 1 and infinity respectively. When no bounds are given the dots may be omitted.",
      queryText = """match (martin)-[:ACTED_IN*1..2]-(x) where martin.name='Martin Sheen' return x""",
      returns = "Returns nodes that are 1 or 2 relationships away from Martin.",
      assertions = (p) => assertEquals(Set(node("Charlie"), node("WallStreet"), node("Michael"), node("TheAmericanPresident")), p.columnAs[Node]("x").toSet)
    )
  }

  @Test def variableLengthPathWithIterableRels() {
    testQuery(
      title = "Relationship identifier in variable length relationships",
      text = "When the connection between two nodes is of variable length, " +
        "a relationship identifier becomes an collection of relationships.",
      queryText = """match (actor)-[r:ACTED_IN*2]-(co_actor) where actor.name='Charlie Sheen' return r""",
      returns = "The query returns a collection of relationships.",
      assertions = (p) => assertEquals(2, p.size)
    )
  }

  @Test def zeroLengthPath() {
    testQuery(
      title = "Zero length paths",
      text = "Using variable length paths that have the lower bound zero means that two identifiers can point" +
        " to the same node. If the distance between two nodes is zero, they are by definition the same node. " +
        "Note that when matching zero length paths the result may contain a match even when matching on a relationship type not in use.",
      queryText = """match (wallstreet:Movie)-[*0..1]-(x) where wallstreet.title='Wall Street' return x""",
      returns = "Returns all nodes that are zero or one relationships away from Wall Street.",
      assertions = (p) => assertEquals(Set(node("WallStreet"), node("Charlie"), node("Michael"), node("Martin"), node("Oliver")), p.columnAs[Node]("x").toSet)
    )
  }

  @Test def fixedLengthPath() {
    testQuery(
      title = "Fixed length relationships",
      text = "Elements that are a fixed number of hops away can be matched by using [*numberOfHops]. ",
      queryText = """match michael-[:ACTED_IN*2]-co_actor where michael.name='Michael Douglas' return co_actor.name""",
      returns = "Returns the 2 nodes connected to Michael by a length-2 chain of ACTED_IN relationships.",
      assertions = (p) => assertEquals(Set("Martin Sheen", "Charlie Sheen"), p.columnAs[String]("co_actor.name").toSet)
    )
  }

  @Test def shortestPathBetweenTwoNodes() {
    testQuery(
      title = "Shortest path",
      text = "Finding a single shortest path between two nodes is as easy as using the `shortestPath` function. It's done like this:",
      queryText =
"""match p = shortestPath( (martin:Person)-[*..15]-(oliver:Person) )
where martin.name = 'Martin Sheen' and oliver.name = 'Oliver Stone'
return p""",
      returns = """This means: find a single shortest path between two nodes, as long as the path is max 15 relationships long. Inside of the parenthesis
 you define a single link of a path -- the starting node, the connecting relationship and the end node. Characteristics describing the relationship
 like relationship type, max hops and direction are all used when finding the shortest path. You can also mark the path as optional.""",
      assertions = (p) => assertEquals(2, p.toList.head("p").asInstanceOf[Path].length())
    )
  }

  @Test def allShortestPathsBetweenTwoNodes() {
    testQuery(
      title = "All shortest paths",
      text = "Finds all the shortest paths between two nodes.",
      queryText = """start martin=node(%Martin%), michael=node(%Michael%) match p = allShortestPaths( martin-[*]-michael ) return p""",
      returns = """Finds the two shortest paths between Martin and Michael.""",
      assertions = (p) => assertEquals(2, p.toList.size)
    )
  }

  @Test def introduceNamedPath() {
    testQuery(
      title = "Named path",
      text = "If you want to return or filter on a path in your pattern graph, you can a introduce a named path.",
      queryText = """match p = (michael)-->() where michael.name='Michael Douglas' return p""",
      returns = """Returns the two paths starting from Michael.""",
      assertions = (p) => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def match_on_bound_relationship() {
    testQuery(
      title = "Matching on a bound relationship",
      text = """When your pattern contains a bound relationship, and that relationship pattern doesn't specify direction,
Cypher will try to match the relationship where the connected nodes switch sides.""",
      queryText = """match a-[r]-b where id(r) = 0 return a,b""",
      returns = "This returns the two connected nodes, once as the start node, and once as the end node.",
      assertions = p => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def match_with_labels() {
    testQuery(
      title = "Match with labels",
      text = "To constrain your pattern with labels on nodes, you add it to your pattern nodes, using the label syntax.",
      queryText = "match (charlie:Person)-->(movie:Movie) where charlie.name='Charlie Sheen' return movie",
      returns = "Return any nodes connected with Charlie that are labeled +Movie+.",
      assertions = p => assertEquals(List(node("WallStreet")), p.columnAs[Node]("movie").toList)
    )
  }
}
