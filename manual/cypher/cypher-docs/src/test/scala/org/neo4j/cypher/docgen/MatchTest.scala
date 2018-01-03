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
package org.neo4j.cypher.docgen

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.Test
import org.neo4j.graphdb._
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.visualization.graphviz.{AsciiDocSimpleStyle, GraphStyle}

import scala.collection.JavaConverters._

class MatchTest extends DocumentingTestBase {

  override def graphDescription = List(
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

  override val setupQueries = List("CREATE (r {name : 'Rob Reiner'})-[:`TYPE THAT HAS SPACE IN IT`]->(c {name : 'Charlie Sheen'})")

  override protected def getGraphvizStyle: GraphStyle =
    AsciiDocSimpleStyle.withAutomaticRelationshipTypeColors()

  def section: String = "MATCH"

  @Test def nodes_by_id() {
    testQuery(
      title = "Node by id",
      text = """
Search for nodes by id can be done with the 'id' function in a predicate.

[NOTE]
Neo4j reuses its internal ids when nodes and relationships are deleted.
This means that applications using, and relying on internal Neo4j ids, are brittle or at risk of making mistakes.
Rather use application generated ids.
             """,
      queryText = "match (n) where id(n) = %Charlie% return n",
      optionalResultExplanation = "The corresponding node is returned.",
      assertions = (p) => assertThat(p.columnAs[Node]("n").toList.asJava, hasItem(node("Charlie"))))
  }

  @Test def relationships_by_id() {
    testQuery(
      title = "Relationship by id",
      text = """
Search for nodes by id can be done with the 'id' function in a predicate.

This is not recommended practice. See <<match-node-by-id>> for more information on the use of Neo4j ids.
             """,
      queryText = "match ()-[r]->() where id(r) = 0 return r",
      optionalResultExplanation = "The relationship with id +0+ is returned.",
      assertions = (p) => assertThat(p.columnAs[Relationship]("r").toList.asJava, hasItem(rel(0))))
  }

  @Test def multiple_nodes_by_id() {
    testQuery(
      title = "Multiple nodes by id",
      text = "Multiple nodes are selected by specifying them in an IN clause.",
      queryText = "match (n) where id(n) in [%Charlie%, %Martin%, %Oliver%] return n",
      optionalResultExplanation = "This returns the nodes listed in the `IN` expression.",
      assertions = (p) => assertEquals(Set(node("Charlie"), node("Martin"), node("Oliver")), p.columnAs[Node]("n").toSet))
  }

  @Test def start_with_multiple_nodes() {
    testQuery(
      title = "Multiple identifiers by id",
      text = "Sometimes you want to return multiple nodes by id and separate identifiers. Just list them separated by commas.",
      queryText = "match (a), (b) where id(a) = %Charlie% and id(b) = %Martin% return a, b",
      optionalResultExplanation = """Both the nodes +Charlie+ and the +Martin+  are returned.""",
      assertions = p => assertEquals(List(Map("a" -> node("Charlie"), "b" -> node("Martin"))), p.toList))
  }

  @Test def get_all_nodes() {
    testQuery(
      title = "Get all nodes",
      text = "By just specifying a pattern with a single node and no labels, all nodes in the graph will be returned.",
      queryText = """match (n) return n""",
      optionalResultExplanation = "Returns all the nodes in the database.",
      assertions = (p) => {
        val allNodes: List[Node] = GlobalGraphOperations.at(db).getAllNodes.asScala.toList

        assertEquals(allNodes, p.columnAs[Node]("n").toList)
      }
    )
  }

  @Test def get_multiple_nodes() {
    testQuery(
      title = "Multiple starting points",
      text = "By specifying two node patterns all combinations of the two nodes will be returned, so it is advised to add a constraint.",
      queryText = """match (a), (b) where id(a) = id(b) return a, b""",
      optionalResultExplanation = "Returns the combinations of the two nodes from the database.",
      assertions = (p) => {
        val allNodes: List[Node] = GlobalGraphOperations.at(db).getAllNodes.asScala.toList

        assertEquals(allNodes, p.columnAs[Node]("a").toList)
      }
    )
  }

  @Test def get_all_labeled_nodes() {
    testQuery(
      title = "Get all nodes with a label",
      text = "Getting all nodes with a label on them is done with a single node pattern where the node has a label on it.",
      queryText = """match (movie:Movie) return movie""",
      optionalResultExplanation = "Returns all the movies in the database.",
      assertions = (p) => assertEquals(nodes("WallStreet", "TheAmericanPresident").toSet, p.columnAs[Node]("movie").toSet)
    )
  }

  @Test def allRelationships() {
    testQuery(
      title = "Related nodes",
      text = "The symbol `--` means _related to,_ without regard to type or direction of the relationship.",
      queryText = """match (director {name:'Oliver Stone'})--(movie) return movie.title""",
      optionalResultExplanation = """Returns all the movies directed by Oliver Stone.""",
      assertions = (p) => assertEquals(List("Wall Street"), p.columnAs[Node]("movie.title").toList)
    )
  }

  @Test def allOutgoingRelationships() {
    testQuery(
      title = "Outgoing relationships",
      text = "When the direction of a relationship is interesting, it is shown by using `-->` or `<--`, like this: ",
      queryText = """match (martin {name:'Martin Sheen'})-->(movie) return movie.title""",
      optionalResultExplanation = """Returns nodes connected to Martin by outgoing relationships.""",
      assertions = (p) => assertEquals(Set("Wall Street", "The American President"), p.columnAs[Node]("movie.title").toSet)
    )
  }

  @Test def allOutgoingRelationships2() {
    testQuery(
      title = "Directed relationships and identifier",
      text = "If an identifier is needed, either for filtering on properties of the relationship, or to return the relationship, " +
        "this is how you introduce the identifier.",
      queryText = """match (martin {name:'Martin Sheen'})-[r]->(movie) return r""",
      optionalResultExplanation = """Returns all outgoing relationships from Martin.""",
      assertions = (p) => assertEquals(2, p.size)
    )
  }

  @Test def relatedNodesByRelationshipType() {
    testQuery(
      title = "Match by relationship type",
      text = "When you know the relationship type you want to match on, you can specify it by using a colon together with the relationship type.",
      queryText = """match (wallstreet {title:'Wall Street'})<-[:ACTED_IN]-(actor) return actor""",
      optionalResultExplanation = """Returns nodes that +ACTED_IN+ Wall Street.""",
      assertions = (p) => assertEquals(nodes("Michael","Martin","Charlie").toSet, p.columnAs[Node]("actor").toSet)
    )
  }

  @Test def relatedNodesByMultipleRelationshipTypes() {
    testQuery(
      title = "Match by multiple relationship types",
      text = "To match on one of multiple types, you can specify this by chaining them together with the pipe symbol `|`.",
      queryText = """match (wallstreet {title:'Wall Street'})<-[:ACTED_IN|:DIRECTED]-(person) return person""",
      optionalResultExplanation = """Returns nodes with a +ACTED_IN+ or +DIRECTED+ relationship to Wall Street.""",
      assertions = (p) => assertEquals(nodes("Michael","Martin","Charlie","Oliver").toSet, p.columnAs[Node]("person").toSet)
    )
  }

  @Test def relationshipsByType() {
    testQuery(
      title = "Match by relationship type and use an identifier",
      text = "If you both want to introduce an identifier to hold the relationship, and specify the relationship type you want, " +
        "just add them both, like this.",
      queryText = """match (wallstreet {title:'Wall Street'})<-[r:ACTED_IN]-(actor) return r""",
      optionalResultExplanation = """Returns nodes that +ACTED_IN+ Wall Street.""",
      assertions = (p) => assertEquals(3, p.size)
    )
  }

  @Test def relationshipsByTypeWithSpace() {
    testQuery(
      title = "Relationship types with uncommon characters",
      text = "Sometime your database will have types with non-letter characters, or with spaces in them. Use +`+ (backtick) to quote these.",
      queryText = """match (n {name:'Rob Reiner'})-[r:`TYPE THAT HAS SPACE IN IT`]->() return r""",
      optionalResultExplanation = """Returns a relationship of a type with spaces in it.""",
      assertions = (p) => assertEquals(1, p.size)
    )
  }

  @Test def multiStepRelationships() {
    testQuery(
      title = "Multiple relationships",
      text = "Relationships can be expressed by using multiple statements in the form of `()--()`, or they can be strung together, " +
        "like this:",
      queryText = """match (charlie {name:'Charlie Sheen'})-[:ACTED_IN]->(movie)<-[:DIRECTED]-(director) return charlie,movie,director""",
      optionalResultExplanation = """Returns the three nodes in the path.""",
      assertions = (p) => assertEquals(List(Map("charlie" -> node("Charlie"), "movie" -> node("WallStreet"), "director" -> node("Oliver"))), p.toList)
    )
  }

  @Test def variableLengthPath() {
    testQuery(
      title = "Variable length relationships",
      text = "Nodes that are a variable number of relationship->node hops away can be found using the following syntax: `-[:TYPE*minHops..maxHops]->`. " +
        "minHops and maxHops are optional and default to 1 and infinity respectively. When no bounds are given the dots may be omitted.",
      queryText = """match (martin {name:"Martin Sheen"})-[:ACTED_IN*1..2]-(x) return x""",
      optionalResultExplanation = "Returns nodes that are 1 or 2 relationships away from Martin.",
      assertions = (p) => assertEquals(Set(node("Charlie"), node("WallStreet"), node("Michael"), node("TheAmericanPresident")), p.columnAs[Node]("x").toSet)
    )
  }

  @Test def variableLengthPathWithIterableRels() {
    testQuery(
      title = "Relationship identifier in variable length relationships",
      text = "When the connection between two nodes is of variable length, " +
        "a relationship identifier becomes an collection of relationships.",
      queryText = """match (actor {name:'Charlie Sheen'})-[r:ACTED_IN*2]-(co_actor) return r""",
      optionalResultExplanation = "The query returns a collection of relationships.",
      assertions = (p) => assertEquals(2, p.size)
    )
  }

  @Test def zeroLengthPath() {
    testQuery(
      title = "Zero length paths",
      text = "Using variable length paths that have the lower bound zero means that two identifiers can point" +
        " to the same node. If the distance between two nodes is zero, they are by definition the same node. " +
        "Note that when matching zero length paths the result may contain a match even when matching on a relationship type not in use.",
      queryText = """match (wallstreet:Movie {title:'Wall Street'})-[*0..1]-(x) return x""",
      optionalResultExplanation = "Returns all nodes that are zero or one relationships away from Wall Street.",
      assertions = (p) => assertEquals(Set(node("WallStreet"), node("Charlie"), node("Michael"), node("Martin"), node("Oliver")), p.columnAs[Node]("x").toSet)
    )
  }

  @Test def fixedLengthPath() {
    testQuery(
      title = "Fixed length relationships",
      text = "Elements that are a fixed number of hops away can be matched by using [*numberOfHops]. ",
      queryText = """match (michael {name:'Michael Douglas'})-[:ACTED_IN*2]-(co_actor) return co_actor.name""",
      optionalResultExplanation = "Returns the 2 nodes connected to Michael by a length-2 chain of ACTED_IN relationships.",
      assertions = (p) => assertEquals(Set("Martin Sheen", "Charlie Sheen"), p.columnAs[String]("co_actor.name").toSet)
    )
  }

  @Test def shortestPathBetweenTwoNodes() {
    testQuery(
      title = "Single shortest path",
      text = "Finding a single shortest path between two nodes is as easy as using the `shortestPath` function. It's done like this:",
      queryText ="""
match (martin:Person {name:"Martin Sheen"} ),
      (oliver:Person {name:"Oliver Stone"}),
      p = shortestPath( (martin)-[*..15]-(oliver) )
return p""",
      optionalResultExplanation = """This means: find a single shortest path between two nodes, as long as the path is max 15 relationships long. Inside of the parentheses
 you define a single link of a path -- the starting node, the connecting relationship and the end node. Characteristics describing the relationship
 like relationship type, max hops and direction are all used when finding the shortest path. You can also mark the path as optional.""",
      assertions = (p) => assertEquals(2, p.toList.head("p").asInstanceOf[Path].length())
    )
  }

  @Test def allShortestPathsBetweenTwoNodes() {
    testQuery(
      title = "All shortest paths",
      text = "Finds all the shortest paths between two nodes.",
      queryText = """
match (martin:Person {name:"Martin Sheen"} ),
      (michael:Person {name:"Michael Douglas"}),
      p = allShortestPaths( (martin)-[*]-(michael) ) return p""",
      optionalResultExplanation = """Finds the two shortest paths between Martin and Michael.""",
      assertions = (p) => assertEquals(2, p.toList.size)
    )
  }

  @Test def introduceNamedPath() {
    testQuery(
      title = "Named path",
      text = "If you want to return or filter on a path in your pattern graph, you can a introduce a named path.",
      queryText = """match p = (michael {name:'Michael Douglas'})-->() return p""",
      optionalResultExplanation = """Returns the two paths starting from Michael.""",
      assertions = (p) => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def match_on_bound_relationship() {
    testQuery(
      title = "Matching on a bound relationship",
      text = """When your pattern contains a bound relationship, and that relationship pattern doesn't specify direction,
Cypher will try to match the relationship in both directions.""",
      queryText = """match (a)-[r]-(b) where id(r) = 0 return a,b""",
      optionalResultExplanation = "This returns the two connected nodes, once as the start node, and once as the end node.",
      assertions = p => assertEquals(2, p.toSeq.length)
    )
  }

  @Test def match_with_labels() {
    testQuery(
      title = "Match with labels",
      text = "To constrain your pattern with labels on nodes, you add it to your pattern nodes, using the label syntax.",
      queryText = "match (charlie:Person {name:'Charlie Sheen'})--(movie:Movie) return movie",
      optionalResultExplanation = "Return any nodes connected with the +Person+ Charlie that are labeled +Movie+.",
      assertions = p => assertEquals(List(node("WallStreet")), p.columnAs[Node]("movie").toList)
    )
  }

  @Test def match_with_properties_on_a_variable_length_path() {
    val preparationQuery = """MATCH (charlie:Person {name:'Charlie Sheen'}), (martin:Person {name:'Martin Sheen'})
      CREATE (charlie)-[:X {blocked:false}]->(:Unblocked)<-[:X {blocked:false}]-(martin)
      CREATE (charlie)-[:X {blocked:true}]->(:Blocked)<-[:X {blocked:false}]-(martin)"""

    prepareAndTestQuery(
      prepare = _ => executePreparationQueries(List(preparationQuery)),
      title = "Match with properties on a variable length path",
      text = """A variable length relationship with properties defined on in it means that all relationships in the path
must have the property set to the given value. In this query, there are two paths between Charile Sheen and his
dad Martin Sheen. One of the includes a ``blocked'' relationship and the other doesn't.
In this case we first alter the original graph by using the following query to add ``blocked'' and ``unblocked'' relationships:

include::includes/match-match-with-properties-on-a-variable-length-path.preparation.asciidoc[]

This means that we are starting out with the following graph:

include::includes/match-match-with-properties-on-a-variable-length-path.preparation-graph.asciidoc[]

""",
      queryText = "MATCH p = (charlie:Person)-[* {blocked:false}]-(martin:Person) " +
        "WHERE charlie.name = 'Charlie Sheen' AND martin.name = 'Martin Sheen' " +
        "RETURN p",
      optionalResultExplanation = "Returns the paths between Charlie and Martin Sheen where all relationships have the +blocked+ property set to +FALSE+.",
      assertions = p => {
        val path = p.next()("p").asInstanceOf[Path].asScala

        assert(!path.exists {
          case n: Node => n.hasLabel(DynamicLabel.label("Blocked"))
          case _       => false
        })
      }
    )
  }
}
