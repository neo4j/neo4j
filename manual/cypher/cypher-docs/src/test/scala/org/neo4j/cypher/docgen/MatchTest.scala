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
package org.neo4j.cypher.docgen

import org.neo4j.cypher.docgen.tooling._
import org.neo4j.graphdb.{Path, Node}
import org.neo4j.tooling.GlobalGraphOperations

import scala.collection.JavaConverters._

class MatchTest extends DocumentingTest {
  override def outputPath = "target/docs/dev/ql/"
  override def doc = new DocBuilder {
    doc("Match", "query-match")
    initQueries(
      """CREATE (charlie:Person {name:'Charlie Sheen'}),
        |       (martin:Person {name: 'Martin Sheen'}),
        |       (michael:Person {name: 'Michael Douglas'}),
        |       (oliver:Person {name: 'Oliver Stone'}),
        |       (rob:Person {name: 'Rob Reiner'}),
        |
        |       (wallStreet:Movie {title: 'Wall Street'}),
        |       (charlie)-[:ACTED_IN {role: "Bud Fox"}]->(wallStreet),
        |       (martin)-[:ACTED_IN {role: "Carl Fox"}]->(wallStreet),
        |       (michael)-[:ACTED_IN {role: "Gordon Gekko"}]->(wallStreet),
        |       (oliver)-[:DIRECTED]->(wallStreet),
        |
        |       (thePresident:Movie {title: 'The American President'}),
        |       (martin)-[:ACTED_IN {role: "A.J. MacInerney"}]->(thePresident),
        |       (michael)-[:ACTED_IN {role: "President Andrew Shepherd"}]->(thePresident),
        |       (rob)-[:DIRECTED]->(thePresident)"""
    )
    synopsis("The `MATCH` clause is used to search for the pattern described in it.")
    section("Introduction") {
      p( """The `MATCH` clause allows you to specify the patterns Neo4j will search for in the database.
           |This is the primary way of getting data into the current set of bindings.
           |It is worth reading up more on the specification of the patterns themselves in <<introduction-pattern>>.""")
      p( """`MATCH` is often coupled to a `WHERE` part which adds restrictions, or predicates, to the `MATCH` patterns, making them more specific.
           |The predicates are part of the pattern description, not a filter applied after the matching is done.
           |_This means that `WHERE` should always be put together with the `MATCH` clause it belongs to._""")
      p( """`MATCH` can occur at the beginning of the query or later, possibly after a `WITH`.
           |If it is the first clause, nothing will have been bound yet, and Neo4j will design a search to find the results matching the clause and any associated predicates specified in any `WHERE` part.
           |This could involve a scan of the database, a search for nodes of a certain label, or a search of an index to find starting points for the pattern matching.
           |Nodes and relationships found by this search are available as _bound pattern elements,_ and can be used for pattern matching of sub-graphs.
           |They can also be used in any further `MATCH` clauses, where Neo4j will use the known elements, and from there find further unknown elements.""")
      p( """Cypher is declarative, and so usually the query itself does not specify the algorithm to use to perform the search.
           |Neo4j will automatically work out the best approach to finding start nodes and matching patterns.
           |Predicates in `WHERE` parts can be evaluated before pattern matching, during pattern matching, or after finding matches.
           |However, there are cases where you can influence the decisions taken by the query compiler.
           |Read more about indexes in <<query-schema-index>>, and more about the specifying index hints to force Neo4j to use a specific index in <<query-using>>.""")
      tip {
        p("To understand more about the patterns used in the +MATCH+ clause, read <<introduction-pattern>>")
      }
      p("The following graph is used for the examples below:")
      graphViz()
    }
    section("Basic node finding") {
      section("Get all nodes") {
        p("By just specifying a pattern with a single node and no labels, all nodes in the graph will be returned.")
        query("MATCH (n) RETURN n", assertAllNodesReturned) {
          p("Returns all the nodes in the database.")
          resultTable()
        }
      }
      section("Get all nodes with a label") {
        p("Getting all nodes with a label on them is done with a single node pattern where the node has a label on it.")
        query("MATCH (movie:Movie) RETURN movie.title", assertAllMoviesAreReturned) {
          p("Returns all the movies in the database.")
          resultTable()
        }
      }
      section("Related nodes") {
        p("The symbol `--` means _related to,_ without regard to type or direction of the relationship.")
        query("MATCH (director { name:'Oliver Stone' })--(movie) RETURN movie.title", assertWallStreetIsReturned) {
          p("Returns all the movies directed by Oliver Stone.")
          resultTable()
        }
      }
      section("Match with labels") {
        p("To constrain your pattern with labels on nodes, you add it to your pattern nodes, using the label syntax.")
        query("MATCH (:Person { name:'Oliver Stone' })--(movie:Movie) RETURN movie.title", assertWallStreetIsReturned) {
          p("Return any nodes connected with the +Person+ Oliver that are labeled +Movie+.")
          resultTable()
        }
      }
    }
    section("Relationship basics") {
      section("Outgoing relationships") {
        p("When the direction of a relationship is interesting, it is shown by using `-->` or `<--`, like this:")
        query("MATCH (:Person { name:'Oliver Stone' })-->(movie) RETURN movie.title", assertWallStreetIsReturned) {
          p("Return any nodes connected with the +Person+ Oliver that are labeled +Movie+.")
          resultTable()
        }
      }
      section("Directed relationships and identifier") {
        p("If an identifier is needed, either for filtering on properties of the relationship, or to return the relationship, this is how you introduce the identifier.")
        query("MATCH (:Person { name:'Oliver Stone' })-[r]->(movie) RETURN type(r)", assertRelationshipIsDirected) {
          p("Returns all outgoing relationships from Oliver.")
          resultTable()
        }
      }
      section("Match by relationship type") {
        p("When you know the relationship type you want to match on, you can specify it by using a colon together with the relationship type.")
        query("MATCH (wallstreet:Movie { title:'Wall Street' })<-[:ACTED_IN]-(actor) RETURN actor.name", assertAllActorsOfWallStreetAreFound) {
          p("Returns all outgoing relationships from Oliver.")
          resultTable()
        }
      }
      section("Match by multiple relationship types") {
        p("To match on one of multiple types, you can specify this by chaining them together with the pipe symbol `|`.")
        query("MATCH (wallstreet { title:'Wall Street' })<-[:ACTED_IN|:DIRECTED]-(person) RETURN person.name", assertEveryoneConnectedToWallStreetIsFound) {
          p("Returns nodes with a +ACTED_IN+ or +DIRECTED+ relationship to Wall Street.")
          resultTable()
        }
      }
      section("Match by relationship type and use an identifier") {
        p("If you both want to introduce an identifier to hold the relationship, and specify the relationship type you want, just add them both, like this:")
        query("MATCH (wallstreet { title:'Wall Street' })<-[r:ACTED_IN]-(actor) RETURN r", assertRelationshipsToWallStreetAreReturned) {
          p("Returns nodes that +ACTED_IN+ Wall Street.")
          resultTable()
        }
      }
    }
    section("Relationships in depth") {
      note {
        p("Inside a single pattern, relationships will only be matched once. You can read more about this in <<cypherdoc-uniqueness>>")
      }
      section("Relationship types with uncommon characters") {
        p("Sometime your database will have types with non-letter characters, or with spaces in them. Use +`+ (backtick) to quote these.")
        query("MATCH (n { name:'Rob Reiner' })-[r:`TYPE THAT HAS SPACE IN IT`]->() RETURN count(*)", assertTypeWithRelsExist) {
          resultTable()
        }
      }
      section("Multiple relationships") {
        p("Relationships can be expressed by using multiple statements in the form of `()--()`, or they can be strung together, like this:")
        query("match (charlie {name:'Charlie Sheen'})-[:ACTED_IN]->(movie)<-[:DIRECTED]-(director) return movie.title, director.name", assertFindAllDirectors) {
          p("Returns the three nodes in the path.")
          resultTable()
        }
      }
      section("Variable length relationships") {
        p("Nodes that are a variable number of relationship->node hops away can be found using the following syntax: `-[:TYPE*minHops..maxHops]->`. ")
        query("match (martin {name:'Charlie Sheen'})-[:ACTED_IN*1..3]-(movie:Movie) return movie.title", assertAllMoviesAreReturned) {
          p("Returns the three nodes in the path.")
          resultTable()
        }
      }
      section("Relationship identifier in variable length relationships") {
        p("When the connection between two nodes is of variable length, a relationship identifier becomes an collection of relationships.")
        query("MATCH (actor { name:'Charlie Sheen' })-[r:ACTED_IN*2]-(co_actor)\nRETURN r", assertActedInRelationshipsAreReturned) {
          p("The query returns a collection of relationships.")
          resultTable()
        }
      }
      section("Match with properties on a variable length path") {
        val initQuery =
          """MATCH (charlie:Person {name:'Charlie Sheen'}), (martin:Person {name:'Martin Sheen'})
            |CREATE (charlie)-[:X {blocked:false}]->(:Unblocked)<-[:X {blocked:false}]-(martin)
            |CREATE (charlie)-[:X {blocked:true}]->(:Blocked)<-[:X {blocked:false}]-(martin)"""
        p("""A variable length relationship with properties defined on in it means that all
            |relationships in the path must have the property set to the given value. In this query,
            |there are two paths between Charlie Sheen and his dad Martin Sheen. One of the includes a
            |``blocked'' relationship and the other doesn't. In this case we first alter the original
            |graph by using the following query to add ``blocked'' and ``unblocked'' relationships:""")
        query(initQuery, assertBlockingRelationshipsAdded) {
          p("This means that we are starting out with the following graph: ")
          graphViz()
        }
        query("MATCH p = (charlie:Person)-[* {blocked:false}]-(martin:Person) " +
          "WHERE charlie.name = 'Charlie Sheen' AND martin.name = 'Martin Sheen' " +
          "RETURN p", assertBlockingRelationshipsAdded) {
          initQueries(initQuery)
          resultTable()
          p("Returns the paths between Charlie and Martin Sheen where all relationships have the +blocked+ property set to +FALSE+.")
        }
      }
    }
    section("Shortest path", "query-shortest-path") {
      section("Single shortest path") {
        p("Finding a single shortest path between two nodes is as easy as using the `shortestPath` function. It's done like this:")
        query(
          """MATCH (martin:Person {name:"Martin Sheen"} ),
            |      (oliver:Person {name:"Oliver Stone"}),
            |      p = shortestPath( (martin)-[*..15]-(oliver) )
            |RETURN p""", assertShortestPathLength) {
          resultTable()
          p(
            """This means: find a single shortest path between two nodes, as long as the path is max 15 relationships long. Inside of the parentheses
              | you define a single link of a path -- the starting node, the connecting relationship and the end node. Characteristics describing the relationship
              | like relationship type, max hops and direction are all used when finding the shortest path. You can also mark the path as optional.""")
        }
      }
      section("All shortest paths") {
        p("Finds all the shortest paths between two nodes.")
        query("""match (martin:Person {name:"Martin Sheen"} ),
                |      (michael:Person {name:"Michael Douglas"}),
                |      p = allShortestPaths( (martin)-[*]-(michael) ) return p""", assertAllShortestPaths) {
          p("Finds the two shortest paths between Martin and Michael.")
          resultTable()
        }
      }
    }
    section("Get node or relationship by id") {
      section("Node by id", "match-node-by-id") {
        p("Search for nodes by id can be done with the 'id' function in a predicate.")
        note {
          p("""Neo4j reuses its internal ids when nodes and relationships are deleted.
              |This means that applications using, and relying on internal Neo4j ids, are brittle or at risk of making mistakes.
              |Rather use application generated ids.""")
        }
        query("match (n) where id(n) = 0 return n", assertHasNodes(0)) {
          p("The corresponding node is returned.")
          resultTable()
        }
      }
      section("Relationship by id") {
        p("""
            |Search for relationships by id can be done with the 'id' function in a predicate.
            |
            |This is not recommended practice. See <<match-node-by-id>> for more information on the use of Neo4j ids.
            |""")
        query("match ()-[r]->() where id(r) = 0 return r", assertHasRelationship0) {
          p("The relationship with id +0+ is returned.")
          resultTable()
        }
      }
      section("Multiple nodes by id") {
        p("Multiple nodes are selected by specifying them in an IN clause.")
        query("match (n) where id(n) in [0,3,5] return n", assertHasNodes(0, 3, 5)) {
          p("This returns the nodes listed in the `IN` expression.")
          resultTable()
        }
      }
    }
  }.build()

  private def assertAllNodesReturned = ResultAndDbAssertions((p, db) => {
    val tx = db.beginTx()
    try {
      val allNodes: List[Node] = GlobalGraphOperations.at(db).getAllNodes.asScala.toList
      allNodes should equal(p.columnAs[Node]("n").toList)
    } finally tx.close()
  })

  private def assertAllMoviesAreReturned = ResultAssertions(result =>
    result.toSet should equal(Set(
      Map("movie.title" -> "The American President"),
      Map("movie.title" -> "Wall Street")))
  )

  private def assertBlockingRelationshipsAdded = NoAssertions
  private def assertWallStreetIsReturned = ResultAssertions(result =>
    result.toList should equal(
      List(Map("movie.title" -> "Wall Street")))
  )

  private def assertActedInRelationshipsAreReturned = ResultAssertions(result =>
    result.toSet.size shouldBe 2
  )

  private def assertRelationshipIsDirected = ResultAssertions(result =>
    result.toList should equal(
      List(Map("type(r)" -> "DIRECTED")))
  )

  private def assertAllActorsOfWallStreetAreFound = ResultAssertions(result =>
    result.toSet should equal(Set(
      Map("actor.name" -> "Michael Douglas"),
      Map("actor.name" -> "Martin Sheen"),
      Map("actor.name" -> "Charlie Sheen")))
  )

  private def assertTypeWithRelsExist = ResultAssertions(result =>
    result.toList should equal(List(Map("count(*)" -> 0))))

  private def assertEveryoneConnectedToWallStreetIsFound = ResultAssertions(result =>
    result.toSet should equal(Set(
      Map("person.name" -> "Michael Douglas"),
      Map("person.name" -> "Oliver Stone"),
      Map("person.name" -> "Martin Sheen"),
      Map("person.name" -> "Charlie Sheen")))
  )

  private def assertRelationshipsToWallStreetAreReturned = ResultAssertions(result =>
    result.size should equal(3)
  )

  private def assertFindAllDirectors = ResultAssertions(result =>
    result.toList should equal(
      List(Map("movie.title" -> "Wall Street", "director.name" -> "Oliver Stone"))))

  private def assertShortestPathLength = ResultAssertions(result =>
    result.toList.head("p").asInstanceOf[Path].length() shouldBe 2)

  private def assertAllShortestPaths = ResultAssertions(result =>
    result.toList.size shouldBe 2)


  private def assertHasRelationship0 = ResultAssertions(result =>
     result.toList.size shouldBe 1)

  private def assertHasNodes(nodeIds: Long*) = ResultAssertions(result =>
    result.toList.map(_.head._2.asInstanceOf[Node].getId) should equal(nodeIds.toList)
  )

}
