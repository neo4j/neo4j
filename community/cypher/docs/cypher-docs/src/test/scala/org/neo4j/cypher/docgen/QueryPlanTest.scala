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
package org.neo4j.cypher.docgen

import org.junit.Test
import org.junit.Assert._
import org.hamcrest.CoreMatchers._
import org.scalatest.Ignore

class QueryPlanTest extends DocumentingTestBase {
  override val setupQueries = List(
    """CREATE (me:Person {name:'me'})
       CREATE (andres:Person {name:'Andres'})
       CREATE (andreas:Person {name:'Andreas'})
       CREATE (mattis:Person {name:'Mattis'})
       CREATE (Lovis:Person {name:'Lovis'})

       CREATE (london:Location {name:'London'})

       CREATE (england:Country {name:'England'})
       CREATE (field:Team {name:'Field'})
       CREATE (engineering:Team {name:'Engineering'})

       CREATE (me)-[:WORKS_IN {duration: 190}]->(london)
       CREATE (andreas)-[:WORKS_IN {duration: 187}]->(london)
       CREATE (andres)-[:WORKS_IN {duration: 150}]->(london)
       CREATE (mattis)-[:WORKS_IN {duration: 230}]->(london)
       CREATE (london)-[:IN]->(england)
       CREATE (me)-[:FRIENDS_WITH]->(andres)
       CREATE (andres)-[:FRIENDS_WITH]->(andreas)
    """.stripMargin)

  override val setupConstraintQueries = List(
    "CREATE INDEX ON :Location(name)".stripMargin,
    "CREATE CONSTRAINT ON (team:Team) ASSERT team.name is UNIQUE".stripMargin
  )

  def section = "Query Plan"

  @Test def allNodesScan() {
    profileQuery(
      title = "All Nodes Scan",
      text =
        """Reads all nodes from the node store. The identifier that will contain the nodes is seen in the arguments.
          |The following query will return all nodes. It's not a good idea to run a query like this on a production database.""".stripMargin,
      queryText = """MATCH (n) RETURN n""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("AllNodesScan"))
    )
  }

  @Test def nodeByLabelScan() {
    profileQuery(
      title = "Node by label scan",
      text = """Using the label index, fetches all nodes with a specific label on them.
                |The following query will return all nodes which have label `Person` where the property `name` has the value "me" via a scan of the `Person` label index.""".stripMargin,
      queryText = """MATCH (person:Person {name: "me"}) RETURN person""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("LabelScan"))
    )
  }

  @Test def nodeByIndexSeek() {
    profileQuery(
      title = "Node index seek",
      text = """Finds nodes using an index seek. The node identifier and the index used is shown in the arguments of the operator.
                |The following query will return all nodes which have label `Location` where the property `name` has the value "Malmo" using the `Location` index.""".stripMargin,
      queryText = """MATCH (location:Location {name: "Malmo"}) RETURN location""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeIndexSeek"))
    )
  }

  @Test def nodeByUniqueIndexSeek() {
    profileQuery(
      title = "Node unique index seek",
      text =
        """Finds nodes using an index seek on a unique index. The node identifier and the index used is shown in the arguments of the operator.
          |The following query will return all nodes which have the label `Team` where the property `name` has the value "Field" using the `Team` unique index.""".stripMargin,
      queryText = """MATCH (team:Team {name: "Field"}) RETURN team""".stripMargin,
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeUniqueIndexSeek"))
    )
  }

  @Test def nodeByIdSeek() {
    profileQuery(
      title = "Node by Id seek",
      text =
        """Reads one or more nodes by id from the node store.
          |The following query will return the node which has nodeId `0`.""".stripMargin,
      queryText = """MATCH n WHERE id(n) = 0 RETURN n""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeByIdSeek"))
    )
  }

  @Test def projection() {
    profileQuery(
      title = "Projection",
      text =
        """For each row from its input, projection executes a set of expressions and produces a row with the results of the expressions.
          |The following query will produce one row with the value "hello".""".stripMargin,
      queryText = """RETURN "hello" AS greeting""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, startsWith("Projection"))
    )
  }

  @Test def selection() {
    profileQuery(
      title = "Selection",
      text =
        """Filters each row coming from the child operator, only passing through rows that evaluate the predicates to `TRUE`.
          |The following query will look for nodes with the label `Person` and filter those whose name begins with the letter `a`.""".stripMargin,
      queryText = """MATCH (p:Person) WHERE p.name =~ "^a.*" RETURN p""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Filter"))
    )
  }

  @Test def cartesianProduct() {
    profileQuery(
      title = "Cartesian Product",
      text =
        """Produces a cross product of the two inputs -- each row coming from the left child will be matched with all the rows from the right child operator.
          |The following query will join all the people with all the locations and return the cartesian product of the nodes with those labels.
        """.stripMargin,
      queryText = """MATCH (p:Person), (l:Location) RETURN p, l""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("CartesianProduct"))
    )
  }

//  @Test def optionalMatch() {
//    profileQuery(
//      title = "Optional",
//      text =
//        """Takes the input from it's child and passes it on. If the input is empty, a single empty row is generated instead.
//          |The following query will find all the people and the location they work in if there is one.
//        """.stripMargin,
//      queryText = """MATCH (p:Person) OPTIONAL MATCH (p)-[:WORKS_IN]->(l) RETURN p, l""",
//      assertion = (p) => {
//        val string = p.executionPlanDescription().toString
//        println(string)
//        assertThat(string, containsString("Optional"))
//      }
//    )
//  }

  @Test def optionalExpand() {
    profileQuery(
      title = "Optional Expand",
      text =
        """Optional expand traverses relationships from a given node, and makes sure that predicates are evaluated before producing rows.
          |
          |If no matching relationships are found, a single row with `NULL` for the relationship and end node identifier is produced.
          |
          |The following query will find all the people and the location they work in as long as they've worked there for more than 180 days.
        """.stripMargin,
      queryText =
        """MATCH (p:Person)
           OPTIONAL MATCH (p)-[works_in:WORKS_IN]->(l) WHERE works_in.duration > 180
           RETURN p, l""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("OptionalExpand"))
    )
  }

  @Test def sort() {
    profileQuery(
      title = "Sort",
      text =
        """Sorts rows by a provided key.
          |
          |The following query will find all the people and return them sorted alphabetically by name.
        """.stripMargin,
      queryText = """MATCH (p:Person) RETURN p ORDER BY p.name""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Sort"))
    )
  }

  @Test def sortedLimit() {
    profileQuery(
      title = "Sorted Limit",
      text =
        """Returns the first 'n' rows sorted by a provided key. The physical operator is called `Top`. Instead of sorting the whole input, only the top X rows are kept.
          |
          |The following query will find the first 2 people sorted alphabetically by name.
        """.stripMargin,
      queryText = """MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 2""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Top"))
    )
  }

  @Test def limit() {
    profileQuery(
      title = "Limit",
      text =
        """Returns the first 'n' rows.
          |
          |The following query will return the first 3 people in an arbitrary order.
        """.stripMargin,
      queryText = """MATCH (p:Person) RETURN p LIMIT 3""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Limit"))
    )
  }

  @Test def expand() {
    profileQuery(
      title = "Expand",
      text =
        """Given a start node, expand will follow relationships coming in or out, depending on the pattern relationship. Can also handle variable length pattern relationships.
          |
          |The following query will return my friends of friends.
        """.stripMargin,
      queryText = """MATCH (p:Person {name: "me"})-[:FRIENDS_WITH]->(fof) RETURN fof""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Expand"))
    )
  }

  @Test def semiApply() {
    profileQuery(
      title = "Semi Apply",
      text =
        """Tests for the existence of a pattern predicate.
          |`SemiApply` takes a row from it's child operator and feeds it to the `Argument` operator on the right hand side of `SemiApply`.
          |If the right hand side operator tree yields at least one row, the row from the left hand side is yielded by the `SemiApply` operator.
          |This makes `SemiApply` a filtering operator, used mostly for pattern predicates in queries.
          |
          |The following query will find all the people who have a friend.
        """.stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE (other)-[:FRIENDS_WITH]->()
           RETURN other""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("SemiApply"))
    )
  }

  @Test def selectOrSemiApply() {
    profileQuery(
      title = "Select Or Semi Apply",
      text =
        """Tests for the existence of a pattern predicate and evaluates a predicate.
          |This operator allows for the mixing of normal predicates and pattern predicates that check for the existing of a pattern.
          |First the normal expression predicate is evaluated, and only if it returns `FALSE` the costly pattern predicate evaluation is performed.
          |
          |The following query will find all the people who have a friend or are older than 25.
        """.stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE other.age > 25 OR (other)-[:FRIENDS_WITH]->()
           RETURN other""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("SelectOrSemiApply"))
    )
  }

  @Test def antiSemiApply() {
    profileQuery(
      title = "Anti Semi Apply",
      text =
        """Tests for the absence of a pattern predicate.
          |A pattern predicate that is prepended by `NOT` is solved with `AntiSemiApply`.
          |
          |The following query will find all the people who aren't my friend.
        """.stripMargin,
      queryText =
        """MATCH (me:Person {name: "me"}), (other:Person)
           WHERE NOT (me)-[:FRIENDS_WITH]->(other)
           RETURN other""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("AntiSemiApply"))
    )
  }

  @Test def selectOrAntiSemiApply() {
    profileQuery(
      title = "Select Or Anti Semi Apply",
      text =
        """Tests for the absence of a pattern predicate and evaluates a predicate.
          |
          |The following query will find all the people who don't have a friend or are older than 25.
        """.stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE other.age > 25 OR NOT (other)-[:FRIENDS_WITH]->()
           RETURN other""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("SelectOrAntiSemiApply"))
    )
  }

  @Test def directedRelationshipById() {
    profileQuery(
      title = "Directed Relationship By Id Seek",
      text =
        """Reads one or more relationships by id from the relationship store. Produces both the relationship and the nodes on either side.
          |
          |The following query will find the relationship with id `0` and will return a row for the source node of that relationship.
        """.stripMargin,
      queryText =
        """MATCH (n1)-[r]->()
           WHERE id(r) = 0
           RETURN r, n1
        """.stripMargin,
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("DirectedRelationshipByIdSeek"))
    )
  }

  @Test def undirectedRelationshipById() {
    profileQuery(
      title = "Undirected Relationship By Id Seek",
      text =
        """Reads one or more relationships by id from the relationship store.
          |For each relationship, two rows are produced with start and end nodes arranged differently.
          |
          |The following query will find the relationship with id `1` and will return a row for both the source and destination nodes of that relationship.
        """.stripMargin,
      queryText =
        """MATCH (n1)-[r]-()
           WHERE id(r) = 1
           RETURN r, n1
        """.stripMargin,
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("UndirectedRelationshipByIdSeek"))
    )
  }

  @Ignore def nodeHashJoin() {
    profileQuery(
      title = "Node Hash Join",
      text =
        """Using a hash table, a node hash join joins the inputs coming from the left with the inputs coming from the right. The join key is specific in the arguments of the operator.
          |
          |The following query will find the people who work in London and the country which London belongs to.
        """.stripMargin,
      queryText =
        """MATCH (andreas:Person {name:'Andreas'})-[:WORKS_IN]->(location)<-[:WORKS_IN]-(mattis:Person {name:'Mattis'})
           RETURN location""",
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeHashJoin"))
    )
  }

  @Test def skip() {
    profileQuery(
      title = "Skip",
      text =
        """Skips 'n' rows
          |
          |The following query will skip the person with the lowest `id` property and return the rest.
        """.stripMargin,
      queryText =
        """MATCH (p:Person)
           RETURN p
           ORDER BY p.id
           SKIP 1
        """.stripMargin,
      assertion = (p) =>  assertThat(p.executionPlanDescription().toString, containsString("Skip"))
    )
  }

  //24-11-2014: Davide - Ignored since we disabled varlength planning in 2.2M01 release (TODO: reenable it asap)
  @Ignore def apply() {
    profileQuery(
      title = "Apply",
      text =
        """Apply works by performing a nested loop.
          |Every row being produced on the left hand side of the Apply operator will be fed to the Argument operator on the right hand side, and then Apply will yield the results coming from the RHS.
          |Apply, being a nested loop, can be seen as a warning that a better plan was not found.
          |
          |The following query will return all the people that have at least one friend and the city they work in.
        """.stripMargin,
      queryText =
        """MATCH (p:Person)-[:FRIENDS_WITH]->(f)
           WITH p, count(f) as fs
           WHERE fs > 0
           OPTIONAL MATCH (p)-[:WORKS_IN*1..2]->(city)
           RETURN p, city
        """.stripMargin,
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Apply"))
    )
  }

  @Test def union() {
    profileQuery(
      title = "Union",
      text =
        "Union concatenates the results from the right plan after the results of the left plan.",
      queryText =
        """MATCH (p:Location)
           RETURN p.name
           UNION ALL
           MATCH (p:Country)
           RETURN p.name
        """.stripMargin,
      assertion = (p) => assertThat(p.executionPlanDescription().toString, containsString("Union"))
    )
  }
}
