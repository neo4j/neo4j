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
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{IndexSeekByRange, UniqueIndexSeekByRange}
import org.scalatest.Ignore

class QueryPlanTest extends DocumentingTestBase with SoftReset {
  override val setupQueries = List(
    """CREATE (me:Person {name:'me'})
       CREATE (andres:Person {name:'Andres'})
       CREATE (andreas:Person {name:'Andreas'})
       CREATE (mattias:Person {name:'Mattias'})
       CREATE (lovis:Person {name:'Lovis'})
       CREATE (pontus:Person {name:'Pontus'})
       CREATE (max:Person {name:'Max'})
       CREATE (konstantin:Person {name:'Konstantin'})
       CREATE (stefan:Person {name:'Stefan'})
       CREATE (mats:Person {name:'Mats'})
       CREATE (petra:Person {name:'Petra'})
       CREATE (craig:Person {name:'Craig'})
       CREATE (steven:Person {name:'Steven'})
       CREATE (chris:Person {name:'Chris'})

       CREATE (london:Location {name:'London'})
       CREATE (malmo:Location {name:'Malmo'})
       CREATE (sf:Location {name:'San Francisco'})
       CREATE (berlin:Location {name:'Berlin'})
       CREATE (newyork:Location {name:'New York'})
       CREATE (kuala:Location {name:'Kuala Lumpur'})
       CREATE (stockholm:Location {name:'Stockholm'})
       CREATE (paris:Location {name:'Paris'})
       CREATE (madrid:Location {name:'Madrid'})
       CREATE (rome:Location {name:'Rome'})

       CREATE (england:Country {name:'England'})
       CREATE (field:Team {name:'Field'})
       CREATE (engineering:Team {name:'Engineering'})
       CREATE (sales:Team {name:'Sales'})
       CREATE (monads:Team {name:'Team Monads'})
       CREATE (birds:Team {name:'Team Enlightened Birdmen'})
       CREATE (quality:Team {name:'Team Quality'})
       CREATE (rassilon:Team {name:'Team Rassilon'})
       CREATE (executive:Team {name:'Team Executive'})
       CREATE (remoting:Team {name:'Team Remoting'})
       CREATE (other:Team {name:'Other'})

       CREATE (me)-[:WORKS_IN {duration: 190}]->(london)
       CREATE (andreas)-[:WORKS_IN {duration: 187}]->(london)
       CREATE (andres)-[:WORKS_IN {duration: 150}]->(london)
       CREATE (mattias)-[:WORKS_IN {duration: 230}]->(london)
       CREATE (lovis)-[:WORKS_IN {duration: 230}]->(sf)
       CREATE (pontus)-[:WORKS_IN {duration: 230}]->(malmo)
       CREATE (max)-[:WORKS_IN {duration: 230}]->(newyork)
       CREATE (konstantin)-[:WORKS_IN {duration: 230}]->(london)
       CREATE (stefan)-[:WORKS_IN {duration: 230}]->(london)
       CREATE (stefan)-[:WORKS_IN {duration: 230}]->(berlin)
       CREATE (mats)-[:WORKS_IN {duration: 230}]->(malmo)
       CREATE (petra)-[:WORKS_IN {duration: 230}]->(london)
       CREATE (craig)-[:WORKS_IN {duration: 230}]->(malmo)
       CREATE (steven)-[:WORKS_IN {duration: 230}]->(malmo)
       CREATE (chris)-[:WORKS_IN {duration: 230}]->(madrid)
       CREATE (london)-[:IN]->(england)
       CREATE (me)-[:FRIENDS_WITH]->(andres)
       CREATE (andres)-[:FRIENDS_WITH]->(andreas)
    """.stripMargin)

  override val setupConstraintQueries = List(
    "CREATE INDEX ON :Location(name)".stripMargin,
    "CREATE INDEX ON :Person(name)".stripMargin,
    "CREATE CONSTRAINT ON (team:Team) ASSERT team.name is UNIQUE".stripMargin
  )

  def section = "Query Plan"

  @Test def allNodesScan() {
    profileQuery(
      title = "All Nodes Scan",
      text =
        """Reads all nodes from the node store. The identifier that will contain the nodes is seen in the arguments.
          |If your query is using this operator, you are very likely to see performance problems on any non-trivial database.""".stripMargin,
      queryText = """MATCH (n) RETURN n""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("AllNodesScan"))
    )
  }

  @Test def constraintOperation() {
    profileQuery(
      title = "Constraint Operation",
      text =
        """Creates a constraint on a (label,property) pair.
          |The following query will create a unique constraint on the `name` property of nodes with the `Country` label.""".stripMargin,
      queryText = """CREATE CONSTRAINT ON (c:Country) ASSERT c.name is UNIQUE""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("ConstraintOperation"))
    )
  }

  @Test def distinct() {
    profileQuery(
      title = "Distinct",
      text =
        """Removes duplicate rows from the incoming stream of rows.""".stripMargin,
      queryText = """MATCH (l:Location)<-[:WORKS_IN]-(p:Person) RETURN DISTINCT l""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Distinct"))
    )
  }

  @Test def eagerAggregation() {
    profileQuery(
      title = "Eager Aggregation",
      text =
        """Eagerly loads underlying results and stores it in a hash-map, using the grouping keys as the keys for the map.""".stripMargin,
      queryText = """MATCH (l:Location)<-[:WORKS_IN]-(p:Person) RETURN l.name AS location, COLLECT(p.name) AS people""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("EagerAggregation"))
    )
  }

  @Test def eager() {
    profileQuery(
      title = "Eager",
      text =
        """For isolation purposes this operator makes sure that operations that affect subsequent operations are executed fully for the whole dataset before continuing execution. 
           | Otherwise it could trigger endless loops, matching data again, that was just created.
           | The Eager operator can cause high memory usage when importing data or migrating graph structures.
           | In such cases split up your operations into simpler steps e.g. you can import nodes and relationships separately. 
           | Alternatively return the records to be updated and run an update statement afterwards.""".stripMargin,
      queryText = """MATCH (p:Person) MERGE (:Person:Clone {name:p.name})""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Eager"))
    )
  }

  @Test def updateGraph() {
    profileQuery(
      title = "Update Graph",
      text =
        """Applies updates to the graph.""".stripMargin,
      queryText = """CREATE (:Person {name: "Alistair"})""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("UpdateGraph"))
    )
  }

  @Test def mergeInto() {
    profileQuery(
      title = "Merge Into",
      text =
        """When both the start and end node have already been found, merge-into is used to find all connecting relationships or creating a new relationship between the two nodes.""".stripMargin,
      queryText = """MATCH (p:Person {name: "me"}), (f:Person {name: "Andres"}) MERGE (p)-[:FRIENDS_WITH]->(f)""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Merge(Into)"))
    )
  }

  @Test def emptyResult() {
    profileQuery(
      title = "Empty Result",
      text =
        """Eagerly loads everything coming in to the EmptyResult operator and discards it.""".stripMargin,
      queryText = """CREATE (:Person)""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("EmptyResult"))
    )
  }

  @Test def nodeByLabelScan() {
    profileQuery(
      title = "Node by label scan",
      text = """Using the label index, fetches all nodes with a specific label on them from the node label index.""".stripMargin,
      queryText = """MATCH (person:Person) RETURN person""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("LabelScan"))
    )
  }

  @Test def nodeByIndexSeek() {
    profileQuery(
      title = "Node index seek",
      text = """Finds nodes using an index seek. The node identifier and the index used is shown in the arguments of the operator.
                |If the index is a unique index, the operator is called NodeUniqueIndexSeek instead.""".stripMargin,
      queryText = """MATCH (location:Location {name: "Malmo"}) RETURN location""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeIndexSeek"))
    )
  }

  @Test def nodeIndexRangeSeek() {
    executePreparationQueries {
      val a = (0 to 100).map { i => "CREATE (:Location)" }.toList
      val b = (0 to 300).map { i => s"CREATE (:Location {name: '$i'})" }.toList
      a ++ b
    }

    sampleAllIndicesAndWait()

    profileQuery(title = "Node index range seek",
                 text =
                   """Finds nodes using an index seek where the value of the property matches a given prefix string.
                     |This operator can be used for +STARTS WITH+ and comparators such as `<`, `>`, `<=` and `>=`""".stripMargin,
                 queryText = "MATCH (l:Location) WHERE l.name STARTS WITH 'Lon' RETURN l",
                 assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString(IndexSeekByRange.name))
    )
  }

  @Test def nodeIndexScan() {
    executePreparationQueries((0 to 250).map { i =>
      "CREATE (:Location)"
    }.toList)
    profileQuery(title = "Node index scan",
                 text = """
                          |An index scan goes through all values stored in an index, and can be used to find all nodes with a particular label having a specified property (e.g. `exists(n.prop)`).""".stripMargin,
                 queryText = "MATCH (l:Location) WHERE has(l.name) RETURN l",
                 assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeIndexScan"))
    )
  }

  @Test def nodeByIdSeek() {
    profileQuery(
      title = "Node by Id seek",
      text =
        """Reads one or more nodes by id from the node store.""".stripMargin,
      queryText = """MATCH (n) WHERE id(n) = 0 RETURN n""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeByIdSeek"))
    )
  }

  @Test def projection() {
    profileQuery(
      title = "Projection",
      text =
        """For each row from its input, projection evaluates a set of expressions and produces a row with the results of the expressions.""".stripMargin,
      queryText = """RETURN "hello" AS greeting""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Projection"))
    )
  }

  @Test def filter() {
    profileQuery(
      title = "Filter",
      text =
        """Filters each row coming from the child operator, only passing through rows that evaluate the predicates to `TRUE`.""".stripMargin,
      queryText = """MATCH (p:Person) WHERE p.name =~ "^a.*" RETURN p""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Filter"))
    )
  }

  @Test def cartesianProduct() {
    profileQuery(
      title = "Cartesian Product",
      text =
        """Produces a cartesian product of the two inputs -- each row coming from the left child will be combined with all the rows from the right child operator.""".stripMargin,
      queryText = """MATCH (p:Person), (t:Team) RETURN p, t""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("CartesianProduct"))
    )
  }

  @Test def optionalExpand() {
    profileQuery(
      title = "Optional Expand All",
      text =
        """Optional expand traverses relationships from a given node, and makes sure that predicates are evaluated before producing rows.
          |
          |If no matching relationships are found, a single row with `NULL` for the relationship and end node identifier is produced.""".stripMargin,
      queryText =
        """MATCH (p:Person)
           OPTIONAL MATCH (p)-[works_in:WORKS_IN]->(l) WHERE works_in.duration > 180
           RETURN p, l""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("OptionalExpand(All)"))
    )
  }

  @Test def sort() {
    profileQuery(
      title = "Sort",
      text =
        """Sorts rows by a provided key.""".stripMargin,
      queryText = """MATCH (p:Person) RETURN p ORDER BY p.name""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Sort"))
    )
  }

  @Test def top() {
    profileQuery(
      title = "Top",
      text =
        """Returns the first 'n' rows sorted by a provided key. The physical operator is called `Top`. Instead of sorting the whole input, only the top X rows are kept.""".stripMargin,
      queryText = """MATCH (p:Person) RETURN p ORDER BY p.name LIMIT 2""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Top"))
    )
  }

  @Test def limit() {
    profileQuery(
      title = "Limit",
      text =
        """Returns the first 'n' rows from the incoming input.""".stripMargin,
      queryText = """MATCH (p:Person) RETURN p LIMIT 3""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Limit"))
    )
  }

  @Test def expandAll() {
    profileQuery(
      title = "Expand All",
      text =
        """Given a start node, expand-all will follow relationships coming in or out, depending on the pattern relationship. Can also handle variable length pattern relationships.""".stripMargin,
      queryText = """MATCH (p:Person {name: "me"})-[:FRIENDS_WITH]->(fof) RETURN fof""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Expand(All)"))
    )
  }

  @Test def expandInto() {
    profileQuery(
      title = "Expand Into",
      text =
        """When both the start and end node have already been found, expand-into is used to find all connecting relationships between the two nodes.""".stripMargin,
      queryText = """MATCH (p:Person {name: "me"})-[:FRIENDS_WITH]->(fof)-->(p) RETURN fof""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Expand(Into)"))
    )
  }

  @Test def semiApply() {
    profileQuery(
      title = "Semi Apply",
      text =
        """Tests for the existence of a pattern predicate.
          |`SemiApply` takes a row from it's child operator and feeds it to the `Argument` operator on the right hand side of `SemiApply`.
          |If the right hand side operator tree yields at least one row, the row from the left hand side is yielded by the `SemiApply` operator.
          |This makes `SemiApply` a filtering operator, used mostly for pattern predicates in queries.""".stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE (other)-[:FRIENDS_WITH]->()
           RETURN other""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("SemiApply"))
    )
  }

  @Test def letSemiApply() {
    profileQuery(
      title = "Let Semi Apply",
      text =
        """Tests for the existence of a pattern predicate.
          |When a query contains multiple pattern predicates `LetSemiApply` will be used to evaluate the first of these.
          |It will record the result of evaluating the predicate but will leave any filtering to a another operator.
          |The following query will find all the people who have a friend or who work somewhere. The `LetSemiApply` operator
          |will be used to check for the existence of the `FRIENDS_WITH` relationship from each person.
        """.stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE (other)-[:FRIENDS_WITH]->() OR (other)-[:WORKS_IN]->()
           RETURN other""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("LetSemiApply"))
    )
  }

  @Test def letAntiSemiApply() {
    profileQuery(
      title = "Let Anti Semi Apply",
      text =
        """Tests for the absence of a pattern predicate.
          |When a query contains multiple pattern predicates `LetSemiApply` will be used to evaluate the first of these.
          |It will record the result of evaluating the predicate but will leave any filtering to another operator.
          |The following query will find all the people who don't have anyfriend or who work somewhere. The `LetSemiApply` operator
          |will be used to check for the absence of the `FRIENDS_WITH` relationship from each person.
        """.stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE NOT((other)-[:FRIENDS_WITH]->()) OR (other)-[:WORKS_IN]->()
           RETURN other""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("LetAntiSemiApply"))
    )
  }


  @Test def selectOrSemiApply() {
    profileQuery(
      title = "Select Or Semi Apply",
      text =
        """Tests for the existence of a pattern predicate and evaluates a predicate.
          |This operator allows for the mixing of normal predicates and pattern predicates that check for the existing of a pattern.
          |First the normal expression predicate is evaluated, and only if it returns `FALSE` the costly pattern predicate evaluation is performed.""".stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE other.age > 25 OR (other)-[:FRIENDS_WITH]->()
           RETURN other""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("SelectOrSemiApply"))
    )
  }

  @Test def antiSemiApply() {
    profileQuery(
      title = "Anti Semi Apply",
      text =
        """Tests for the absence of a pattern predicate.
          |A pattern predicate that is prepended by `NOT` is solved with `AntiSemiApply`.""".stripMargin,
      queryText =
        """MATCH (me:Person {name: "me"}), (other:Person)
           WHERE NOT (me)-[:FRIENDS_WITH]->(other)
           RETURN other""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("AntiSemiApply"))
    )
  }

  @Test def selectOrAntiSemiApply() {
    profileQuery(
      title = "Select Or Anti Semi Apply",
      text =
        """Tests for the absence of a pattern predicate and evaluates a predicate.""".stripMargin,
      queryText =
        """MATCH (other:Person)
           WHERE other.age > 25 OR NOT (other)-[:FRIENDS_WITH]->()
           RETURN other""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("SelectOrAntiSemiApply"))
    )
  }

  @Test def directedRelationshipById() {
    profileQuery(
      title = "Directed Relationship By Id Seek",
      text =
        """Reads one or more relationships by id from the relationship store. Produces both the relationship and the nodes on either side.""".stripMargin,
      queryText =
        """MATCH (n1)-[r]->()
           WHERE id(r) = 0
           RETURN r, n1
        """.stripMargin,
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("DirectedRelationshipByIdSeek"))
    )
  }

  @Test def undirectedRelationshipById() {
    profileQuery(
      title = "Undirected Relationship By Id Seek",
      text =
        """Reads one or more relationships by id from the relationship store.
          |For each relationship, two rows are produced with start and end nodes arranged differently.""".stripMargin,
      queryText =
        """MATCH (n1)-[r]-()
           WHERE id(r) = 1
           RETURN r, n1
        """.stripMargin,
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("UndirectedRelationshipByIdSeek"))
    )
  }

  @Test def nodeHashJoin() {
    executePreparationQueries((0 to 250).map { i =>
      """MATCH (london:Location {name: 'London'}), (person:Person {name: 'Pontus'})
        |FOREACH(x in range(0,250) |
        |  CREATE (person)-[:WORKS_IN]->(london)
        |)""".stripMargin

    }.toList)

    profileQuery(
      title = "Node Hash Join",
      text =
        """Using a hash table, a node hash join joins the inputs coming from the left with the inputs coming from the right. The join key is specified in the arguments of the operator.""".stripMargin,
      queryText =
        """MATCH (andy:Person {name:'Andreas'})-[:WORKS_IN]->(loc)<-[:WORKS_IN]-(matt:Person {name:'Mattis'})
           RETURN loc""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("NodeHashJoin"))
    )
  }

  @Test def triadic() {
    profileQuery(
      title = "Triadic",
      text =
        """Triadic is used to solve triangular queries, such as the very common "find my friend-of-friends that are not already my friend".
          |It does so by putting all the "friends" in a set, and use that set to check if the friend-of-friends are already connected to me.
        """.stripMargin,
      queryText =
        """MATCH (me:Person)-[:FRIENDS_WITH]-()-[:FRIENDS_WITH]-(other) WHERE NOT (me)-[:FRIENDS_WITH]-(other)
           RETURN other""",
      assertions = (p) => {
        assertThat(p.executionPlanDescription().toString, containsString("Triadic"))
      }
    )
  }

  @Test def skip() {
    profileQuery(
      title = "Skip",
      text =
        """Skips 'n' rows from the incoming rows
        """.stripMargin,
      queryText =
        """MATCH (p:Person)
           RETURN p
           ORDER BY p.id
           SKIP 1
        """.stripMargin,
      assertions = (p) =>  assertThat(p.executionPlanDescription().toString, containsString("Skip"))
    )
  }

  @Test def apply() {
    profileQuery(
      title = "Apply",
      text =
        """Apply works by performing a nested loop.
          |Every row being produced on the left hand side of the Apply operator will be fed to the Argument operator on the right hand side, and then Apply will yield the results coming from the RHS.
          |Apply, being a nested loop, can be seen as a warning that a better plan was not found.""".stripMargin,
      queryText =
        """MATCH (p:Person)-[:FRIENDS_WITH]->(f)
           WITH p, count(f) as fs
           WHERE fs > 0
           OPTIONAL MATCH (p)-[:WORKS_IN*1..2]->(city)
           RETURN p, city
        """.stripMargin,
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Apply"))
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
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("Union"))
    )
  }

  @Test def unwind() {
    profileQuery(
      title = "Unwind",
      text =
        """Takes a collection of values and returns one row per item in the collection.""".stripMargin,
      queryText = """UNWIND range(1,5) as value return value;""",
      assertions = (p) => assertThat(p.executionPlanDescription().toString, containsString("UNWIND"))
    )
  }
}
