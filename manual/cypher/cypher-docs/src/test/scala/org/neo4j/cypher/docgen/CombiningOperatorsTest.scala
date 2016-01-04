/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

class CombiningOperatorsTest extends DocumentingTest {
  override def outputPath = "target/docs/dev/execution-plan-groups/"
  override def doc = new DocBuilder {
    doc("Combining operators", "combining-operators")
    initQueries("CREATE CONSTRAINT ON (team:Team) ASSERT team.name is UNIQUE",
                "CREATE CONSTRAINT ON (team:Team) ASSERT team.id is UNIQUE",
                "CREATE INDEX ON :Location(name)",
                "CREATE INDEX ON :Person(name)",
              """CREATE (me:Person {name:'me'})
                |CREATE (andres:Person {name:'Andres'})
                |CREATE (andreas:Person {name:'Andreas'})
                |CREATE (mattias:Person {name:'Mattias'})
                |CREATE (lovis:Person {name:'Lovis'})
                |CREATE (pontus:Person {name:'Pontus'})
                |CREATE (max:Person {name:'Max'})
                |CREATE (konstantin:Person {name:'Konstantin'})
                |CREATE (stefan:Person {name:'Stefan'})
                |CREATE (mats:Person {name:'Mats'})
                |CREATE (petra:Person {name:'Petra'})
                |CREATE (craig:Person {name:'Craig'})
                |CREATE (steven:Person {name:'Steven'})
                |CREATE (chris:Person {name:'Chris'})
                |
                |CREATE (london:Location {name:'London'})
                |CREATE (malmo:Location {name:'Malmo'})
                |CREATE (sf:Location {name:'San Francisco'})
                |CREATE (berlin:Location {name:'Berlin'})
                |CREATE (newyork:Location {name:'New York'})
                |CREATE (kuala:Location {name:'Kuala Lumpur'})
                |CREATE (stockholm:Location {name:'Stockholm'})
                |CREATE (paris:Location {name:'Paris'})
                |CREATE (madrid:Location {name:'Madrid'})
                |CREATE (rome:Location {name:'Rome'})
                |
                |CREATE (england:Country {name:'England'})
                |CREATE (field:Team {name:'Field'})
                |CREATE (engineering:Team {name:'Engineering', id: 42})
                |CREATE (sales:Team {name:'Sales'})
                |CREATE (monads:Team {name:'Team Monads'})
                |CREATE (birds:Team {name:'Team Enlightened Birdmen'})
                |CREATE (quality:Team {name:'Team Quality'})
                |CREATE (rassilon:Team {name:'Team Rassilon'})
                |CREATE (executive:Team {name:'Team Executive'})
                |CREATE (remoting:Team {name:'Team Remoting'})
                |CREATE (other:Team {name:'Other'})
                |
                |CREATE (me)-[:WORKS_IN {duration: 190}]->(london)
                |CREATE (andreas)-[:WORKS_IN {duration: 187}]->(london)
                |CREATE (andres)-[:WORKS_IN {duration: 150}]->(london)
                |CREATE (mattias)-[:WORKS_IN {duration: 230}]->(london)
                |CREATE (lovis)-[:WORKS_IN {duration: 230}]->(sf)
                |CREATE (pontus)-[:WORKS_IN {duration: 230}]->(malmo)
                |CREATE (max)-[:WORKS_IN {duration: 230}]->(newyork)
                |CREATE (konstantin)-[:WORKS_IN {duration: 230}]->(london)
                |CREATE (stefan)-[:WORKS_IN {duration: 230}]->(london)
                |CREATE (stefan)-[:WORKS_IN {duration: 230}]->(berlin)
                |CREATE (mats)-[:WORKS_IN {duration: 230}]->(malmo)
                |CREATE (petra)-[:WORKS_IN {duration: 230}]->(london)
                |CREATE (craig)-[:WORKS_IN {duration: 230}]->(malmo)
                |CREATE (steven)-[:WORKS_IN {duration: 230}]->(malmo)
                |CREATE (chris)-[:WORKS_IN {duration: 230}]->(madrid)
                |CREATE (london)-[:IN]->(england)
                |CREATE (me)-[:FRIENDS_WITH]->(andres)
                |CREATE (andres)-[:FRIENDS_WITH]->(andreas)"""
    )
    synopsis("The combining operators are used to piece together other operators.")
    p("The following graph is used for the examples below:")
    graphViz()
    section("Apply") {
      p("""`Apply` works by performing a nested loop.
           |Every row being produced on the left-hand side of the `Apply` operator will be fed to the leaf
           |operator on the right-hand side, and then `Apply` will yield the combined results.
           |`Apply`, being a nested loop, can be seen as a warning that a better plan was not found.""")
      query("""MATCH (p:Person)-[:FRIENDS_WITH]->(f)
               |WITH p, count(f) as fs
               |WHERE fs > 2
               |OPTIONAL MATCH (p)-[:WORKS_IN]->(city)
               |RETURN city.name""", assertPlanContains("Apply")) {
        p("Finds all the people with more than two friends and returns the city they work in.")
        executionPlan()
      }
    }
    section("SemiApply") {
      p("""Tests for the existence of a pattern predicate.
           |`SemiApply` takes a row from its child operator and feeds it to the leaf operator on the right-hand side.
           |If the right-hand side operator tree yields at least one row, the row from the
           |left-hand side is yielded by the `SemiApply` operator.
           |This makes `SemiApply` a filtering operator, used mostly for pattern predicates in queries.""")
      query("""MATCH (p:Person)
               |WHERE (p)-[:FRIENDS_WITH]->()
               |RETURN p.name""", assertPlanContains("SemiApply")) {
        p("Finds all the people who have friends.")
        executionPlan()
      }
    }
    section("AntiSemiApply") {
      p("""Tests for the existence of a pattern predicate.
           |`SemiApply` takes a row from its child operator and feeds it to the leaf operator on the right-hand side.
           |If the right-hand side operator tree yields at least one row, the row from the
           |left-hand side is yielded by the `SemiApply` operator.
           |This makes `SemiApply` a filtering operator, used mostly for pattern predicates in queries.""")
      query("""MATCH (me:Person {name: "me"}), (other:Person)
               |WHERE NOT (me)-[:FRIENDS_WITH]->(other)
               |RETURN other.name""", assertPlanContains("AntiSemiApply")) {
        p("Finds the names of all the people who are not my friends.")
        executionPlan()
      }
    }
    section("LetSemiApply") {
      p("""Tests for the existence of a pattern predicate.
          |When a query contains multiple pattern predicates `LetSemiApply` will be used to evaluate the first of these.
          |It will record the result of evaluating the predicate but will leave any filtering to a another operator.""")
      query("""MATCH (other:Person)
              |WHERE (other)-[:FRIENDS_WITH]->() OR (other)-[:WORKS_IN]->()
              |RETURN other.name""", assertPlanContains("LetSemiApply")) {
        p("""Finds the names of all the people who have a friend or who work somewhere.
            |The `LetSemiApply` operator will be used to check for the existence of the `FRIENDS_WITH`
            |relationship from each person.""")
        executionPlan()
      }
    }
    section("LetAntiSemiApply") {
      p("""Tests for the absence of a pattern predicate.
          |When a query contains multiple pattern predicates `LetAntiSemiApply` will be used to evaluate the first of these.
          |It will record the result of evaluating the predicate but will leave any filtering to another operator.
          |The following query will find all the people who don't have any friends or who work somewhere.""")
      query("""MATCH (other:Person)
              |WHERE NOT((other)-[:FRIENDS_WITH]->()) OR (other)-[:WORKS_IN]->()
              |RETURN other.name""", assertPlanContains("LetAntiSemiApply")) {
        p("""Finds all the people who don't have any friends or who work somewhere.
            |The `LetAntiSemiApply` operator will be used to check for the absence of
            |the `FRIENDS_WITH` relationship from each person.""")
        executionPlan()
      }
    }
    section("SelectOrSemiApply") {
      p("""Tests for the existence of a pattern predicate and evaluates a predicate.
          |This operator allows for the mixing of normal predicates and pattern predicates
          |that check for the existence of a pattern.
          |First the normal expression predicate is evaluated, and only if it returns `false`
          |the costly pattern predicate evaluation is performed.""")
      query("""MATCH (other:Person)
              |WHERE other.age > 25 OR (other)-[:FRIENDS_WITH]->()
              |RETURN other.name""", assertPlanContains("SelectOrSemiApply")) {
        p("Finds the names of all people who have friends, or are older than 25.")
        executionPlan()
      }
    }
    section("SelectOrAntiSemiApply") {
      p("Tests for the absence of a pattern predicate and evaluates a predicate.")
      query("""MATCH (other:Person)
              |WHERE other.age > 25 OR NOT (other)-[:FRIENDS_WITH]->()
              |RETURN other.name""", assertPlanContains("SelectOrAntiSemiApply")) {
        p("Finds the names of all people who do not have friends, or are older than 25.")
        executionPlan()
      }
    }
    section("ConditionalApply") {
      p("Checks whether a variable is not `null`, and if so the right-hand side will be executed.")
      query("""MERGE (p:Person {name: 'Andres'}) ON MATCH SET p.exists = true""",
            assertPlanContains("ConditionalApply")) {
        p("Looks for the existence of a person called Andres, and if found sets the `exists` property to `true`.")
        executionPlan()
      }
    }
    section("AntiConditionalApply") {
      p("Checks whether a variable is `null`, and if so the right-hand side will be executed.")
      query("""MERGE (p:Person {name: 'Andres'}) ON CREATE SET p.exists = true""",
            assertPlanContains("ConditionalApply")) {
        p("""Looks for the existence of a person called Andres, and if not found, creates one and
          |sets the `exists` property to `true`.""")
        executionPlan()
      }
    }
    section("AssertSameNode") {
      p("This operator is used to ensure that no uniqueness constraints are violated.")
      query("MERGE (t:Team {name: 'Engineering', id: 42})",
            assertPlanContains("AssertSameNode")) {
        p("""Looks for the existence of a team with the supplied name and id, and if one does not exist,
          |it will be created. Due to the existence of two uniqueness constraints
          |on `:Team(name)` and `:Team(id)`, any node that would be found by the `UniqueIndexSeek`s
          |must be the very same node, or the constraints would be violated.""")
        executionPlan()
      }
    }
    section("NodeHashJoin") {
      p("Using a hash table, a `NodeHashJoin` joins the input coming from the left with the input coming from the right.")
      query("""MATCH (andy:Person {name:'Andreas'})-[:WORKS_IN]->(loc)<-[:WORKS_IN]-(matt:Person {name:'Mattis'})
              |RETURN loc.name""",
            assertPlanContains("NodeHashJoin")) {
        initQueries("""MATCH (london:Location {name: 'London'}), (person:Person {name: 'Pontus'})
                      |FOREACH(x in range(0,250) |
                      |  CREATE (person)-[:WORKS_IN]->(london)
                      |)""")
        p("Returns the name of the location where the matched persons both work.")
        executionPlan()
      }
    }
    section("Triadic") {
      p("""Triadic is used to solve triangular queries, such as the very
          |common 'find my friend-of-friends that are not already my friend'.
          |It does so by putting all the friends in a set, and use that set to check if the
          |friend-of-friends are already connected to me.""")
      query("""MATCH (me:Person)-[:FRIENDS_WITH]-()-[:FRIENDS_WITH]-(other)
              |WHERE NOT (me)-[:FRIENDS_WITH]-(other)
              |RETURN other.name""",
            assertPlanContains("Triadic")) {
        p("Finds the names of all friends of my friends that are not already my friends.")
        executionPlan()
      }
    }
  }.build()

  private def assertPlanContains(planName: String) = ResultAndDbAssertions((pb, db) => {
      pb.executionPlanDescription().toString should include(planName)
  })

}
