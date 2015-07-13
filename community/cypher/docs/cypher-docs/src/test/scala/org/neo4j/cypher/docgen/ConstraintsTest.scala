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

import org.junit.Test
import org.neo4j.cypher.{ConstraintValidationException, CypherExecutionException}
import org.neo4j.kernel.api.constraints.{NodePropertyConstraint, RelationshipPropertyConstraint}

import scala.collection.JavaConverters._

class ConstraintsTest extends DocumentingTestBase with SoftReset {

  def section: String = "Constraints"

  @Test def create_unique_constraint() {
    testQuery(
      title = "Create uniqueness constraint",
      text = "To create a constraint that makes sure that your database will never contain more than one node with a specific " +
        "label and one property value, use the +IS+ +UNIQUE+ syntax.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "",
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def drop_unique_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Drop uniqueness constraint",
      text = "By using +DROP+ +CONSTRAINT+, you remove a constraint from the database.",
      queryText = "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")),
      assertions = (p) => assertNodeConstraintDoesNotExist("Book", "isbn")
    )
  }

  @Test def play_nice_with_unique_property_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Create a node that complies with unique property constraints",
      text = "Create a `Book` node with an `isbn` that isn't already in the database.",
      queryText = "CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")),
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def break_unique_property_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})")

    testFailingQuery[CypherExecutionException](
      title = "Create a node that breaks a unique property constraint",
      text = "Create a `Book` node with an `isbn` that is already used in the database.",
      queryText = "CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})",
      optionalResultExplanation = "In this case the node isn't created in the graph."
    )
  }

  @Test def fail_to_create_constraint() {
    generateConsole = false
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})")
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases 2'})")

    testFailingQuery[CypherExecutionException](
      title = "Failure to create a unique property constraint due to conflicting nodes",
      text = "Create a unique property constraint on the property `isbn` on nodes with the `Book` label when there are two nodes with" +
        " the same `isbn`.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "In this case the constraint can't be created because it is violated by existing " +
        "data. We may choose to use <<query-schema-index>> instead or remove the offending nodes and then re-apply the " +
        "constraint."
    )
  }

  @Test def create_mandatory_node_property_constraint() {
    testQuery(
      title = "Create mandatory node property constraint",
      text = "To create a constraint that makes sure that all nodes with a certain label have a certain property, use the +IS+ +NOT+ +NULL+ syntax.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL",
      optionalResultExplanation = "",
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def drop_mandatory_node_property_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Drop mandatory node property constraint",
      text = "By using +DROP+ +CONSTRAINT+, you remove a constraint from the database.",
      queryText = "DROP CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL")),
      assertions = (p) => assertNodeConstraintDoesNotExist("Book", "isbn")
    )
  }

  @Test def play_nice_with_mandatory_node_property_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Create a node that complies with mandatory property constraints",
      text = "Create a `Book` node with an existing `isbn` property.",
      queryText = "CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL")),
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def break_mandatory_node_property_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL")
    testFailingQuery[ConstraintValidationException](
      title = "Create a node that breaks a mandatory property constraint",
      text = "Create a `Book` node without an `isbn`.",
      queryText = "CREATE (book:Book {title: 'Graph Databases'})",
      optionalResultExplanation = "In this case the node isn't created in the graph."
    )
  }

  @Test def break_mandatory_node_property_constraint_by_removing_property() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL")
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})")
    testFailingQuery[ConstraintValidationException](
      title = "Removing a mandatory node property",
      text = "Trying to remove property `isbn` from an existing book.",
      queryText = "MATCH (book:Book {title: 'Graph Databases'}) REMOVE book.isbn",
      optionalResultExplanation = "In this case the property is not removed."
    )
  }

  @Test def fail_to_create_mandatory_node_property_constraint() {
    generateConsole = false
    engine.execute("CREATE (book:Book {title: 'Graph Databases'})")

    testFailingQuery[CypherExecutionException](
      title = "Failure to create a mandatory node property constraint due to existing node",
      text = "Create a constraint on the property `isbn` on nodes with the `Book` label when there already exists " +
        " a node without an `isbn`.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS NOT NULL",
      optionalResultExplanation = "In this case the constraint can't be created because it is violated by existing " +
        "data. We may choose to remove the offending nodes and then re-apply the constraint."
    )
  }

  @Test def create_mandatory_relationship_property_constraint() {
    testQuery(
      title = "Create mandatory relationship property constraint",
      text = "To create a constraint that makes sure that all relationships with a certain type have a certain property, use the +IS+ +NOT+ +NULL+ syntax.",
      queryText = "CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL",
      optionalResultExplanation = "",
      assertions = (p) => assertRelationshipConstraintExist("LIKED", "day")
    )
  }

  @Test def drop_mandatory_relationship_property_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Drop mandatory relationship property constraint",
      text = "By using +DROP+ +CONSTRAINT+, you remove a constraint from the database.",
      queryText = "DROP CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL")),
      assertions = (p) => assertRelationshipConstraintDoesNotExist("LIKED", "day")
    )
  }

  @Test def play_nice_with_mandatory_relationship_property_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Create a relationship that complies with mandatory property constraints",
      text = "Create a `LIKED` relationship with an existing `day` property.",
      queryText = "CREATE (user:User)-[like:LIKED {day: 'yesterday'}]->(book:Book)",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL")),
      assertions = (p) => assertRelationshipConstraintExist("LIKED", "day")
    )
  }

  @Test def break_mandatory_relationship_property_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL")
    testFailingQuery[ConstraintValidationException](
      title = "Create a relationship that breaks a mandatory property constraint",
      text = "Create a `LIKED` relationship without an existing `day` property.",
      queryText = "CREATE (user:User)-[like:LIKED]->(book:Book)",
      optionalResultExplanation = "In this case the relationship isn't created in the graph."
    )
  }

  @Test def break_mandatory_relationship_property_constraint_by_removing_property() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL")
    engine.execute("CREATE (user:User)-[like:LIKED {day: 'today'}]->(book:Book)")
    testFailingQuery[ConstraintValidationException](
      title = "Removing a mandatory relationship property",
      text = "Trying to remove property `day` from an existing like.",
      queryText = "MATCH (user:User)-[like:LIKED]->(book:Book) REMOVE like.day",
      optionalResultExplanation = "In this case the property is not removed."
    )
  }

  @Test def fail_to_create_mandatory_relationship_property_constraint() {
    generateConsole = false
    engine.execute("CREATE (user:User)-[like:LIKED]->(book:Book)")

    testFailingQuery[CypherExecutionException](
      title = "Failure to create a mandatory relationship property constraint due to existing relationship",
      text = "Create a constraint on the property `day` on relationships with the `LIKED` type when there already " +
        "exists a relationship without an `day`.",
      queryText = "CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT like.day IS NOT NULL",
      optionalResultExplanation = "In this case the constraint can't be created because it is violated by existing " +
        "data. We may choose to remove the offending relationships and then re-apply the constraint."
    )
  }


  private def assertNodeConstraintDoesNotExist(labelName: String, propName: String) {
    assert(getNodeConstraintIterator(labelName, propName).isEmpty, "Expected constraint iterator to be empty")
  }

  private def assertNodeConstraintExist(labelName: String, propName: String) {
    assert(getNodeConstraintIterator(labelName, propName).size === 1)
  }

  private def assertRelationshipConstraintDoesNotExist(typeName: String, propName: String) {
    assert(getRelationshipConstraintIterator(typeName, propName).isEmpty, "Expected constraint iterator to be empty")
  }

  private def assertRelationshipConstraintExist(typeName: String, propName: String) {
    assert(getRelationshipConstraintIterator(typeName, propName).size === 1)
  }

  private def getNodeConstraintIterator(labelName: String, propName: String): Iterator[NodePropertyConstraint] = {
    val statement = db.statement

    val prop = statement.readOperations().propertyKeyGetForName(propName)
    val label = statement.readOperations().labelGetForName(labelName)

    statement.readOperations().constraintsGetForLabelAndPropertyKey(label, prop).asScala
  }

  private def getRelationshipConstraintIterator(typeName: String, propName: String): Iterator[RelationshipPropertyConstraint] = {
    val statement = db.statement

    val prop = statement.readOperations().propertyKeyGetForName(propName)
    val relType = statement.readOperations().relationshipTypeGetForName(typeName)

    statement.readOperations().constraintsGetForRelationshipTypeAndPropertyKey(relType, prop).asScala
  }
}
