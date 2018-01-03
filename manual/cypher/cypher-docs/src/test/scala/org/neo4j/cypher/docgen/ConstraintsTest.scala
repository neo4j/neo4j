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

import org.junit.Test
import org.neo4j.cypher.{ConstraintValidationException, CypherExecutionException}
import org.neo4j.kernel.api.constraints.{NodePropertyConstraint, RelationshipPropertyConstraint}
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory

import scala.collection.JavaConverters._

class ConstraintsTest extends DocumentingTestBase with SoftReset {

  override protected def newTestGraphDatabaseFactory() = new TestEnterpriseGraphDatabaseFactory()

  def section: String = "Constraints"

  @Test def create_unique_constraint() {
    testQuery(
      title = "Create uniqueness constraint",
      text = "To create a constraint that makes sure that your database will never contain more than one node with a specific " +
        "label and one property value, use the `IS UNIQUE` syntax.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE",
      optionalResultExplanation = "",
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def drop_unique_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Drop uniqueness constraint",
      text = "By using `DROP CONSTRAINT`, you remove a constraint from the database.",
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

  @Test def create_node_property_existence_constraint() {
    testQuery(
      title = "Create node property existence constraint",
      text = "To create a constraint that makes sure that all nodes with a certain label have a certain property, use the `ASSERT exists(identifier.propertyName)` syntax.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)",
      optionalResultExplanation = "",
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def drop_node_property_existence_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Drop node property existence constraint",
      text = "By using +DROP+ +CONSTRAINT+, you remove a constraint from the database.",
      queryText = "DROP CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)")),
      assertions = (p) => assertNodeConstraintDoesNotExist("Book", "isbn")
    )
  }

  @Test def play_nice_with_node_property_existence_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Create a node that complies with property existence constraints",
      text = "Create a `Book` node with an existing `isbn` property.",
      queryText = "CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)")),
      assertions = (p) => assertNodeConstraintExist("Book", "isbn")
    )
  }

  @Test def break_node_property_existence_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)")
    testFailingQuery[ConstraintValidationException](
      title = "Create a node that breaks a property existence constraint",
      text = "Trying to create a `Book` node without an `isbn` property, given a property existence constraint on `:Book(isbn)`.",
      queryText = "CREATE (book:Book {title: 'Graph Databases'})",
      optionalResultExplanation = "In this case the node isn't created in the graph."
    )
  }

  @Test def break_node_property_existence_constraint_by_removing_property() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)")
    engine.execute("CREATE (book:Book {isbn: '1449356265', title: 'Graph Databases'})")
    testFailingQuery[ConstraintValidationException](
      title = "Removing an existence constrained node property",
      text = "Trying to remove the `isbn` property from an existing node `book`, given a property existence constraint on `:Book(isbn)`.",
      queryText = "MATCH (book:Book {title: 'Graph Databases'}) REMOVE book.isbn",
      optionalResultExplanation = "In this case the property is not removed."
    )
  }

  @Test def fail_to_create_node_property_existence_constraint() {
    generateConsole = false
    engine.execute("CREATE (book:Book {title: 'Graph Databases'})")

    testFailingQuery[CypherExecutionException](
      title = "Failure to create a node property existence constraint due to existing node",
      text = "Create a constraint on the property `isbn` on nodes with the `Book` label when there already exists " +
        " a node without an `isbn`.",
      queryText = "CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)",
      optionalResultExplanation = "In this case the constraint can't be created because it is violated by existing " +
        "data. We may choose to remove the offending nodes and then re-apply the constraint."
    )
  }

  @Test def create_relationship_property_existence_constraint() {
    testQuery(
      title = "Create relationship property existence constraint",
      text = "To create a constraint that makes sure that all relationships with a certain type have a certain property, use the `ASSERT exists(identifier.propertyName)` syntax.",
      queryText = "CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)",
      optionalResultExplanation = "",
      assertions = (p) => assertRelationshipConstraintExist("LIKED", "day")
    )
  }

  @Test def drop_relationship_property_existence_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Drop relationship property existence constraint",
      text = "To remove a constraint from the database, use `DROP CONSTRAINT`.",
      queryText = "DROP CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)")),
      assertions = (p) => assertRelationshipConstraintDoesNotExist("LIKED", "day")
    )
  }

  @Test def play_nice_with_relationship_property_existence_constraint() {
    generateConsole = false

    prepareAndTestQuery(
      title = "Create a relationship that complies with property existence constraints",
      text = "Create a `LIKED` relationship with an existing `day` property.",
      queryText = "CREATE (user:User)-[like:LIKED {day: 'yesterday'}]->(book:Book)",
      optionalResultExplanation = "",
      prepare = _ => executePreparationQueries(List("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)")),
      assertions = (p) => assertRelationshipConstraintExist("LIKED", "day")
    )
  }

  @Test def break_relationship_property_existence_constraint() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)")
    testFailingQuery[ConstraintValidationException](
      title = "Create a relationship that breaks a property existence constraint",
      text = "Trying to create a `LIKED` relationship without a `day` property, given a property existence constraint `:LIKED(day)`.",
      queryText = "CREATE (user:User)-[like:LIKED]->(book:Book)",
      optionalResultExplanation = "In this case the relationship isn't created in the graph."
    )
  }

  @Test def break_relationship_property_existence_constraint_by_removing_property() {
    generateConsole = false
    engine.execute("CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)")
    engine.execute("CREATE (user:User)-[like:LIKED {day: 'today'}]->(book:Book)")
    testFailingQuery[ConstraintValidationException](
      title = "Removing an existence constrained relationship property",
      text = "Trying to remove the `day` property from an existing relationship `like` of type `LIKED`, given a property existence constraint `:LIKED(day)`.",
      queryText = "MATCH (user:User)-[like:LIKED]->(book:Book) REMOVE like.day",
      optionalResultExplanation = "In this case the property is not removed."
    )
  }

  @Test def fail_to_create_relationship_property_existence_constraint() {
    generateConsole = false
    engine.execute("CREATE (user:User)-[like:LIKED]->(book:Book)")

    testFailingQuery[CypherExecutionException](
      title = "Failure to create a relationship property existence constraint due to existing relationship",
      text = "Create a constraint on the property `day` on relationships with the `LIKED` type when there already " +
        "exists a relationship without a property named `day`.",
      queryText = "CREATE CONSTRAINT ON ()-[like:LIKED]-() ASSERT exists(like.day)",
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
